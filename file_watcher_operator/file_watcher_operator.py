"""
Filewatcher operator controls the deployments, PVs, and PVCs based on instrument CRDs
"""

import logging
import os
import sys
from typing import Dict, Any, Mapping, Tuple, List, MutableMapping

import kopf
import kubernetes  # type: ignore
import yaml

# pylint: disable = (duplicate-code)
# This will be detected from the file watcher which is not the same application.
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
# pylint: enable = duplicate-code


def generate_deployment_body(
    spec: Mapping[str, Any], name: str
) -> Tuple[MutableMapping[str, Any], MutableMapping[str, Any], MutableMapping[str, Any]]:
    """
    Create and return a Kubernetes deployment yaml for each deployment
    :param spec: The kopf spec
    :param name: The instrument name
    :return: Tuple of the mutable mappings containing the deployment specs
    """
    archive_dir = os.environ.get("ARCHIVE_DIR", "/archive")
    queue_host = os.environ.get("QUEUE_HOST", "rabbitmq-cluster.rabbitmq.svc.cluster.local")
    queue_name = os.environ.get("EGRESS_QUEUE_NAME", "watched-files")
    file_watcher_sha = os.environ.get("FILE_WATCHER_SHA256", "")
    db_ip = os.environ.get("DB_IP", "localhost")
    archive_pvc_name = f"{name}-file-watcher-pvc"
    archive_pv_name = f"{name}-file-watcher-pv"
    deployment_spec = yaml.safe_load(
        f"""
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: {name}-file-watcher-deployment
              labels:
                app: {name}-file-watcher
            spec:
              replicas: 1
              selector:
                matchLabels:
                  app: {name}-file-watcher
              template:
                metadata:
                  labels:
                    app: {name}-file-watcher
                spec:
                  containers:
                  - name: {name}-file-watcher
                    image: ghcr.io/fiaisis/filewatcher@sha256:{file_watcher_sha}
                    env:
                    - name: QUEUE_HOST
                      value: {queue_host}
                    - name: EGRESS_QUEUE_NAME
                      value: {queue_name}
                    - name: WATCH_DIR
                      value: {archive_dir}
                    - name: FILE_PREFIX
                      value: {spec.get("filePrefix", "MAR")}
                    - name: INSTRUMENT_FOLDER
                      value: {spec.get("instrumentFolder", "NDXMAR")}
                    - name: DB_IP
                      value: {db_ip}

                    # Secrets
                    - name: QUEUE_USER
                      valueFrom: 
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: queue_user
                    - name: QUEUE_PASSWORD
                      valueFrom: 
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: queue_password
                    - name: DB_USERNAME
                      valueFrom:
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: db_username
                    - name: DB_PASSWORD
                      valueFrom:
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: db_password
                    volumeMounts:
                      - name: archive-mount
                        mountPath: {archive_dir}
                  volumes:
                    - name: archive-mount
                      persistentVolumeClaim:
                        claimName: {archive_pvc_name}
                        readOnly: true
        """
    )
    pvc_spec = yaml.safe_load(
        f"""
            kind: PersistentVolumeClaim
            apiVersion: v1
            metadata:
              name: {archive_pvc_name}
            spec:
              accessModes:
                - ReadOnlyMany
              resources:
                requests:
                  storage: 1000Gi
              volumeName: {archive_pv_name}
              storageClassName: smb
          """
    )

    pv_spec = yaml.safe_load(
        f"""
            apiVersion: v1
            kind: PersistentVolume
            metadata:
              annotations:
                pv.kubernetes.io/provisioned-by: smb.csi.k8s.io
              name: {archive_pv_name}
            spec:
              capacity:
                storage: 1000Gi
              accessModes:
                - ReadOnlyMany
              persistentVolumeReclaimPolicy: Retain
              storageClassName: smb
              mountOptions:
                - noserverino
                - _netdev
                - vers=2.1
                - uid=1001
                - gid=1001
                - dir_mode=0555
                - file_mode=0444
              csi:
                driver: smb.csi.k8s.io
                readOnly: true
                volumeHandle: archive.ir.svc.cluster.local/share##archive
                volumeAttributes:
                  source: "//isisdatar55.isis.cclrc.ac.uk/inst$/"
                nodeStageSecretRef:
                  name: archive-creds
                  namespace: ir
          """
    )

    return deployment_spec, pvc_spec, pv_spec


def deploy_deployment(deployment_spec: Mapping[str, Any], name: str, children: List[Any]) -> None:
    """
    Given a deployment spec, name, and operators children, create the namespaced deployment and add it's uid to the
    children
    :param deployment_spec: The deployment spec
    :param name: The name of the spec
    :param children: The operators children
    :return: None
    """
    app_api = kubernetes.client.AppsV1Api()
    logger.info("Starting deployment of: %s filewatcher", name)
    namespace = os.environ.get("FILEWATCHER_NAMESPACE", "fia")
    depl = app_api.create_namespaced_deployment(namespace=namespace, body=deployment_spec)
    children.append(depl.metadata.uid)
    logger.info("Deployed: %s filewatcher", name)


def deploy_pvc(pvc_spec: Mapping[str, Any], name: str, children: List[Any]) -> None:
    """
    Given a pvc spec, name, and the operators children, create the namespaced persistent volume claim and add its uid
    to the operators children
    :param pvc_spec: The pvc spec
    :param name: The name of the pvc
    :param children: The operators children
    :return: None
    """
    namespace = os.environ.get("FILEWATCHER_NAMESPACE", "fia")
    core_api = kubernetes.client.CoreV1Api()
    # Check if PVC exists else deploy a new one:
    if pvc_spec["metadata"]["name"] not in [
        ii.metadata.name
        for ii in core_api.list_namespaced_persistent_volume_claim(pvc_spec["metadata"]["namespace"]).items
    ]:
        logger.info("Starting deployment of PVC: %s filewatcher", name)
        pvc = core_api.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc_spec)
        children.append(pvc.metadata.uid)
        logger.info("Deployed PVC: %s filewatcher", name)


def deploy_pv(pv_spec: Mapping[str, Any], name: str, children: List[Any]) -> None:
    """
    Given a pvc spec, name, and the operators children, create the namespaced persistent volume and add its uid
    to the operators children
    :param pv_spec: The pv spec
    :param name: The name of the pv
    :param children: The operators children
    :return: None
    """
    core_api = kubernetes.client.CoreV1Api()
    # Check if PV exists else deploy a new one
    if pv_spec["metadata"]["name"] not in [ii.metadata.name for ii in core_api.list_persistent_volume().items]:
        logger.info("Starting deployment of PV: %s filewatcher", name)
        pv = core_api.create_persistent_volume(body=pv_spec)
        children.append(pv.metadata.uid)
        logger.info("Deployed PV: %s filewatcher", name)


@kopf.on.create("fia.com", "v1", "filewatchers")
def create_fn(spec: Any, **kwargs: Any) -> Dict[str, List[Any]]:
    """
    Kopf create event handler, generates all 3 specs then creates them in the cluster, while creating the children and
    adopting the deployment and pvc
    :param spec: Spec of the CRD intercepted by kopf
    :param kwargs: KWARGS
    :return: None
    """
    name = kwargs["body"]["metadata"]["name"]
    logger.info("Name is %s", name)

    deployment_spec, pvc_spec, pv_spec = generate_deployment_body(spec, name)
    # Make the deployment the child of this operator
    kopf.adopt(deployment_spec)
    kopf.adopt(pvc_spec)

    children: List[Any] = []
    deploy_pv(pv_spec, name, children)
    deploy_pvc(pvc_spec, name, children)
    deploy_deployment(deployment_spec, name, children)
    # Update controller's status with child deployment
    return {"children": children}


@kopf.on.delete("fia.com", "v1", "filewatchers")
def delete_func(**kwargs: Any) -> None:
    """
    Kopf delete event handler. This will automatically delete the filewatcher deployment and pvc, and will manually
    delete the persitent volume
    :param kwargs: kwargs
    :return: None
    """
    name = kwargs["body"]["metadata"]["name"]
    client = kubernetes.client.CoreV1Api()
    client.delete_persistent_volume(name=f"{name}-file-watcher-pv")


@kopf.on.update("fia.com", "v1", "filewatchers")
def update_func(spec: Any, **kwargs: Any) -> None:
    """
    kopf update event handler. This automatically updates the filewatcher deployment when the CRD changes
    :param spec: the spec
    :param kwargs: kwargs
    :return: None
    """
    name = kwargs["body"]["metadata"]["name"]

    namespace = kwargs["body"]["metadata"]["namespace"]
    deployment_spec, _, __ = generate_deployment_body(spec, name)
    app_api = kubernetes.client.AppsV1Api()

    app_api.patch_namespaced_deployment(
        name=f"{name}-file-watcher-deployment", namespace=namespace, body=deployment_spec
    )
