"""
Filewatcher operator controls the deployments, PVs, and PVCs based on instrument CRDs
"""

import logging
import os
import sys
from collections.abc import Mapping
from typing import Any

import kopf
from kubernetes import client
from kubernetes.client import (
    V1Container,
    V1CSIPersistentVolumeSource,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1EnvVarSource,
    V1ExecAction,
    V1LabelSelector,
    V1ObjectMeta,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1PersistentVolumeSpec,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1ResourceRequirements,
    V1SecretKeySelector,
    V1Volume,
    V1VolumeMount,
)


class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("/healthz") == -1 and record.getMessage().find("/ready") == -1


stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("aiohttp.access").addFilter(EndpointFilter())


def build_smb_pvc(pvc_name: str, namespace: str, pv_name: str) -> V1PersistentVolumeClaim:
    return V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=V1ObjectMeta(
            name=pvc_name,
            namespace=namespace,
        ),
        spec=V1PersistentVolumeClaimSpec(
            access_modes=["ReadOnlyMany"],
            resources=V1ResourceRequirements(requests={"storage": "1000Gi"}),
            volume_name=pv_name,
            storage_class_name="smb",
        ),
    )


def build_smb_pv(
    pv_name: str, namespace: str, host: str, creds_name: str, mount_options: None | list[str] = None
) -> V1PersistentVolume:
    if mount_options is None:
        mount_options = []
    return V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata=V1ObjectMeta(
            name=pv_name,
            annotations={"pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"},
        ),
        spec=V1PersistentVolumeSpec(
            capacity={"storage": "1000Gi"},
            access_modes=["ReadOnlyMany"],
            persistent_volume_reclaim_policy="Retain",
            storage_class_name="smb",
            mount_options=mount_options,
            csi=V1CSIPersistentVolumeSource(
                driver="smb.csi.k8s.io",
                read_only=True,
                volume_handle=f"{pv_name}.{namespace}.svc.cluster.local/share##{pv_name}",
                volume_attributes={
                    "source": host,
                },
                node_stage_secret_ref={
                    "name": creds_name,
                    "namespace": namespace,
                },
            ),
        ),
    )


def setup_imat_pvcs_pvs(namespace: str) -> tuple[V1PersistentVolume, V1PersistentVolumeClaim]:
    imat_pv_name = "filewatcher-ndximat-data-pv"
    imat_pvc_name = "filewatcher-ndximat-data-pvc"

    imat_pv = build_smb_pv(imat_pv_name, namespace, "//NDXIMAT.isis.cclrc.ac.uk/data$/", "imat-creds")
    imat_pvc = build_smb_pvc(imat_pvc_name, namespace, imat_pv_name)

    return imat_pv, imat_pvc


def setup_archive_pvcs_pvs(namespace: str, name: str) -> tuple[V1PersistentVolume, V1PersistentVolumeClaim]:
    archive_pv_name = f"filewatcher-{name}-pv"
    archive_pvc_name = f"filewatcher-{name}-pvc"

    mount_options = ["noserverino", "_netdev", "vers=2.1", "uid=1001", "gid=1001", "dir_mode=0555", "file_mode=0444"]

    archive_pv = build_smb_pv(
        archive_pv_name, namespace, "//isisdatar55.isis.cclrc.ac.uk/inst$/", "archive-creds", mount_options
    )
    archive_pvc = build_smb_pvc(archive_pvc_name, namespace, archive_pv_name)

    return archive_pv, archive_pvc


def define_mounts(claim_name: str, mount_name: str, mount_path: str) -> tuple[V1Volume, V1VolumeMount]:
    volume_mount = V1VolumeMount(name=mount_name, mount_path=mount_path)
    volume = V1Volume(
        name=mount_name,
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
            claim_name=claim_name,
            read_only=True,
        ),
    )
    return volume_mount, volume


def build_deployment(
    spec: Mapping[str, Any], name: str
) -> tuple[V1Deployment, V1PersistentVolume, V1PersistentVolumeClaim]:
    """
    Create and return a Kubernetes deployment yaml for each deployment
    :param spec: The kopf spec
    :param name: The instrument name
    :param children: The list of children for this filewatcher
    :return: Tuple of the mutable mappings containing the deployment specs
    """
    queue_host = os.environ.get("QUEUE_HOST", "rabbitmq-cluster.rabbitmq.svc.cluster.local")
    queue_name = os.environ.get("EGRESS_QUEUE_NAME", "watched-files")
    file_watcher_sha = os.environ.get("FILE_WATCHER_SHA256", "")
    namespace = os.environ.get("FILEWATCHER_NAMESPACE", "fia")
    fia_api_url = os.environ.get("FIA_API_URL", "localhost:8000")

    if "specialPV" in spec and spec["specialPV"].upper() != "NONE":
        special_pv = spec.get("specialPV", "imat").lower()
        match special_pv:
            case "imat":
                watch_pv, watch_pvc = setup_imat_pvcs_pvs(namespace)
                volume_mount, volume = define_mounts(
                    claim_name=watch_pvc.metadata.name, mount_name="imat-mount", mount_path="/imat"
                )
            case _:
                watch_pv, watch_pvc = setup_archive_pvcs_pvs(namespace, name=name)
                volume_mount, volume = define_mounts(
                    claim_name=watch_pvc.metadata.name, mount_name="archive-mount", mount_path="/archive"
                )
    else:
        watch_pv, watch_pvc = setup_archive_pvcs_pvs(namespace, name=name)
        volume_mount, volume = define_mounts(
            claim_name=watch_pvc.metadata.name, mount_name="archive-mount", mount_path="/archive"
        )

    env = [
        V1EnvVar(name="QUEUE_HOST", value=queue_host),
        V1EnvVar(name="EGRESS_QUEUE_NAME", value=queue_name),
        V1EnvVar(name="WATCH_FILE", value=spec.get("lastrunFilePath", "/archive/NDXMARI/Instrument/logs/lastrun.txt")),
        V1EnvVar(name="FILE_PREFIX", value=spec.get("filePrefix", "MAR")),
        V1EnvVar(name="INSTRUMENT_FOLDER", value=spec.get("instrumentFolder", "NDXMAR")),
        V1EnvVar(name="FIA_API_URL", value=fia_api_url),
        V1EnvVar(
            name="QUEUE_USER",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="filewatcher-secrets",
                    key="queue_user",
                )
            ),
        ),
        V1EnvVar(
            name="QUEUE_PASSWORD",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="filewatcher-secrets",
                    key="queue_password",
                )
            ),
        ),
        V1EnvVar(
            name="FIA_API_API_KEY",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="fia-api",
                    key="fia_api_api_key",
                )
            ),
        ),
    ]

    heartbeat_probe_cmd = [
        "sh",
        "-c",
        (
            "CURRENT_TIME=$(date +%s); "
            "FILE_TIME=$(date -r /tmp/heartbeat +%s); "
            "DIFF=$((CURRENT_TIME - FILE_TIME)); "
            "if [ $DIFF -lt 20 ]; then exit 0; else exit 1; fi"
        ),
    ]

    readiness_probe = V1Probe(
        _exec=V1ExecAction(command=heartbeat_probe_cmd),
        initial_delay_seconds=30,
        period_seconds=10,
    )

    liveness_probe = V1Probe(
        _exec=V1ExecAction(command=heartbeat_probe_cmd),
        initial_delay_seconds=30,
        failure_threshold=3,
        period_seconds=10,
    )

    container = V1Container(
        name=f"filewatcher-{name}",
        image=f"ghcr.io/fiaisis/filewatcher@sha256:{file_watcher_sha}",
        env=env,
        readiness_probe=readiness_probe,
        liveness_probe=liveness_probe,
        volume_mounts=[volume_mount],
    )

    pod_spec = V1PodSpec(
        containers=[container],
        volumes=[volume],
    )

    template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(labels={"app": f"filewatcher-{name}"}),
        spec=pod_spec,
    )

    deployment_spec = V1DeploymentSpec(
        replicas=1,
        selector=V1LabelSelector(match_labels={"app": f"filewatcher-{name}"}),
        template=template,
    )

    deployment = V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=V1ObjectMeta(
            name=f"filewatcher-{name}-deployment",
            namespace=namespace,
            labels={"app": f"filewatcher-{name}"},
        ),
        spec=deployment_spec,
    )

    return deployment, watch_pvc, watch_pv


def deploy_deployment(deployment_spec: Mapping[str, Any], name: str, children: list[Any]) -> None:
    """
    Given a deployment spec, name, and operators children, create the namespaced deployment and add it's uid to the
    children
    :param deployment_spec: The deployment spec
    :param name: The name of the spec
    :param children: The operators children
    :return: None
    """
    app_api = client.AppsV1Api()
    logger.info("Starting deployment of: filewatcher-%s", name)
    namespace = os.environ.get("FILEWATCHER_NAMESPACE", "fia")
    depl = app_api.create_namespaced_deployment(namespace=namespace, body=deployment_spec)
    children.append(depl.metadata.uid)
    logger.info("Deployed: filewatcher-%s", name)


def deploy_pvc(pvc_spec: V1PersistentVolumeClaim, name: str, children: list[Any] | None) -> None:
    """
    Given a pvc spec, name, and the operators children, create the namespaced persistent volume claim and add its uid
    to the operators children
    :param pvc_spec: The pvc spec
    :param name: The name of the pvc
    :param children: The operators children
    :return: None
    """
    namespace = os.environ.get("FILEWATCHER_NAMESPACE", "fia")
    core_api = client.CoreV1Api()
    # Check if PVC exists else deploy a new one:
    if pvc_spec.metadata.name not in [
        ii.metadata.name for ii in core_api.list_namespaced_persistent_volume_claim(pvc_spec.metadata.namespace).items
    ]:
        logger.info("Starting deployment of PVC: filewatcher-%s", name)
        pvc = core_api.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc_spec)
        if children is not None:
            children.append(pvc.metadata.uid)
        logger.info("Deployed PVC: filewatcher-%s", name)


def deploy_pv(pv_spec: V1PersistentVolume, name: str, children: list[Any] | None) -> None:
    """
    Given a pvc spec, name, and the operators children, create the namespaced persistent volume and add its uid
    to the operators children
    :param pv_spec: The pv spec
    :param name: The name of the pv
    :param children: The operators children
    :return: None
    """
    core_api = client.CoreV1Api()
    # Check if PV exists else deploy a new one
    if pv_spec.metadata.name not in [ii.metadata.name for ii in core_api.list_persistent_volume().items]:
        logger.info("Starting deployment of PV: filewatcher-%s", name)
        pv = core_api.create_persistent_volume(body=pv_spec)
        if children is not None:
            children.append(pv.metadata.uid)
        logger.info("Deployed PV: filewatcher-%s", name)


@kopf.on.create("fia.com", "v1", "filewatchers")
def create_fn(spec: Any, **kwargs: Any) -> dict[str, list[Any]]:
    """
    Kopf create event handler, generates all 3 specs then creates them in the cluster, while creating the children and
    adopting the deployment and pvc
    :param spec: Spec of the CRD intercepted by kopf
    :param kwargs: KWARGS
    :return: None
    """
    name = kwargs["body"]["metadata"]["name"]
    logger.info("Name is filewatcher-%s", name)
    children: list[Any] = []

    # Build and deploy everything
    deployment, pvc, pv = build_deployment(spec, name)
    deploy_pv(pv, name, children)
    deploy_pvc(pvc, name, children)
    deploy_deployment(deployment, name, children)

    # Make the deployment the child of this operator
    kopf.adopt(deployment)
    kopf.adopt(pvc)

    # Update controller's status with child deployment
    return {"children": children}


@kopf.on.delete("fia.com", "v1", "filewatchers")
def delete_func(**kwargs: Any) -> None:
    """
    Kopf delete event handler. This will automatically delete the filewatcher deployment and pvc, and will manually
    delete the persistent volume
    :param kwargs: kwargs
    :return: None
    """
    name = kwargs["body"]["metadata"]["name"]
    imat_pv = "filewatcher-ndximat-data-pv"
    default_pv = f"filewatcher-{name}-pv"
    core_api = client.CoreV1Api()
    if name.lower() == "imat" and imat_pv in [ii.metadata.name for ii in core_api.list_persistent_volume().items]:
        client.CoreV1Api().delete_persistent_volume(name=imat_pv)
    elif default_pv in [ii.metadata.name for ii in core_api.list_persistent_volume().items]:
        client.CoreV1Api().delete_persistent_volume(name=default_pv)
    else:
        logger.info("PV for %s could not be found", name)


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
    deployment_spec, _, __ = build_deployment(spec, name)
    app_api = client.AppsV1Api()

    app_api.patch_namespaced_deployment(
        name=f"filewatcher-{name}-deployment", namespace=namespace, body=deployment_spec
    )
