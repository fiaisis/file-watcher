# File Watcher repository contains two projects
## FileWatcher
A piece of software designed to look at a instruments lastrun.txt file and determine when a new .nxs file has appeared. Once it has appeared it will send a message to the correct memphis station.

There is a recovery attempt that can be made for the instrument, by checking if we have missed any .nxs files from the instrument by using some saved state in the Database.

## FileWatcherOperator
The point of the operator is to check for CustomResourceDefinition files that have been applied to the cluster. Examples can be found in the GitOps repository of what these should look like as part of the deployment for the file-watcher-operator. When a CRD is applied, this software should create a Deployment responsible for ensuring a file-watcher exists for the parameters in the CRD file.

## Docker

Login using docker login

Then build and push the file-watcher container
```shell
docker build . -f ./container/file_watcher.D -t ghcr.io/interactivereduction/filewatcher
docker push ghcr.io/interactivereduction/filewatcher -a
```

Then build and push the file-watcher-operator container

```shell
docker build . -f ./container/file_watcher_operator.D -t ghcr.io/interactivereduction/filewatcher-operator
docker push ghcr.io/interactivereduction/filewatcher-operator -a
```

The file-watcher container, when updated needs to be updated in each of the CRDs controlled by the file watcher operator.

The file-watcher-operator container, when updated should be updated like any of the containers in the GitOps repository.