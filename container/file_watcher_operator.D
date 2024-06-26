FROM python:3.12-slim

RUN pip install kopf
RUN pip install kubernetes

RUN mkdir /src
ADD file_watcher_operator/file_watcher_operator.py /src

CMD kopf run --liveness=http://0.0.0.0:8080/healthz /src/file_watcher_operator.py --verbose