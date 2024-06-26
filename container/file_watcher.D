FROM python:3.12-slim

WORKDIR /file_watcher

COPY . /file_watcher
RUN python -m pip install --no-cache-dir .
ENV PYTHONPATH=/file_watcher

CMD ["file_watcher"]