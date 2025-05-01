"""
Main module
"""

import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Union

from pika import BlockingConnection, ConnectionParameters, PlainCredentials  # type: ignore
from pika.adapters.blocking_connection import BlockingChannel  # type: ignore

from file_watcher.lastrun_file_monitor import create_last_run_detector
from file_watcher.utils import logger


@dataclass
class Config:
    """
    Config for watcher
    """

    host: str
    username: str
    password: str
    queue_name: str
    watch_dir: Path
    run_file_prefix: str
    instrument_folder: str
    db_ip: str
    db_username: str
    db_password: str


def load_config() -> Config:
    """
    Load config values from env vars or get defaults and return the config object
    :return: Config
    """
    return Config(
        os.environ.get("QUEUE_HOST", "localhost"),
        os.environ.get("QUEUE_USER", "guest"),
        os.environ.get("QUEUE_PASSWORD", "guest"),
        os.environ.get("EGRESS_QUEUE_NAME", "watched-files"),
        Path(os.environ.get("WATCH_DIR", "/archive")),
        os.environ.get("FILE_PREFIX", "MAR"),
        os.environ.get("INSTRUMENT_FOLDER", "NDXMARI"),
        os.environ.get("DB_IP", "localhost"),
        os.environ.get("DB_USERNAME", "admin"),
        os.environ.get("DB_PASSWORD", "admin"),
    )


def write_readiness_probe_file() -> None:
    """
    Write the file with the timestamp for the readinessprobe
    :return: None
    """
    path = Path("/tmp/heartbeat")  # noqa: S108
    with path.open("w", encoding="utf-8") as file:
        file.write(time.strftime("%Y-%m-%d %H:%M:%S"))


class FileWatcher:
    """
    The FileWatcher is responsible for owning and running the latest run file detector and then ensuring that Memphis
    received those messages.
    """

    def __init__(self, config: Config):
        self.config = config

    def _get_channel(self) -> BlockingChannel:
        """Get a BlockingChannel"""
        credentials = PlainCredentials(username=self.config.username, password=self.config.password)
        connection_parameters = ConnectionParameters(self.config.host, 5672, credentials=credentials)
        connection = BlockingConnection(connection_parameters)
        channel = connection.channel()
        channel.exchange_declare(self.config.queue_name, exchange_type="direct", durable=True)
        channel.queue_declare(self.config.queue_name, durable=True, arguments={"x-queue-type": "quorum"})
        channel.queue_bind(self.config.queue_name, self.config.queue_name, routing_key="")
        return channel

    @contextmanager
    def producer_channel(self) -> Generator[BlockingChannel, Any, None]:
        """
        Returns a context managed blocking channel
        :return: BlockingChannel
        """
        channel = self._get_channel()
        yield channel
        channel.close()
        channel.connection.close()

    def on_event(self, path: Path) -> None:
        """
        Given a path publish to rabbitmq if not a directory
        :param path: The path to publish
        :return: None
        """
        str_path = str(path)
        if path.is_dir():
            logger.info("Skipping directory creation for %s", str_path)
            return
        with self.producer_channel() as channel:
            channel.basic_publish(self.config.queue_name, "", str(path).encode())

    def start_watching(self) -> None:
        """
        Start the PollingObserver with the queue based event handler and the given queue
        :return: None
        """
        write_readiness_probe_file()

        def _event_occurred(path_to_add: Union[Path, None]) -> None:
            if path_to_add is not None:
                self.on_event(path_to_add)

        last_run_detector = create_last_run_detector(
            self.config.watch_dir,
            self.config.instrument_folder,
            _event_occurred,
            run_file_prefix=self.config.run_file_prefix,
            db_ip=self.config.db_ip,
            db_username=self.config.db_username,
            db_password=self.config.db_password,
        )

        try:
            last_run_detector.watch_for_new_runs(callback_func=write_readiness_probe_file)
        except Exception as exception:
            logger.info("File observer fell over watching because of the following exception:")
            logger.exception(exception)


def main() -> None:
    """Main function Create the file watcher and start watching for changes"""
    config = load_config()
    file_watcher = FileWatcher(config)
    file_watcher.start_watching()


if __name__ == "__main__":
    main()
