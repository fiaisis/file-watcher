import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import patch

from file_watcher.main import load_config, FileWatcher, main, write_readiness_probe_file
from test.file_watcher.utils import AwaitableNonAsyncMagicMock


class MainTest(unittest.TestCase):
    def setUp(self):
        self.config = load_config()

    def test_load_config_defaults(self):
        config = load_config()

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.username, "guest")
        self.assertEqual(config.password, "guest")
        self.assertEqual(config.queue_name, "watched-files")
        self.assertEqual(config.watch_dir, Path("/archive"))
        self.assertEqual(config.run_file_prefix, "MAR")
        self.assertEqual(config.instrument_folder, "NDXMARI")
        self.assertEqual(config.db_ip, "localhost")
        self.assertEqual(config.db_username, "admin")
        self.assertEqual(config.db_password, "admin")

    def test_load_config_environ_vars(self):
        host = str(mock.MagicMock())
        username = str(mock.MagicMock())
        password = str(mock.MagicMock())
        queue_name = str(mock.MagicMock())
        watch_dir = "/" + str(mock.MagicMock())
        run_file_prefix = str(mock.MagicMock())
        instrument_folder = str(mock.MagicMock())
        db_ip = str(mock.MagicMock())
        db_username = str(mock.MagicMock())
        db_password = str(mock.MagicMock())

        os.environ["QUEUE_HOST"] = host
        os.environ["QUEUE_USER"] = username
        os.environ["QUEUE_PASSWORD"] = password
        os.environ["EGRESS_QUEUE_NAME"] = queue_name
        os.environ["WATCH_DIR"] = watch_dir
        os.environ["FILE_PREFIX"] = run_file_prefix
        os.environ["INSTRUMENT_FOLDER"] = instrument_folder
        os.environ["DB_IP"] = db_ip
        os.environ["DB_USERNAME"] = db_username
        os.environ["DB_PASSWORD"] = db_password

        config = load_config()

        self.assertEqual(config.host, host)
        self.assertEqual(config.username, username)
        self.assertEqual(config.password, password)
        self.assertEqual(config.watch_dir, Path(watch_dir))
        self.assertEqual(config.run_file_prefix, run_file_prefix)
        self.assertEqual(config.instrument_folder, instrument_folder)
        self.assertEqual(config.db_ip, db_ip)
        self.assertEqual(config.db_username, db_username)
        self.assertEqual(config.db_password, db_password)

    @patch("file_watcher.main.FileWatcher.producer_channel")
    def test_file_watcher_on_event_produces_message(self, mock_producer):
        self.file_watcher = FileWatcher(self.config)
        self.file_watcher.connect_to_broker = mock.MagicMock()
        self.file_watcher.is_connected = mock.MagicMock(return_value=True)

        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"Hello world!")
            path = Path(fp.name)

        self.file_watcher.on_event(path)
        mock_producer.return_value.__enter__.return_value.basic_publish.assert_called_once_with(
            "watched-files", "", str(path).encode()
        )

    @patch("file_watcher.main.logger")
    @patch("file_watcher.main.FileWatcher.producer_channel")
    def test_file_watcher_on_event_skips_dir_creation(self, mock_producer, logger):
        self.file_watcher = FileWatcher(self.config)
        self.file_watcher.connect_to_broker = mock.MagicMock()

        with tempfile.TemporaryDirectory() as tmpdirname:
            path = Path(tmpdirname)
            self.file_watcher.on_event(path)
            logger.info.assert_called_with("Skipping directory creation for %s", tmpdirname)
            self.assertEqual(logger.info.call_count, 1)
        mock_producer.assert_not_called()

    @patch("file_watcher.main.write_readiness_probe_file")
    @patch("file_watcher.main.create_last_run_detector")
    @patch("file_watcher.main.logger")
    def test_file_watcher_start_watching_handles_exceptions_from_watcher(
        self, mock_logger, mock_create_last_run_detector, mock_write_readiness_probe_file
    ):
        self.file_watcher = FileWatcher(self.config)
        exception = Exception("CRAZY EXCEPTION!")

        def raise_exception(callback_func):
            raise exception

        mock_create_last_run_detector.return_value.watch_for_new_runs = AwaitableNonAsyncMagicMock(
            side_effect=raise_exception
        )

        # Should not raise, if raised it does not handle exceptions correctly
        self.file_watcher.start_watching()

        mock_create_last_run_detector.return_value.watch_for_new_runs.assert_called_once_with(
            callback_func=mock_write_readiness_probe_file
        )
        mock_logger.info.assert_called_with("File observer fell over watching because of the following exception:")
        mock_logger.exception.assert_called_with(exception)

    @patch("file_watcher.main.create_last_run_detector")
    def test_file_watcher_start_watching_creates_last_run_detector(self, mock_create_last_run_detector):
        self.file_watcher = FileWatcher(self.config)

        with mock.patch("file_watcher.main.write_readiness_probe_file") as write_readiness_probe_file_mock:
            self.file_watcher.start_watching()

        mock_create_last_run_detector.return_value.watch_for_new_runs.assert_called_once_with(
            callback_func=write_readiness_probe_file_mock
        )

    @patch("file_watcher.main.load_config")
    @patch("file_watcher.main.FileWatcher")
    def test_main(self, mock_watcher, mock_load_config):
        main()
        mock_load_config.assert_called_once()
        mock_watcher.assert_called_once_with(mock_load_config.return_value)
        mock_watcher.return_value.start_watching.assert_called_once()

    @patch("file_watcher.main.PlainCredentials")
    @patch("file_watcher.main.ConnectionParameters")
    @patch("file_watcher.main.BlockingConnection")
    def test_channel_producer(self, mock_connection, mock_conn_params, mock_creds):
        channel = mock_connection.return_value.channel.return_value
        with FileWatcher(self.config).producer_channel():
            mock_creds.assert_called_once_with(username=self.config.username, password=self.config.password)
            mock_conn_params.assert_called_once_with(self.config.host, 5672, credentials=mock_creds.return_value)
            mock_connection.assert_called_once_with(mock_conn_params.return_value)
            channel.exchange_declare.assert_called_once_with(
                self.config.queue_name, exchange_type="direct", durable=True
            )
            channel.queue_declare.assert_called_once_with(
                self.config.queue_name, durable=True, arguments={"x-queue-type": "quorum"}
            )
            channel.queue_bind.assert_called_once_with(self.config.queue_name, self.config.queue_name, routing_key="")
        channel.close.assert_called_once()
        channel.connection.close.assert_called_once()
