import os
import tempfile
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

from file_watcher.main import FileWatcher, load_config, main
from test.file_watcher.utils import AwaitableNonAsyncMagicMock


@pytest.fixture
def config():
    return load_config()


@pytest.fixture
def file_watcher(config):
    return FileWatcher(config)


@pytest.fixture
def clear_environment():
    yield
    del os.environ["QUEUE_HOST"]
    del os.environ["QUEUE_USER"]
    del os.environ["QUEUE_PASSWORD"]
    del os.environ["EGRESS_QUEUE_NAME"]
    del os.environ["WATCH_DIR"]
    del os.environ["FILE_PREFIX"]
    del os.environ["INSTRUMENT_FOLDER"]
    del os.environ["FIA_API_URL"]
    del os.environ["FIA_API_API_KEY"]


def test_load_config_defaults(config):
    config = load_config()

    assert config.host == "localhost"
    assert config.username == "guest"
    assert config.password == "guest"  # noqa: S105
    assert config.queue_name == "watched-files"
    assert config.watch_dir == Path("/archive")
    assert config.run_file_prefix == "MAR"
    assert config.instrument_folder == "NDXMARI"
    assert config.fia_api_url == "localhost:8000"
    assert config.fia_api_api_key == "shh"


def test_load_config_environ_vars(config, clear_environment):
    host = str(mock.MagicMock())
    username = str(mock.MagicMock())
    password = str(mock.MagicMock())
    queue_name = str(mock.MagicMock())
    watch_dir = "/" + str(mock.MagicMock())
    run_file_prefix = str(mock.MagicMock())
    instrument_folder = str(mock.MagicMock())
    fia_api_url = str(mock.MagicMock())
    fia_api_api_key = str(mock.MagicMock())

    os.environ["QUEUE_HOST"] = host
    os.environ["QUEUE_USER"] = username
    os.environ["QUEUE_PASSWORD"] = password
    os.environ["EGRESS_QUEUE_NAME"] = queue_name
    os.environ["WATCH_DIR"] = watch_dir
    os.environ["FILE_PREFIX"] = run_file_prefix
    os.environ["INSTRUMENT_FOLDER"] = instrument_folder
    os.environ["FIA_API_URL"] = fia_api_url
    os.environ["FIA_API_API_KEY"] = fia_api_api_key

    config = load_config()

    assert config.host == host
    assert config.username == username
    assert config.password == password
    assert config.watch_dir == Path(watch_dir)
    assert config.run_file_prefix == run_file_prefix
    assert config.instrument_folder == instrument_folder
    assert config.fia_api_url == fia_api_url
    assert config.fia_api_api_key == fia_api_api_key


@patch("file_watcher.main.FileWatcher.producer_channel")
def test_file_watcher_on_event_produces_message(mock_producer, file_watcher):
    file_watcher.connect_to_broker = mock.MagicMock()
    file_watcher.is_connected = mock.MagicMock(return_value=True)

    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write(b"Hello world!")
        path = Path(fp.name)

    file_watcher.on_event(path)

    mock_producer.return_value.__enter__.return_value.basic_publish.assert_called_once_with(
        "watched-files", "", str(path).encode()
    )


@patch("file_watcher.main.logger")
@patch("file_watcher.main.FileWatcher.producer_channel")
def test_file_watcher_on_event_skips_dir_creation(mock_producer, logger, file_watcher):
    file_watcher.connect_to_broker = mock.MagicMock()

    with tempfile.TemporaryDirectory() as tmpdirname:
        path = Path(tmpdirname)
        file_watcher.on_event(path)
        logger.info.assert_called_with("Skipping directory creation for %s", tmpdirname)
        assert logger.info.call_count == 1
    mock_producer.assert_not_called()


@patch("file_watcher.main.write_readiness_probe_file")
@patch("file_watcher.main.create_last_run_detector")
@patch("file_watcher.main.logger")
def test_file_watcher_start_watching_handles_exceptions_from_watcher(
    mock_logger, mock_create_last_run_detector, mock_write_readiness_probe_file, file_watcher, config
):
    exception = Exception("CRAZY EXCEPTION!")

    def raise_exception(callback_func):
        raise exception

    mock_create_last_run_detector.return_value.watch_for_new_runs = AwaitableNonAsyncMagicMock(
        side_effect=raise_exception
    )

    # Should not raise, if raised it does not handle exceptions correctly
    file_watcher.start_watching()

    assert mock_create_last_run_detector.return_value.watch_for_new_runs.call_count == 1
    assert mock_create_last_run_detector.return_value.watch_for_new_runs.call_args.kwargs["callback_func"].args == (
        config.fia_api_url,  # this needs to be a tuple
    )
    assert (
        mock_create_last_run_detector.return_value.watch_for_new_runs.call_args.kwargs["callback_func"].func
        == mock_write_readiness_probe_file
    )
    mock_logger.info.assert_called_with("File observer fell over watching because of the following exception:")
    mock_logger.exception.assert_called_with(exception)


@patch("file_watcher.main.write_readiness_probe_file")
@patch("file_watcher.main.create_last_run_detector")
def test_file_watcher_start_watching_creates_last_run_detector(
    mock_create_last_run_detector, mock_write_readiness_probe_file, file_watcher, config
):
    file_watcher.start_watching()

    assert mock_create_last_run_detector.return_value.watch_for_new_runs.call_count == 1
    assert mock_create_last_run_detector.return_value.watch_for_new_runs.call_args.kwargs["callback_func"].args == (
        config.fia_api_url,  # this needs to be a tuple
    )
    assert (
        mock_create_last_run_detector.return_value.watch_for_new_runs.call_args.kwargs["callback_func"].func
        == mock_write_readiness_probe_file
    )


@patch("file_watcher.main.load_config")
@patch("file_watcher.main.FileWatcher")
def test_main(mock_watcher, mock_load_config):
    main()
    mock_load_config.assert_called_once()
    mock_watcher.assert_called_once_with(mock_load_config.return_value)
    mock_watcher.return_value.start_watching.assert_called_once()


@patch("file_watcher.main.PlainCredentials")
@patch("file_watcher.main.ConnectionParameters")
@patch("file_watcher.main.BlockingConnection")
def test_channel_producer(mock_connection, mock_conn_params, mock_creds, config):
    channel = mock_connection.return_value.channel.return_value
    with FileWatcher(config).producer_channel():
        mock_creds.assert_called_once_with(username=config.username, password=config.password)
        mock_conn_params.assert_called_once_with(config.host, 5672, credentials=mock_creds.return_value)
        mock_connection.assert_called_once_with(mock_conn_params.return_value)
        channel.exchange_declare.assert_called_once_with(config.queue_name, exchange_type="direct", durable=True)
        channel.queue_declare.assert_called_once_with(
            config.queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        channel.queue_bind.assert_called_once_with(config.queue_name, config.queue_name, routing_key="")
    channel.close.assert_called_once()
    channel.connection.close.assert_called_once()
