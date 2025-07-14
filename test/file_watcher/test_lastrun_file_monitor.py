import datetime
import logging
import tempfile
import unittest
from http import HTTPStatus
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, call, patch

from file_watcher.lastrun_file_monitor import create_last_run_detector
from file_watcher.utils import logger
from test.file_watcher.utils import AwaitableNonAsyncMagicMock


class LastRunFileMonitorTest(unittest.TestCase):
    def setUp(self):
        self.db_updater_patch = patch("file_watcher.lastrun_file_monitor.DBUpdater")
        self.db_updater_mock = self.db_updater_patch.start()
        self.create_instrument_files()
        self.callback = MagicMock()
        self.run_file_prefix = MagicMock()
        self.db_ip = MagicMock()
        self.db_username = MagicMock()
        self.db_password = MagicMock()
        self.fia_api_url = MagicMock()

    def tearDown(self):
        self.db_updater_patch.stop()
        self.archive_temp_dir.cleanup()

    def create_instrument_files(self):
        # Create this instrument
        self.archive_temp_dir = tempfile.TemporaryDirectory()
        self.archive_path = Path(self.archive_temp_dir.name)
        self.path = Path(self.archive_path, "instrument_name")
        self.instrument = "instrument_name"
        self.path = self.path / "Instrument" / "logs"
        self.path.mkdir(parents=True, exist_ok=True)
        self.path = self.path / "lastrun.txt"
        with Path(self.path).open("w+") as file:
            file.write(f"{self.instrument} 0001 0")

        # Ensure wish exists
        wish_path = self.archive_path / "NDXWISH" / "instrument" / "data" / "cycle_23_2"
        wish_path.mkdir(parents=True, exist_ok=True)

    def mock_response(self):
        """
        Return a mock response object for testing.
        """
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {}
        return response

    @patch("file_watcher.lastrun_file_monitor.LastRunDetector")
    def test_create_last_run_detector(self, mock_lrd):
        create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )

        mock_lrd.assert_called_once_with(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )

    def test_get_latest_run_from_db(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )

        self.lrd.get_latest_run_from_db()

        self.db_updater_mock.return_value.get_latest_run.assert_called_with(self.instrument[3:])
        assert self.db_updater_mock.return_value.get_latest_run.call_count == 2  # noqa: PLR2004

    @patch("file_watcher.lastrun_file_monitor.requests.request")
    def test_get_latest_run_from_fia(self, mock_get):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        mock_response = MagicMock()
        mock_get.return_value = mock_response
        mock_response.status_code = 200
        mock_response.json.return_value = {"latest_run": 1234}
        self.lrd.get_latest_run_from_fia("MARI")

        assert self.lrd.get_latest_run_from_fia("MARI") == 1234

    @patch("file_watcher.lastrun_file_monitor.requests.get")
    def test_get_latest_run_from_fia_raises_exception(self, mock_get):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        mock_response = MagicMock()
        mock_get.return_value = mock_response
        mock_response.status_code = HTTPStatus.FORBIDDEN

        with self.assertRaises(Exception):
            self.lrd.get_latest_run_from_fia("MARI")

    def test_watch_for_new_runs_checks_for_latest_cycle_after_6_hours(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        now = datetime.datetime.now(datetime.UTC)
        self.lrd.last_cycle_folder_check = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            seconds=21601
        )  # Set to over 6 hours ago
        self.lrd.get_latest_cycle = MagicMock()

        assert self.lrd.get_latest_cycle.call_count == 0

        self.lrd.watch_for_new_runs(mock.MagicMock(), run_once=True)

        assert self.lrd.get_latest_cycle.call_count == 1
        assert self.lrd.last_cycle_folder_check > now

    def test_watch_for_new_runs_checks_contents_of_last_run_file(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.get_last_run_from_file = MagicMock()

        self.lrd.watch_for_new_runs(mock.MagicMock(), run_once=True)

        self.lrd.get_last_run_from_file.assert_called_once_with()

    def test_watch_for_new_runs_calls_callback_func(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        callback_func = mock.MagicMock()

        self.lrd.watch_for_new_runs(callback_func, run_once=True)

        callback_func.assert_called_once_with()

    def test_watch_for_new_runs_handles_exceptions_from_get_last_run_from_file(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        exception = RuntimeError("EXCEPTIONS!")

        def raise_exception():
            raise exception

        self.lrd.get_last_run_from_file = MagicMock(side_effect=raise_exception)

        with patch("file_watcher.lastrun_file_monitor.logger") as logger:
            self.lrd.watch_for_new_runs(mock.MagicMock(), run_once=True)

            self.lrd.get_last_run_from_file.assert_called_once_with()

            logger.exception.assert_called_once_with(exception)

    def test_latest_run_is_new(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.get_last_run_from_file = MagicMock(return_value="0002")
        self.lrd.new_run_detected = AwaitableNonAsyncMagicMock()

        self.lrd.watch_for_new_runs(mock.MagicMock(), run_once=True)

        self.lrd.get_last_run_from_file.assert_called_once_with()
        self.lrd.new_run_detected.assert_called_once_with("0002")

    def test_latest_run_is_new_and_more_than_one_file(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.get_last_run_from_file = MagicMock(return_value="0003")
        self.lrd.recover_lost_runs = AwaitableNonAsyncMagicMock()

        self.lrd.watch_for_new_runs(mock.MagicMock(), run_once=True)

        self.lrd.get_last_run_from_file.assert_called_once_with()
        self.lrd.recover_lost_runs.assert_called_with("0001", "0003")

    def test_latest_run_is_not_new(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.get_last_run_from_file = MagicMock(return_value="0001")
        self.lrd.recover_lost_runs = AwaitableNonAsyncMagicMock()
        self.lrd.new_run_detected = AwaitableNonAsyncMagicMock()

        self.lrd.watch_for_new_runs(mock.MagicMock(), run_once=True)

        self.lrd.get_last_run_from_file.assert_called_once_with()
        self.lrd.recover_lost_runs.assert_not_called()
        self.lrd.new_run_detected.assert_not_called()

    def test_generate_run_path_functions_as_expected(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.lastest_cycle = "cycle_23_2"
        self.lrd.run_file_prefix = "TMP"
        expected_path = self.path.parent.parent / "data" / "cycle_23_2"
        expected_path.mkdir(parents=True, exist_ok=True)
        expected_path = expected_path / f"{self.lrd.run_file_prefix}0001.nxs"
        with Path(expected_path).open("+w") as file:
            file.write("HELLO!")

        returned_path = self.lrd.generate_run_path("0001")

        assert expected_path == returned_path

    def test_generate_run_path_handles_the_file_not_existing(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )

        def raise_exception():
            raise Exception()

        self.lrd.find_file_in_instruments_data_folder = MagicMock(side_effect=raise_exception)

        with self.assertRaises(FileNotFoundError):  # noqa: PT027
            self.lrd.generate_run_path("0001")

    def test_generate_run_path_handles_the_file_not_existing_where_expected_but_in_another_folder(
        self,
    ):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        expected_path = MagicMock()
        self.lrd.find_file_in_instruments_data_folder = MagicMock(return_value=expected_path)

        returned_path = self.lrd.generate_run_path("0001")

        self.lrd.find_file_in_instruments_data_folder.assert_called_once_with("0001")
        assert expected_path == returned_path

    def test_new_run_detected_handles_just_run_number(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.update_db_with_latest_run = MagicMock()
        run_path = MagicMock()
        self.lrd.generate_run_path = MagicMock(return_value=run_path)

        self.lrd.new_run_detected("0001")

        self.lrd.update_db_with_latest_run.assert_called_once_with("0001")

    def test_new_run_detected_handles_file_not_found_by_generate_run_path(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        exception = FileNotFoundError("FILE NOT FOUND!")

        def raise_file_not_found(_):
            raise exception

        self.lrd.generate_run_path = MagicMock(side_effect=raise_file_not_found)

        with patch("file_watcher.lastrun_file_monitor.logger") as logger:
            self.lrd.new_run_detected(run_path=None, run_number="0001")

            logger.exception.assert_called_once_with(exception)

        self.lrd.generate_run_path.assert_called_once_with("0001")

    def test_get_last_run_from_file_raises_when_file_formatted_poorly(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"Hello world!")
            path = Path(fp.name)

        self.lrd.last_run_file = path

        self.assertRaises(RuntimeError, self.lrd.get_last_run_from_file)  # noqa: PT027

    def test_recover_lost_runs_finds_runs_that_were_lost(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.generate_run_path = MagicMock(return_value=Path("/run/path/NDXMARI/MAR001"))

        self.lrd.recover_lost_runs("0001", "0003")

        assert self.lrd.generate_run_path.call_args_list == [call("0002"), call("0003")]
        assert self.lrd.generate_run_path.call_count == 2  # noqa: PLR2004

    def test_recover_lost_runs_handles_file_not_found_twice(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        exception = FileNotFoundError("FILE NOT FOUND!")

        def raise_file_not_found(_):
            raise exception

        self.lrd.generate_run_path = MagicMock(side_effect=raise_file_not_found)

        with patch("file_watcher.lastrun_file_monitor.logger") as logger:
            self.lrd.recover_lost_runs("0001", "0003")
            logger.exception.assert_called_with(exception)
            assert logger.exception.call_count == 2  # noqa: PLR2004

        assert self.lrd.generate_run_path.call_args_list == [call("0002"), call("002"), call("0003"), call("003")]
        assert self.lrd.generate_run_path.call_count == 4  # noqa: PLR2004

    def test_update_db_with_latest_run_sends_instrument_minus_ndx(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        self.lrd.instrument = "NDXMARI"
        self.lrd.db_updater.update_latest_run = MagicMock()

        self.lrd.update_db_with_latest_run("0001")

        self.lrd.db_updater.update_latest_run.assert_called_once_with("MARI", 1)

    def test_find_file_in_instruments_data_folder_finds_file_in_instrument_data_folder(
        self,
    ):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        run_number = "0001"
        self.lrd.archive_path = MagicMock()
        instrument_dir = self.lrd.archive_path.joinpath.return_value.joinpath.return_value
        instrument_dir.rglob = MagicMock(return_value=iter(["banana"]))
        return_value = self.lrd.find_file_in_instruments_data_folder(run_number)

        self.lrd.archive_path.joinpath.assert_called_once_with(self.lrd.instrument)
        self.lrd.archive_path.joinpath.return_value.joinpath.assert_called_once_with("Instrument/data")
        instrument_dir.rglob.assert_called_once_with(f"cycle_??_?/*{run_number}.nxs")

        assert return_value == "banana"

    def test_get_latest_cycle_handles_lack_of_cycles_in_archive(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = Path(tmpdirname)
            self.lrd.archive_path = path
            path = path / "NDXWISH" / "instrument" / "data"
            path.mkdir(parents=True, exist_ok=True)

            self.assertRaises(FileNotFoundError, self.lrd.get_latest_cycle)  # noqa: PT027

    def test_get_latest_cycle_finds_latest_cycle(self):
        self.lrd = create_last_run_detector(
            self.archive_path,
            self.instrument,
            self.callback,
            self.run_file_prefix,
            self.db_ip,
            self.db_username,
            self.db_password,
            self.fia_api_url,
        )
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = Path(tmpdirname)
            self.lrd.archive_path = path
            path = path / "NDXWISH" / "instrument" / "data" / "cycle_25_2"
            path.mkdir(parents=True, exist_ok=True)

            latest_cycle = self.lrd.get_latest_cycle()

            assert latest_cycle == "cycle_25_2"
