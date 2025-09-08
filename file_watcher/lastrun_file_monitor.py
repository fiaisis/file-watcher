"""
The module responsible for handling Last run detection including the LastRunDetector class and the creation of those
objects
"""

import datetime
import os
import re
import time
from http import HTTPStatus
from pathlib import Path
from time import sleep
from typing import Any, Callable, Literal, Union, cast

import requests
from requests import HTTPError

from file_watcher.utils import logger


class LastRunDetector:
    """
    The last run detector is a class to detect when a new run has occured and callback, then recover lost runs that
    were missed.
    """

    def __init__(
        self,
        archive_path: Path,
        instrument: str,
        callback: Callable[[Path | None], None],
        run_file_prefix: str,
        fia_api_url: str,
        fia_api_api_key: str,
        request_timeout_length: int = 1,
    ):
        self.instrument = instrument
        self.fia_api_url = fia_api_url
        self.fia_api_api_key = fia_api_api_key
        self.run_file_prefix = run_file_prefix
        self.callback = callback
        self.archive_path = archive_path
        self.last_run_file = archive_path.joinpath(instrument).joinpath("Instrument/logs/lastrun.txt")
        self.last_recorded_run_from_file = self.get_last_run_from_file()
        self.request_timeout_length = request_timeout_length
        logger.info(
            "Last run in lastrun.txt for instrument %s is: %s",
            self.instrument,
            self.last_recorded_run_from_file,
        )
        self.last_cycle_folder_check = datetime.datetime.now(datetime.UTC)
        self.latest_cycle = self.get_latest_cycle()

        # FIA API get last run setup and checks if runs missed then recovery
        try:
            self.latest_known_run_from_fia = self.get_latest_run_from_fia_api(self.instrument)
        except HTTPError as e:
            logger.fatal("Failed to get last run from FIA API: ", e)
            raise e

        logger.info("Last run from FIA is: %s", self.latest_known_run_from_fia)
        if self.latest_known_run_from_fia is None or self.latest_known_run_from_fia == "None":
            logger.info(
                "Adding latest run to FIA as there is no data: %s",
                self.last_recorded_run_from_file,
            )
            self.update_latest_run_to_fia_api(self.last_recorded_run_from_file)
            self.latest_known_run_from_fia = self.last_recorded_run_from_file
        if (
            self.latest_known_run_from_fia is not None
            and self.last_recorded_run_from_file is not None
            and int(self.latest_known_run_from_fia) < int(self.last_recorded_run_from_file)
        ):
            logger.info(
                "Recovering lost runs between %s and%s",
                self.last_recorded_run_from_file,
                self.latest_known_run_from_fia,
            )
            self.recover_lost_runs(self.latest_known_run_from_fia, self.last_recorded_run_from_file)
            self.latest_known_run_from_fia = self.last_recorded_run_from_file

    def retry_api_request(
        self,
        url_request_string: str,
        method: Literal["GET", "POST", "PATCH", "PUT", "DELETE"],
        body: dict[Any, Any] | None = None,
        retry_attempts: int = 5,
    ) -> requests.Response:
        """
        A helper function to handle multiple attempts at HTTP Requests
        :param url_request_string: url of FIA API to get/put
        :param method: GET or POST
        :param body: dictionary to POST
        :param retry_attempts: number of times to retry the request
        :return: a requests.Response object for handling
        """
        attempts = 0
        while attempts < retry_attempts:
            req = requests.request(
                method=method,
                url=url_request_string,
                timeout=self.request_timeout_length,
                headers={"Authorization": f"Bearer {self.fia_api_api_key}"},
                json=body,
            )
            attempts += 1
            if req.status_code == HTTPStatus.OK:
                return req
            if retry_attempts == attempts:
                return req
            # increment sleep time as retries persist
            time.sleep(5 * self.request_timeout_length)

        # if retries failed, try one last time and return a response object for handling
        return requests.request(method=method, url=url_request_string, timeout=30)

    def get_latest_run_from_fia_api(self, instrument: str) -> str | None:
        """
        Retrieve the latest run using FIA API
        :param instrument: name of instrument
        :return: Return the latest run for the instrument that is set on this objector or None if unsuccessful
        """
        instrument_name = instrument if not instrument.startswith("NDX".upper()) else instrument[3:]
        try:
            request = self.retry_api_request(
                url_request_string=f"{self.fia_api_url}/instrument/{instrument_name}/latest-run", method="GET"
            )

            request.raise_for_status()
            return cast("str | None", request.json()["latest_run"])

        except HTTPError as e:
            logger.warning("Failed to get latest run: ", exc_info=e)
            raise e

    def update_latest_run_to_fia_api(self, run_number: str) -> str:
        """
        Update FIA API with the latest run
        :param run_number: number of run to update to FIA API
        :return: confirmation string if successful
        """
        # Use request library to FIA API
        instrument_name = self.instrument[3:]
        try:
            request = self.retry_api_request(
                url_request_string=f"{self.fia_api_url}/instrument/{instrument_name}/latest-run",
                method="PUT",
                body={"latest_run": run_number},
            )

            request.raise_for_status()
            return f"Latest run update: run number {run_number}"

        except HTTPError as e:
            logger.warning("Failed to update latest run: ", exc_info=e)
            raise e

    def watch_for_new_runs(self, callback_func: Callable[[], None], run_once: bool = False) -> None:
        """
        This is the main loop for waiting for new runs and triggering
        :param callback_func: This is a callable that is called once per loop
        :param run_once: Defaults to False, and will only run the loop once, aimed at simplicity for testing.
        """
        logger.info("Starting watcher...")
        run = True
        while run:
            if run_once:
                run = False
            self._check_for_new_cycle_folder()
            callback_func()
            try:
                run_in_file = self.get_last_run_from_file()
            except RuntimeError as exception:
                # Correctly handle problems with parsing the last run file by just trying again, this is likely an
                # error in file integrity and should be fixed by the time we check again but happens often enough
                # for this fix to be necessary
                logger.exception(exception)
                continue
            if run_in_file != self.last_recorded_run_from_file:
                logger.info("New run detected: %s", run_in_file)
                self._check_for_missed_runs(run_in_file)

            sleep(0.1)

    def _check_for_missed_runs(self, run_in_file: str) -> None:
        # If difference > 1 then try to recover potentially missed runs:
        if int(run_in_file) - int(self.last_recorded_run_from_file) > 1:
            self.recover_lost_runs(self.last_recorded_run_from_file, run_in_file)
        else:
            self.new_run_detected(run_in_file)

    def _check_for_new_cycle_folder(self) -> None:
        current_time = datetime.datetime.now(datetime.UTC)
        time_between_cycle_folder_checks = current_time - self.last_cycle_folder_check
        # If it's been 6 hours do another check for the latest folder
        if time_between_cycle_folder_checks.total_seconds() > 21600:  # noqa: PLR2004
            self.latest_cycle = self.get_latest_cycle()
            self.last_cycle_folder_check = current_time

    def generate_run_path(self, run_number: str) -> Path:
        """
        Generate the path and verify it exists, if it does not exist then search for it, if it can't be found then
        raise a FileNotFoundError exception.
        :param run_number: The run number to generate and check for
        :return: The path to the file for the run number
        """
        path = (
            self.archive_path.joinpath(self.instrument)
            .joinpath("Instrument/data")
            .joinpath(self.latest_cycle)
            .joinpath(self.run_file_prefix + run_number + ".nxs")
        )
        if not path.exists():
            try:
                path = self.find_file_in_instruments_data_folder(run_number)
            except Exception as exc:
                raise FileNotFoundError(f"This run number doesn't have a file: {run_number}") from exc
        return path

    def new_run_detected(self, run_number: str, run_path: Union[Path, None] = None) -> None:
        """
        Called when a new run is detected, you can use the run number OR the run path
        :param run_number: The run number that was detected or
        :param run_path: The run path that was detected
        """
        if run_path is None and run_number is not None:
            try:
                run_path = self.generate_run_path(run_number)
            except FileNotFoundError as exception:
                logger.exception(exception)
                return
        self.callback(run_path)
        self.update_latest_run_to_fia_api(run_number)
        self.last_recorded_run_from_file = run_number

    def get_last_run_from_file(self) -> str:
        """
        Retrieve the last run from the instrument log's file.
        :return: The middle of the file, specifically the last run that was in the file.
        """
        with Path(self.last_run_file).open(mode="r", encoding="utf-8") as last_run:
            line_parts = last_run.readline().split()
            if len(line_parts) != 3:  # noqa: PLR2004
                raise RuntimeError(f"Unexpected last run file format for '{self.last_run_file}'")
        return line_parts[1]

    def recover_lost_runs(self, earlier_run: str, later_run: str) -> None:
        """
        The aim is to send all the runs that have not been sent, in between the two passed run numbers, it will also
        submit the value for later_run
        :param earlier_run: The run that was submitted to the FIA API last
        :param later_run: The run that was last detected
        """

        def grab_zeros_from_beginning_of_string(string: str) -> str:
            match = re.match(r"^0*", string)
            if match:
                return match.group(0)
            return ""

        initial_zeros = grab_zeros_from_beginning_of_string(earlier_run)

        for run in range(int(earlier_run) + 1, int(later_run) + 1):
            # If file exists new run detected
            actual_run_number = initial_zeros + str(run)

            # Handle edge case where 1 less zero is needed when the numbers roll over
            actual_run_number_1_less_zero = actual_run_number[1:] if len(initial_zeros) > 1 else actual_run_number

            try:
                # Generate run_path which checks that path is genuine and exists
                run_path = self.generate_run_path(actual_run_number)
                self.new_run_detected(actual_run_number, run_path=run_path)
            except FileNotFoundError as exception:
                try:
                    if actual_run_number_1_less_zero != actual_run_number:
                        alt_run_path = self.generate_run_path(actual_run_number_1_less_zero)
                        self.new_run_detected(actual_run_number_1_less_zero, run_path=alt_run_path)
                    else:
                        raise FileNotFoundError(
                            "Alt run path does not exist, and neither does original path, "
                            f"run number: {actual_run_number} does not exist as a .nxs file in "
                            "latest cycle."
                        ) from exception
                except FileNotFoundError as exception_:
                    logger.exception(exception_)

    def find_file_in_instruments_data_folder(self, run_number: str) -> Path:
        """
        Slow but guaranteed to find the file if it exists.
        :param run_number: The run number you need to go and find
        :return: The Path if it exists of the run number
        """
        instrument_dir = self.archive_path.joinpath(self.instrument).joinpath("Instrument/data")
        return next(instrument_dir.rglob(f"cycle_??_?/*{run_number}.nxs"))

    def get_latest_cycle(self) -> str:
        """
        Gets the latest cycle, uses NDXWISH as the bases of it, as it is a TS2 instrument and allows for
        significantly reduced complications vs TS1 instruments who collected data in cycles_98_1 and so on (centuries
        and all that being rather complicated for a machine to understand without appropriate context).
        :return: The latest cycle in the WISH folder.
        """
        logger.info("Finding latest cycle...")
        # Use WISH (or any other TS2 instrument as their data started in 2008 and avoids the 98/99 issue of TS1
        # instruments) to determine which is the most recent cycle.
        all_cycles = os.listdir(f"{self.archive_path}/NDXWISH/instrument/data/")  # noqa: PTH208
        all_cycles.sort()
        try:
            most_recent_cycle = all_cycles[-1]
        except IndexError as exc:
            raise FileNotFoundError(f"No cycles present in archive path: {self.archive_path}") from exc
        logger.info("Latest cycle found: %s", most_recent_cycle)
        return most_recent_cycle


def create_last_run_detector(
    archive_path: Path,
    instrument: str,
    callback: Callable[[Path | None], None],
    run_file_prefix: str,
    fia_api_url: str,
    fia_api_api_key: str,
    request_timeout_length: int = 1,
) -> LastRunDetector:
    """
    Create asynchronously the LastRunDetector object,
    :param archive_path: The path to the archive on this host
    :param instrument: The instrument folder to be used in the archive
    :param callback: The function to be called when a new run is detected
    :param run_file_prefix: The prefix for the .nxs files e.g. MAR for MARI or WISH for WISH
    :param fia_api_url: URL of the FIA API
    :param fia_api_api_key: API KEY of the FIA API for Authorisation
    :return:
    """
    return LastRunDetector(
        archive_path,
        instrument,
        callback,
        run_file_prefix,
        fia_api_url,
        fia_api_api_key,
        request_timeout_length,
    )
