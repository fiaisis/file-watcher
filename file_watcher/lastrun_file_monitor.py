"""
The module responsible for handling Last run detection including the LastRunDetector class and the creation of those
objects
"""

import datetime
import os
import re
from pathlib import Path
from time import sleep
from typing import Callable, Union

from file_watcher.database.db_updater import DBUpdater
from file_watcher.utils import logger


class LastRunDetector:
    """
    The last run detector is a class to detect when a new run has occured and callback, then recover lost runs that
    were missed.
    """

    def __init__(
        self,
        last_run_file: Path,
        instrument: str,
        callback: Callable[[Path | None], None],
        run_file_prefix: str,
        db_ip: str,
        db_username: str,
        db_password: str,
    ):
        self.instrument = instrument
        self.run_file_prefix = run_file_prefix
        self.callback = callback
        self.last_run_file = last_run_file
        self.instrument_data_path = last_run_file.parent.parent.joinpath("data")
        self.last_recorded_run_from_file = self.get_last_run_from_file()
        logger.info(
            "Last run in lastrun.txt for instrument %s is: %s",
            self.instrument,
            self.last_recorded_run_from_file,
        )
        self.last_cycle_folder_check = datetime.datetime.now(datetime.UTC)
        self.latest_cycle = self.get_latest_cycle()

        # Database setup and checks if runs missed then recovery
        self.db_updater = DBUpdater(ip=db_ip, username=db_username, password=db_password)
        self.latest_known_run_from_db = self.get_latest_run_from_db()
        logger.info("Last run in DB is: %s", self.latest_known_run_from_db)
        if self.latest_known_run_from_db is None or self.latest_known_run_from_db == "None":
            logger.info(
                "Adding latest run to DB as there is no data: %s",
                self.last_recorded_run_from_file,
            )
            self.update_db_with_latest_run(self.last_recorded_run_from_file)
            self.latest_known_run_from_db = self.last_recorded_run_from_file
        if (
            self.latest_known_run_from_db is not None
            and self.last_recorded_run_from_file is not None
            and int(self.latest_known_run_from_db) < int(self.last_recorded_run_from_file)
        ):
            logger.info(
                "Recovering lost runs between %s and%s",
                self.last_recorded_run_from_file,
                self.latest_known_run_from_db,
            )
            self.recover_lost_runs(self.latest_known_run_from_db, self.last_recorded_run_from_file)
            self.latest_known_run_from_db = self.last_recorded_run_from_file

    def get_latest_run_from_db(self) -> Union[str, None]:
        """
        Retrieve the latest run from the database
        :return: Return the latest run for the instrument that is set on this object
        """
        # This likely contains NDX<INSTNAME> so remove the NDX and go for it with the DB
        actual_instrument = self.instrument[3:]
        return self.db_updater.get_latest_run(actual_instrument)

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
        path = (self.instrument_data_path.joinpath(self.latest_cycle)
                .joinpath(self.run_file_prefix + run_number + ".nxs"))
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
        self.update_db_with_latest_run(run_number)
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
        :param earlier_run: The run that was submitted to the db last
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

    def update_db_with_latest_run(self, run_number: str) -> None:
        """
        Change the details in the database to reflect this number
        :param run_number: The run number to be put into the database to reflect the most recent detected work
        """
        # This likely contains NDX<INSTNAME> so remove the NDX and go for it with the DB
        actual_instrument = self.instrument[3:]
        self.db_updater.update_latest_run(actual_instrument, int(run_number))

    def find_file_in_instruments_data_folder(self, run_number: str) -> Path:
        """
        Slow but guaranteed to find the file if it exists.
        :param run_number: The run number you need to go and find
        :return: The Path if it exists of the run number
        """
        return next(self.instrument_data_path.rglob(f"cycle_??_?/*{run_number}.nxs"))

    def get_latest_cycle(self) -> str:
        """
        Get the latest cycle for the given instrument
        """
        logger.info("Finding latest cycle...")
        all_cycles = os.listdir(self.instrument_data_path)  # noqa: PTH208
        all_cycles.sort()
        try:
            most_recent_cycle = all_cycles[-1]
        except IndexError as exc:
            raise FileNotFoundError(f"No cycles present in path: {self.instrument_data_path}") from exc
        logger.info("Latest cycle found: %s", most_recent_cycle)
        return most_recent_cycle


def create_last_run_detector(
    archive_path: Path,
    instrument: str,
    callback: Callable[[Path | None], None],
    run_file_prefix: str,
    db_ip: str,
    db_username: str,
    db_password: str,
) -> LastRunDetector:
    """
    Create asynchronously the LastRunDetector object,
    :param archive_path: The path to the archive on this host
    :param instrument: The instrument folder to be used in the archive
    :param callback: The function to be called when a new run is detected
    :param run_file_prefix: The prefix for the .nxs files e.g. MAR for MARI or WISH for WISH
    :param db_ip: The ip of the database
    :param db_username: The username used for the database
    :param db_password: The password used for the database
    :return:
    """
    return LastRunDetector(
        archive_path,
        instrument,
        callback,
        run_file_prefix,
        db_ip,
        db_username,
        db_password,
    )
