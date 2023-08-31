# pylint: disable=too-many-arguments, too-many-instance-attributes
"""
The module responsible for handling Last run detection including the LastRunDetector class and the creation of those
objects
"""

import datetime
import os
import re
from pathlib import Path
from time import sleep
from typing import Callable

from file_watcher.database.db_updater import DBUpdater
from file_watcher.utils import logger


async def create_last_run_detector(
    archive_path: Path,
    instrument: str,
    callback: Callable,
    run_file_prefix: str,
    db_ip: str,
    db_username: str,
    db_password: str,
):
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
    lrd = LastRunDetector(archive_path, instrument, callback, run_file_prefix, db_ip, db_username, db_password)
    await lrd._init()  # pylint: disable=protected-access
    return lrd


class LastRunDetector:
    """
    The last run detector is a class to detect when a new run has occured and callback, then recover lost runs that
    were missed.
    """

    def __init__(
        self,
        archive_path: Path,
        instrument: str,
        async_callback: Callable,
        run_file_prefix: str,
        db_ip: str,
        db_username: str,
        db_password: str,
    ):
        self.instrument = instrument
        self.run_file_prefix = run_file_prefix
        self.async_callback = async_callback
        self.archive_path = archive_path
        self.last_run_file = archive_path.joinpath(instrument).joinpath("Instrument/logs/lastrun.txt")
        self.last_recorded_run_from_file = self.get_last_run_from_file()
        logger.info(
            "Last run in lastrun.txt for instrument %s is: %s", self.instrument, self.last_recorded_run_from_file
        )
        self.last_cycle_folder_check = datetime.datetime.now()
        self.latest_cycle = self.get_latest_cycle()

        # Database setup and checks if runs missed then recovery
        self.db_updater = DBUpdater(ip=db_ip, username=db_username, password=db_password)
        self.latest_known_run_from_db = self.get_latest_run_from_db()
        logger.info("Last run in DB is: %s", self.latest_known_run_from_db)
        if self.latest_known_run_from_db is None:
            logger.info("Adding latest run to DB as there is no data: %s", self.last_recorded_run_from_file)
            self.update_db_with_latest_run(self.last_recorded_run_from_file)
            self.latest_known_run_from_db = self.last_recorded_run_from_file

    async def _init(self):
        """
        This function needs to be called before any other function and is the equivalent of a setup, it has to be
        done outside __init__ because it is an Async function, and async functionality cannot be completed inside
        __init__.
        """
        if int(self.latest_known_run_from_db) < int(self.last_recorded_run_from_file):
            logger.info(
                "Recovering lost runs between %s and%s", self.last_recorded_run_from_file, self.latest_known_run_from_db
            )
            await self.recover_lost_runs(self.latest_known_run_from_db, self.last_recorded_run_from_file)
            self.latest_known_run_from_db = self.last_recorded_run_from_file

    def get_latest_run_from_db(self) -> str:
        """
        Retrieve the latest run from the database
        :return: Return the latest run for the instrument that is set on this object
        """
        # This likely contains NDX<INSTNAME> so remove the NDX and go for it with the DB
        actual_instrument = self.instrument[3:]
        return self.db_updater.get_latest_run(actual_instrument)

    async def watch_for_new_runs(self, run_once=False):
        """
        This is the main loop for waiting for new runs and triggering
        :param run_once: Defaults to False, and will only run the loop once, aimed at simplicity for testing.
        """
        logger.info("Starting watcher...")
        run = True
        while run:
            if run_once:
                run = False
            current_time = datetime.datetime.now()
            time_between_cycle_folder_checks = current_time - self.last_cycle_folder_check
            # If it's been 6 hours do another check for the latest folder
            if time_between_cycle_folder_checks.total_seconds() > 21600:
                self.latest_cycle = self.get_latest_cycle()
                self.last_cycle_folder_check = current_time

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
                # If difference > 1 then try to recover potentially missed runs:
                if int(run_in_file) - int(self.last_recorded_run_from_file) > 1:
                    await self.recover_lost_runs(self.last_recorded_run_from_file, run_in_file)
                else:
                    await self.new_run_detected(run_in_file)

            sleep(0.1)

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

    async def new_run_detected(self, run_number: str, run_path: Path = None) -> None:
        if run_path is None and run_number is not None:
            try:
                run_path = self.generate_run_path(run_number)
            except FileNotFoundError as exception:
                logger.exception(exception)
                return
        await self.async_callback(run_path)
        self.update_db_with_latest_run(run_number)
        self.last_recorded_run_from_file = run_number

    def get_last_run_from_file(self) -> str:
        """
        Retrieve the last run from the instrument log's file.
        :return: The middle of the file, specifically the last run that was in the file.
        """
        with open(self.last_run_file, mode="r", encoding="utf-8") as last_run:
            line_parts = last_run.readline().split()
            if len(line_parts) != 3:
                raise RuntimeError(f"Unexpected last run file format for '{self.last_run_file}'")
        return line_parts[1]

    async def recover_lost_runs(self, earlier_run: str, later_run: str):
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
            if len(initial_zeros) > 1:
                actual_run_number_1_less_zero = actual_run_number[1:]
            else:
                # In the case where there are no 0s don't handle it at all.
                actual_run_number_1_less_zero = actual_run_number

            try:
                # Generate run_path which checks that path is genuine and exists
                run_path = self.generate_run_path(actual_run_number)
                await self.new_run_detected(actual_run_number, run_path=run_path)
            except FileNotFoundError as exception:
                try:
                    if actual_run_number_1_less_zero != actual_run_number:
                        alt_run_path = self.generate_run_path(actual_run_number_1_less_zero)
                        await self.new_run_detected(actual_run_number_1_less_zero, run_path=alt_run_path)
                    else:
                        raise FileNotFoundError(
                            "Alt run path does not exist, and neither does original path, "
                            f"run number: {actual_run_number} does not exist as a .nxs file in "
                            "latest cycle."
                        ) from exception
                except FileNotFoundError as exception:
                    logger.exception(exception)

    def update_db_with_latest_run(self, run_number):
        """
        Change the details in the database to reflect this number
        :param run_number: The run number to be put into the database to reflect the most recent detected work
        """
        # This likely contains NDX<INSTNAME> so remove the NDX and go for it with the DB
        actual_instrument = self.instrument[3:]
        self.db_updater.update_latest_run(actual_instrument, run_number)

    def find_file_in_instruments_data_folder(self, run_number: str) -> Path:
        """
        Slow but guaranteed to find the file if it exists.
        :param run_number: The run number you need to go and find
        :return: The Path if it exists of the run number
        """
        instrument_dir = self.archive_path.joinpath(self.instrument).joinpath("Instrument/data")
        return list(instrument_dir.rglob(f"cycle_??_?/*{run_number}.nxs"))[0]

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
        all_cycles = os.listdir(f"{self.archive_path}/NDXWISH/instrument/data/")
        all_cycles.sort()
        try:
            most_recent_cycle = all_cycles[-1]
        except IndexError as exc:
            raise FileNotFoundError(f"No cycles present in archive path: {self.archive_path}") from exc
        logger.info("Latest cycle found: %s", most_recent_cycle)
        return most_recent_cycle
