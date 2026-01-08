import contextlib
import json
from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Literal, Optional, Set, cast

import requests

# Constants
MAX_SEARCH_WAIT = 3600  # Time in seconds
MAX_CONNECTION_RETRIES = 10  # Number of HTTP connection retries allowed
INITIAL_CONNECTION_RETRY_DELAY = 10  # Initial timeout in seconds
MAX_FAILURE_RATIO = 0.05  # 5% of total series can fail before aborting


def print_success(text: str) -> None:
    print(f"\033[92m{text}\033[0m")


def print_info(text: str) -> None:
    print(f"\033[96m{text}\033[0m")


def print_warning(text: str) -> None:
    print(f"\033[93m{text}\033[0m")


def print_error(text: str) -> None:
    print(f"\033[91m{text}\033[0m")


def input_bold(text: str) -> str:
    return input(f"\033[1m{text}\033[0m")


def input_warning(text: str) -> str:
    return input(f"\033[93m{text}\033[0m")


def msg(
    *text: str, type: Literal["info", "success", "warning", "error"] = "info"
) -> None:
    """Print a formatted multi-part message to the console."""
    _msg = "  —  ".join(text) if isinstance(text, tuple) else text
    if type == "info":
        print_info(_msg)
    elif type == "success":
        print_success(_msg)
    elif type == "warning":
        print_warning(_msg)
    elif type == "error":
        print_error(_msg)


def humanized_eta(seconds: int) -> str:
    """Convert seconds to a human-readable time estimate."""
    _min, sec = divmod(seconds, 60)
    _hr, min = divmod(_min, 60)
    days, hr = divmod(_hr, 24)
    time_parts = []

    days, hr, min, sec = round(days), round(hr), round(min), round(sec)

    if days > 0:
        time_parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hr > 0:
        time_parts.append(f"{hr} hour{'s' if hr != 1 else ''}")
    if min > 0:
        time_parts.append(f"{min} minute{'s' if min != 1 else ''}")
    if sec > 0:
        time_parts.append(f"{sec} second{'s' if sec != 1 else ''}")

    return " ".join(time_parts[:2]) if time_parts else "TBD"


def http_success(status_code: int) -> bool:
    return 200 <= status_code < 300


class StateManager:
    STATE_FILE = Path("downloader_state.json")

    def __init__(self):
        self.sonarr_url: str = ""
        self.api_key: str = ""
        self.series_ids: List[int] = []
        self.time_estimate: int = 0
        self.speed: Literal[0, 1, 2, 3] = 0
        self.total_completed: int = 0

    def load(self) -> bool:
        if self.STATE_FILE.is_file():
            try:
                with open(self.STATE_FILE, "r") as f:
                    state = json.load(f)
                    self.sonarr_url = state.get("sonarr_url", "")
                    self.api_key = state.get("api_key", "")
                    self.series_ids = state.get("series_ids", [])
                    self.time_estimate = state.get("time_estimate", 0)
                    self.speed = state.get("speed", 1)
                    self.total_completed = state.get("total_completed", 0)
                    if self.series_ids:
                        return True
            except json.JSONDecodeError:
                print_error("State file corrupted.")
        return False

    def save(self) -> None:
        state = {
            "sonarr_url": self.sonarr_url,
            "api_key": self.api_key,
            "time_estimate": self.time_estimate,
            "total_completed": self.total_completed,
            "series_ids": self.series_ids,
            "speed": self.speed,
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state, f)

    def clear(self) -> None:
        self.series_ids = []
        self.time_estimate = 0
        self.speed = 1
        self.total_completed = 0
        self.save()


class SonarrClient:
    def __init__(self, url: str, api_key: str):
        if not url:
            raise ValueError("Sonarr URL cannot be empty.")
        if not api_key:
            raise ValueError("Sonarr API key cannot be empty.")
        self.url = url.rstrip("/")
        self.api_key = api_key

    def _request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[requests.Response]:
        url = f"{self.url}/api/v3{endpoint}"
        params = kwargs.get("params", {})
        params["apikey"] = self.api_key
        kwargs["params"] = params
        for x in range(MAX_CONNECTION_RETRIES):
            delay = INITIAL_CONNECTION_RETRY_DELAY * (x + 1)
            with contextlib.suppress(Exception):
                response = requests.request(method, url, **kwargs)
                if http_success(response.status_code):
                    return response

            print_warning(
                f"Connection to Sonarr failed. Retrying in {humanized_eta(delay)}..."
            )
            sleep(delay)
        return None

    def get_all_series(self) -> List[Dict]:
        response = self._request("GET", "/series")
        if response:
            return response.json()
        raise ConnectionError("Failed to retrieve series list from Sonarr.")

    def search_series(self, series_id: int) -> Optional[int]:
        """Triggers a SeriesSearch and returns the command ID."""
        payload = {"name": "SeriesSearch", "seriesId": series_id}
        response = self._request("POST", "/command", json=payload)
        return response.json().get("id") if response else None

    def get_command_status(self, command_id: int) -> Optional[str]:
        response = self._request("GET", f"/command/{command_id}")
        return response.json().get("status") if response else None

    def wait_for_command(self, command_id: int, series_title: str) -> bool:
        start_time = datetime.now()
        while True:
            sleep(10)
            result = self.check_command_completion(command_id, series_title, start_time)
            if result is not None:
                return result

    def check_command_completion(
        self, command_id: int, series_title: str, start_time: datetime
    ) -> Optional[bool]:
        status = self.get_command_status(command_id)
        if not status:
            msg(series_title, "Failed to get command status.", type="error")
            return False  # Failed communication

        if status in {"failed", "cancelled", "orphaned", "aborted"}:
            msg(series_title, "Search failed/cancelled.", type="error")
            return False  # Failed command

        if status == "completed":
            return True  # Success

        if (datetime.now() - start_time).total_seconds() > MAX_SEARCH_WAIT:
            msg(
                series_title,
                f"Took longer than {humanized_eta(MAX_SEARCH_WAIT)} to process. Moving on...",
                type="warning",
            )
            return True

        return None  # Still running


class SonarrRedownloader:
    def __init__(self):
        self.state = StateManager()
        self.client: SonarrClient
        self.resume = False
        self.all_series = []
        self.max_failures = 0

    def setup(self):
        # 1. Load state
        if (
            not self.resume
            and self.state.load()
            and (input_bold("Resume previous search? [Y/N]: ").lower() == "y")
        ):
            self.resume = True

        # 2. Configure Connection
        if not self.resume:
            self._configure_connection()
        self.client = SonarrClient(self.state.sonarr_url, self.state.api_key)

        # 3. Connect and fetch series
        print_info("Connecting to Sonarr...")
        try:
            self.all_series = self.client.get_all_series()
            print_info(f"Connected! Found {len(self.all_series)} series.")
        except ConnectionError as e:
            print_error(str(e))
            return False

        # 4. Prompt for user settings / preferences
        self._configure_preferences()
        self.state.save()

        # 5. Adjust max failures
        if len(self.state.series_ids) > 0:
            self.max_failures = round(len(self.state.series_ids) * MAX_FAILURE_RATIO)

        return True

    def _configure_connection(self):
        if self.state.sonarr_url:
            print_info(f"Previous URL: {self.state.sonarr_url}")
        if self.state.api_key:
            print_info("Previous API Key: ***********")

        url = input_bold(
            f"Sonarr URL {self.state.sonarr_url and '[Enter to re-use]'}: "
        )
        if url:
            self.state.sonarr_url = url

        api_key = input_bold(
            f"Sonarr API Key {self.state.api_key and '[Enter to re-use]'}: "
        )
        if api_key:
            self.state.api_key = api_key

    def _configure_preferences(self):
        if not self.resume:
            root_dir = input_bold("Root directory to upgrade (optional): ")
            try:
                max_episodes = int(
                    input_bold("Skip shows with greater than X episodes (optional): ")
                )
            except ValueError:
                max_episodes = float("inf")

            self.state.total_completed = 0
            self.state.series_ids = self._filter_series(root_dir, max_episodes)

        # Always prompt for speed to allow user to cancel script and adjust
        if self.state.speed != 0:
            print_info(f"Previous speed '{self.state.speed}' [Enter to re-use]")
        while True:
            speed_input = cast(
                Literal[1, 2, 3],
                int(
                    input_bold("Search speed [1-3] (optional): ")
                    or self.state.speed
                    or 1
                ),
            )
            if speed_input == 3:
                confirm = input_warning(
                    "Speed '3' queues everything at once! This is difficult to stop once started, and speed '2' is nearly as fast. Are you sure? [Y/N]: "
                )
                if confirm.lower() == "y":
                    self.state.speed = 3
                    break
            elif speed_input in {1, 2}:
                self.state.speed = speed_input
                break
            else:
                print_warning("Invalid speed level. Please enter 1, 2, or 3.")

    def _filter_series(self, root_dir: str, max_episodes: float) -> List[int]:
        ids = []
        for s in self.all_series:
            if root_dir and not s["path"].startswith(root_dir):
                continue  # Skip series not in the specified root directory

            stats = s.get("statistics")
            if not stats:
                print_warning(
                    f"Skipping series '{s['title']}' due to missing statistics."
                )
                continue

            if stats["episodeFileCount"] > max_episodes:
                print_info(
                    f"Skipping series '{s['title']}' due to episode count ({stats['episodeFileCount']})."
                )
                continue

            ids.append(s["id"])
        return ids

    def _get_series_by_id(self, sid: int) -> Optional[Dict]:
        return next((s for s in self.all_series if s["id"] == sid), None)

    def _calculate_total_episodes(self, ids: List[int]) -> int:
        count = 0
        for sid in ids:
            s = self._get_series_by_id(sid)
            if s and "statistics" in s:
                count += s["statistics"]["totalEpisodeCount"]
        return count

    def _time_estimate(
        self,
        initial_session_remaining: int,
        initial_session_eps: int,
        session_start_time: datetime,
    ) -> int:
        # Sometimes, 'SeriesSearch' falls back to searching for individual episodes,
        # so we average several estimates to improve accuracy.
        remaining_searches = len(self.state.series_ids)
        if remaining_searches < initial_session_remaining:
            remaining_eps = self._calculate_total_episodes(self.state.series_ids)
            elapsed = (datetime.now() - session_start_time).total_seconds()

            # Based on remaining episodes count
            eps_completed = initial_session_eps - remaining_eps + 1
            est_ep = (elapsed / eps_completed) * remaining_eps

            # Based on remaining series count
            session_completed = initial_session_remaining - remaining_searches + 1
            est_series = (elapsed / session_completed) * remaining_searches

            # Set the value within the state storage
            self.state.time_estimate = int((est_ep + est_series) / 2)

        return self.state.time_estimate

    def _wait_for(self, futures: Set[Future], wait_all: bool = False) -> int:
        """
        Waits for futures to complete.
        If wait_all is False, waits for FIRST_COMPLETED.
        If wait_all is True, waits for ALL_COMPLETED.
        Removes completed futures from the set.
        Returns the number of failures from completed futures.
        """
        new_failures = 0
        if not futures:
            return 0

        return_when = ALL_COMPLETED if wait_all else FIRST_COMPLETED
        done, not_done = wait(futures, return_when=return_when)

        for f in done:
            # wait_for_command returns True/False
            if not f.result():
                new_failures += 1

        # Update the set of futures to only contain the pending ones
        futures.clear()
        futures.update(not_done)

        return new_failures

    def run(self) -> None:
        if not self.setup():
            return  # Setup failed, it should have printed an error
        initial_session_remaining = len(self.state.series_ids)
        total_series = initial_session_remaining + self.state.total_completed
        if initial_session_remaining == 0:
            return print_info("No series to process.")
        initial_session_eps = self._calculate_total_episodes(self.state.series_ids)
        start_time = datetime.now()
        failures = 0
        ids_to_process = list(self.state.series_ids)
        active_futures: Set[Future] = set()

        with ThreadPoolExecutor(max_workers=3) as executor:
            for x, series_id in enumerate(ids_to_process):
                # Check if the series exists
                series = self._get_series_by_id(series_id)
                if not series:
                    print_warning(
                        f"Series with ID {series_id} missing from Sonarr (likely deleted by user). Skipping..."
                    )
                    continue

                # Print out progress / status
                percent = (self.state.total_completed / total_series) * 100
                time_estimate_sec = 0
                if x > 3:  # Wait a bit before estimating time
                    time_estimate_sec = self._time_estimate(
                        initial_session_remaining, initial_session_eps, start_time
                    )
                msg(
                    f"{series['title']}",
                    f"{self.state.total_completed}/{total_series} ({percent:.2f}%)",
                    f"{humanized_eta(time_estimate_sec)} remaining (estimated)",
                    type="success",
                )

                # Queue a "series search" in Sonarr
                command_id = self.client.search_series(series_id)
                if not command_id:
                    msg(series["title"], "Search command failed.", type="warning")
                    failures += 1
                    if failures > self.max_failures:
                        print_error("Too many failures! Aborting.")
                        break
                    continue

                # Remove from series from list (search was successfully queued)
                if series_id in self.state.series_ids:
                    self.state.series_ids.remove(series_id)
                    self.state.total_completed += 1
                    self.state.save()

                # Wait for search to complete, depending on speed setting
                if self.state.speed == 3:
                    continue  # All searches queued at once, no waiting here
                future = executor.submit(
                    self.client.wait_for_command, command_id, series["title"]
                )
                active_futures.add(future)
                active_limit = 3 if self.state.speed == 2 else 1
                if len(active_futures) >= active_limit:
                    failures += self._wait_for(active_futures)
                if failures > self.max_failures:
                    print_error("Too many failures! Aborting.")
                    break

            # All tasks (for speed 1 and 2) have been queued, wait for last batch to complete
            if failures <= self.max_failures:
                failures += self._wait_for(active_futures, wait_all=True)

        # Done
        elapsed_total = (datetime.now() - start_time).total_seconds()
        msg(
            f"Processed {total_series - len(self.state.series_ids)} series in {humanized_eta(int(elapsed_total))}.",
            type="info",
        )

    def retry_failures_prompt(self):
        if self.state.series_ids:
            retry_input = input_bold(
                "Do you want to retry all failed searches? [Y/N]: "
            ).lower()
            if retry_input == "y":
                print_info("Retrying failed searches...")
                self.resume = True
                self.run()


if __name__ == "__main__":
    app: Optional[SonarrRedownloader] = None
    try:
        app = SonarrRedownloader()
        app.run()
        app.retry_failures_prompt()
        app.state.clear()
        print_success("Completed.")
    except KeyboardInterrupt:
        print_error("Script aborted by user.")
    except Exception as e:
        print_error(f"An unexpected error occurred: {e}")
