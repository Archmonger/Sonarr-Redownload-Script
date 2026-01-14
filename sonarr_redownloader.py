import contextlib
import json
import logging
import re
import sys
import threading
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
from datetime import datetime, timedelta
from logging import FileHandler
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Set, cast

import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[FileHandler("redownloader.log", encoding="utf-8")],
)

# Globals
MAX_SEARCH_WAIT = 5400  # Time in seconds
MAX_CONNECTION_RETRIES = 10  # Number of HTTP connection retries allowed
INITIAL_CONNECTION_RETRY_DELAY = 10  # Initial timeout in seconds
CHECK_STATUS_INTERVAL = 10  # Time in seconds between status checks
MAX_FAILURE_RATIO = 0.05  # 5% of total series can fail before aborting
STOP_EVENT = threading.Event()
STICKY_TEXT = ""


def set_sticky_text(text: str) -> None:
    global STICKY_TEXT
    if not text:
        return
    if text != STICKY_TEXT:
        _log(text, logging.INFO)
    STICKY_TEXT = text
    sys.stdout.write(f"\r\033[K{text}\r")
    sys.stdout.flush()


def end_sticky_text() -> None:
    global STICKY_TEXT
    if STICKY_TEXT:
        sys.stdout.write(f"\r\033[K{STICKY_TEXT}\n")
        sys.stdout.flush()
        STICKY_TEXT = ""


def _print_text(text: str = "") -> None:
    sys.stdout.write(f"\r\033[K{text}\n")
    if STICKY_TEXT:
        sys.stdout.write(f"\033[K{STICKY_TEXT}\r")
    sys.stdout.flush()


def _log(text: str, level: int) -> None:
    # Remove pre-inserted dates from text
    if text.startswith("[") and "]" in text:
        text = text.split("] ", 1)[1]

    # Strip ANSI escape codes (console font colors)
    text = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", text)
    logging.log(level, text)


def print_success(text: str) -> None:
    _log(text, logging.INFO)
    _print_text(f"\033[92m{text}\033[0m")


def print_info(text: str) -> None:
    _log(text, logging.INFO)
    _print_text(f"\033[96m{text}\033[0m")


def print_warning(text: str) -> None:
    _log(text, logging.WARNING)
    _print_text(f"\033[93m{text}\033[0m")


def print_error(text: str) -> None:
    _log(text, logging.ERROR)
    _print_text(f"\033[91m{text}\033[0m")


def input_bold(text: str) -> str:
    return input(f"\033[1m{text}\033[0m")


def input_warning(text: str) -> str:
    return input(f"\033[93m{text}\033[0m")


def msg(
    *text: str, type: Literal["info", "success", "warning", "error"] = "info"
) -> None:
    """Print a formatted multi-part message to the console."""
    _timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    _msg = "  —  ".join(text) if isinstance(text, tuple) else text
    _msg = f"[{_timestamp}] {_msg}"
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


def handle_user_interrupts(func):
    def wrapper(self: "SonarrRedownloader", *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except KeyboardInterrupt:
            STOP_EVENT.set()
            end_sticky_text()
            print_warning("\nWaiting for background tasks to cleanup...")
            for f in self.active_futures:
                with contextlib.suppress(Exception):
                    f.result(timeout=10)
                with contextlib.suppress(Exception):
                    # The series were optimistically removed from the pending list earlier.
                    # Since they've made it into Sonarr's queue, we will continue being
                    # optimistic and count them as completed.
                    self.state.completed.add(self.active_futures[f])
                    self.state.save()
            raise

    return wrapper


class StateManager:
    STATE_FILE = Path("redownloader_state.json")

    def __init__(self):
        self.sonarr_url: str = ""
        self.api_key: str = ""
        self.time_estimate: int = 0
        self.speed: Literal[0, 1, 2, 3, 4] = 0
        self.completed: Set[int] = set()
        self.suspicious: Set[int] = set()
        self.failures: Set[int] = set()
        self.queue: Set[int] = set()

    def load(self) -> bool:
        if self.STATE_FILE.is_file():
            try:
                with open(self.STATE_FILE, "r") as f:
                    state = json.load(f)
                    self.sonarr_url = state.get("sonarr_url", "")
                    self.api_key = state.get("api_key", "")
                    self.time_estimate = state.get("time_estimate", 0)
                    self.speed = state.get("speed", 1)
                    self.completed = set(state.get("completed", []))
                    self.suspicious = set(state.get("suspicious", []))
                    self.failures = set(state.get("failures", []))
                    self.queue = set(state.get("queue", []))

                    # Determine if there's any state to resume
                    if self.queue or self.failures or self.suspicious:
                        return True
            except Exception:
                print_error("State file corrupted.")
                raise
        return False

    def save(self) -> None:
        state = {
            "sonarr_url": self.sonarr_url,
            "api_key": self.api_key,
            "time_estimate": self.time_estimate,
            "speed": self.speed,
            "completed": list(self.completed),
            "suspicious": list(self.suspicious),
            "failures": list(self.failures),
            "queue": list(self.queue),
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state, f)

    def reset(self) -> None:
        self.time_estimate = 0
        self.completed = set()
        self.suspicious = set()
        self.queue = set()
        self.failures = set()
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
            if STOP_EVENT.wait(delay):
                return None
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

    def wait_for_search(self, command_id: int, series_title: str) -> tuple[bool, bool]:
        start_time = datetime.now()
        success_flag = False
        suspicious_flag = False

        # Keep iterating until `check_search_completion` completion/time-out, or STOP_EVENT abort.
        while not STOP_EVENT.is_set() and not STOP_EVENT.wait(CHECK_STATUS_INTERVAL):
            result = self.check_search_completion(command_id, series_title, start_time)
            if result is True and (datetime.now() - start_time).total_seconds() < 30:
                msg(
                    series_title,
                    "Search finished suspiciously fast...",
                    type="warning",
                )
                suspicious_flag = True
            if result is not None:
                success_flag = result
                break
        return success_flag, suspicious_flag

    def check_search_completion(
        self, command_id: int, series_title: str, start_time: datetime
    ) -> Optional[bool]:
        response = self._request("GET", f"/command/{command_id}")
        result = response.json() if response else {}
        status = result.get("status") if response else None
        duration_str = result.get("duration")
        duration = 0
        if duration_str:
            h, m, s = duration_str.split(":")
            duration = int(
                timedelta(
                    hours=int(h),
                    minutes=int(m),
                    seconds=float(s),
                ).total_seconds()
            )
        humanized_duration = humanized_eta(duration) if duration else "0 seconds"

        if not status:
            msg(series_title, "Failed to get command status.", type="error")
            return False  # Failed communication

        if status in {"failed", "cancelled", "orphaned", "aborted"}:
            msg(
                series_title,
                f"Search concluded with status '{status}'.",
                type="error",
            )
            return False  # Failed command

        if status == "completed":
            msg(
                series_title,
                f"Search completed in {humanized_duration}.",
                type="success",
            )
            return True  # Success

        if (datetime.now() - start_time).total_seconds() > MAX_SEARCH_WAIT:
            msg(
                series_title,
                f"Search took longer than {humanized_eta(MAX_SEARCH_WAIT)}. Moving on...",
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
        self.active_futures: Dict[Future, int] = {}

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
        if len(self.state.queue) > 0:
            self.max_failures = round(len(self.state.queue) * MAX_FAILURE_RATIO)

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

            self.state.reset()
            self.state.queue = set(self._filter_series(root_dir, max_episodes))

        # Always prompt for speed to allow user to cancel script and adjust
        if self.state.speed != 0:
            print_info(f"Previous speed '{self.state.speed}' [Enter to re-use]")
        while True:
            speed_input = cast(
                Literal[1, 2, 3, 4],
                int(
                    input_bold("Search speed [1-4] (optional): ")
                    or self.state.speed
                    or 2
                ),
            )
            if speed_input == 4:
                confirm = input_warning(
                    "Speed '4' queues everything at once! This is difficult to stop once started, and most indexers will block or rate limit you.\n"
                    "Are you sure? [Y/N]: "
                )
                if confirm.lower() == "y":
                    self.state.speed = 4
                    break
            elif speed_input in {1, 2, 3}:
                self.state.speed = speed_input
                break
            else:
                print_warning("Invalid speed level. Please enter 1, 2, 3, or 4.")

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

    def _calculate_total_episodes(self, ids: Iterable[int]) -> int:
        count = 0
        for sid in ids:
            s = self._get_series_by_id(sid)
            if s and "statistics" in s:
                count += s["statistics"]["totalEpisodeCount"]
        return count

    def _calculate_time_estimate(
        self, queue_length: int, episode_count: int, start_time: datetime
    ) -> None:
        # Sometimes, 'SeriesSearch' falls back to searching for individual episodes,
        # so we average several estimates to improve accuracy.
        remaining_searches = len(self.state.queue)
        if remaining_searches < queue_length:
            remaining_eps = self._calculate_total_episodes(self.state.queue)
            elapsed = (datetime.now() - start_time).total_seconds()

            # Based on remaining episodes count
            eps_completed = episode_count - remaining_eps + 1
            est_ep = (elapsed / eps_completed) * remaining_eps

            # Based on remaining series count
            session_completed = queue_length - remaining_searches + 1
            est_series = (elapsed / session_completed) * remaining_searches

            # Set the value within the state storage
            # Average with previous estimate for smoothing, if possible
            new_estimate = int((est_ep + est_series) / 2)
            self.state.time_estimate = (
                int((self.state.time_estimate + new_estimate) / 2)
                if self.state.time_estimate > 0
                else new_estimate
            )

    def _check_failure(self, futures: Dict[Future, int]) -> int:
        """
        Waits for a 'future' (series search) to complete. Returns the number of failures that occurred.
        """
        if not futures:
            return 0

        done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
        failures = 0
        for f in done:
            series_id = futures.pop(f)

            try:
                success, suspicious = f.result()
                if suspicious:
                    self.state.suspicious.add(series_id)

                if not success:
                    failures += 1
                    self.state.failures.add(series_id)
                else:
                    self.state.completed.add(series_id)
            except Exception as e:
                failures += 1
                print_error(
                    f"An unexpected error occurred while processing a series: {e}"
                )
        self.state.save()
        return failures

    def retry_failures_prompt(self):
        if self.state.failures:
            retry_input = input_bold(
                "Do you want to retry all failed searches? [Y/N]: "
            ).lower()
            if retry_input == "y":
                print_info("Retrying failed searches...")
                self.state.queue.update(self.state.failures)
                self.state.failures = set()
                self.resume = True
                self.run()

    def retry_suspicious_prompt(self):
        if self.state.suspicious:
            retry_input = input_bold(
                "Do you want to retry all suspicious searches? [Y/N]: "
            ).lower()
            if retry_input == "y":
                print_info("Retrying suspicious searches...")
                self.state.queue.update(self.state.suspicious)
                self.state.suspicious = set()
                self.resume = True
                self.run()

    def update_status(
        self,
        initial_queue_length: int,
        initial_episode_count: int,
        queue_position: int,
        current_failures: int,
        start_time: datetime,
    ) -> None:
        total_failures = len(self.state.failures) + current_failures
        total = (
            len(self.state.queue) + len(self.state.failures) + len(self.state.completed)
        )
        percent = ((len(self.state.completed) + total_failures) / total) * 100
        if queue_position > self.state.speed * 2:  # Wait a bit before time estimation
            self._calculate_time_estimate(
                initial_queue_length, initial_episode_count, start_time
            )
        set_sticky_text(
            f"\033[92mCompleted: {len(self.state.completed)}\033[0m  —  "
            f"\033[91mFailed: {total_failures}\033[0m  —  "
            f"\033[93mTotal: {len(self.state.completed) + total_failures}/{total} ({percent:.2f}%)\033[0m  —  "
            f"\033[96mRemaining: {humanized_eta(self.state.time_estimate)}\033[0m"
        )

    @handle_user_interrupts
    def run(self) -> None:
        if not self.setup():
            return  # Setup failed, it should have printed an error
        initial_queue_length = len(self.state.queue)
        if initial_queue_length == 0:
            return print_info("No series to process.")
        initial_episode_count = self._calculate_total_episodes(self.state.queue)
        current_failures = 0
        start_time = datetime.now()
        self.active_futures = {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            for position, series_id in enumerate(self.state.queue.copy()):
                # Check for abort conditions
                if STOP_EVENT.is_set():
                    break
                if current_failures > self.max_failures:
                    print_error("Script encountered too many failures! Aborting.")
                    break

                # Check if the series exists
                series = self._get_series_by_id(series_id)
                if not series:
                    print_warning(
                        f"Series with ID {series_id} missing from Sonarr (likely deleted by user). Skipping..."
                    )
                    continue

                # Update progress information
                self.update_status(
                    initial_queue_length,
                    initial_episode_count,
                    position,
                    current_failures,
                    start_time,
                )

                # Send a "series search" command to Sonarr
                command_id = self.client.search_series(series_id)
                if not command_id:
                    msg(
                        series["title"],
                        "Sonarr did not respond to 'Series Search' command!",
                        type="error",
                    )
                    current_failures += 1
                    continue
                else:
                    msg(f"{series['title']}", "Search started.", type="info")

                # Sonarr successfully received the search request. Optimistically remove it
                # from our pending list in case the user terminates the script.
                if series_id in self.state.queue:
                    self.state.queue.remove(series_id)
                self.state.save()

                # Wait for search to complete, depending on speed setting
                if self.state.speed == 4:
                    continue  # Fastest speed does not wait for task completion
                f = executor.submit(
                    self.client.wait_for_search, command_id, series["title"]
                )
                self.active_futures[f] = series_id
                if len(self.active_futures) >= self.state.speed:
                    current_failures += self._check_failure(self.active_futures)

            # For speed 1 and 2: All tasks are within our internal queue. Wait for the last batch to complete.
            while (
                self.active_futures
                and current_failures <= self.max_failures
                and not STOP_EVENT.is_set()
            ):
                current_failures += self._check_failure(self.active_futures)

        # Done
        end_sticky_text()
        print_success("Processing complete!")


if __name__ == "__main__":
    app: Optional[SonarrRedownloader] = None
    try:
        app = SonarrRedownloader()
        app.run()
        app.retry_failures_prompt()
        app.retry_suspicious_prompt()
    except KeyboardInterrupt:
        end_sticky_text()
        print_error("Script aborted by user.")
    except Exception as e:
        end_sticky_text()
        print_error(f"An unexpected error occurred: {e}")
