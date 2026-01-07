import contextlib
import json
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Literal

import requests

MAX_TIMEOUT = 3600  # Time in seconds
MAX_CONNECTION_RETRIES = 10  # Number of attempts
CONNECTION_RETRY_TIMEOUT = 10  # Time in seconds
MAX_FAILURES = 20  # Number of failures allowed before aborting the script


def content_redownloader():
    """Have Sonarr perform a "Series Search" on your content to upgrade them."""
    state: dict = {}
    resume = False
    rapid = False
    series_ids = []
    time_estimate = 0

    # Check for existing state file
    if Path("downloader_state.json").is_file():
        with open("downloader_state.json", "r") as f:
            state = json.load(f)
            sonarr_url = state["sonarr_url"]
            api_key = state["api_key"]
            series_ids = state["series_ids"]
            time_estimate = state["time_estimate"]

        if input_bold("Resume from previous state? [Y/N]: ").lower() == "y":
            resume = True

    # User input for Sonarr URL and API key
    if not resume:
        if state.get("sonarr_url"):
            print_info(f"Previous Sonarr URL detected: {state.get('sonarr_url')}")
        if state.get("api_key"):
            print_info("Previous API key detected: *******************")
        if state.get("sonarr_url") or state.get("api_key"):
            print_info("You can press Enter to reuse these value(s).")
        sonarr_url = input_bold(
            "Sonarr URL (ex. http://192.168.86.20:8989): "
        ) or state.get("sonarr_url", "")
        api_key = input_bold("Sonarr API key: ") or state.get("api_key", "")

    # Get a list of all series within Sonarr
    print_info("Attempting to connect to Sonarr...")
    get_series_url = sonarr_url + "/api/v3/series?apikey=" + api_key
    get_series_status = 404
    for _ in range(MAX_CONNECTION_RETRIES):
        with contextlib.suppress(Exception):
            get_series_response = requests.get(get_series_url)
            get_series_status = get_series_response.status_code
            if http_success(get_series_status):
                break
        print_warning(
            f"Failed initial connection to Sonarr! Retrying in {CONNECTION_RETRY_TIMEOUT} seconds..."
        )
        sleep(CONNECTION_RETRY_TIMEOUT)
    all_series = json.loads(get_series_response.content)
    print_info(f"Connected to Sonarr! Found {len(all_series)} series in your library.")

    # Obtain user preferences
    if not resume:
        root_dir = input_bold("Root directory to upgrade (ex. /media/TV) (optional): ")

        max_episodes = int(
            input_bold("Skip shows with more than _____ episodes (optional): ")
        )
        if not isinstance(max_episodes, int) or int(max_episodes) <= 0:
            raise ValueError("You must enter a positive integer.")

        if input_bold("Rapid mode [Y/N] (optional): ").lower() == "y":
            user_val = input_warning(
                "WARNING: 'Rapid' immediately queues all search queries in parallel. This is difficult to stop once started.\n"
                "Are you sure? [Y/N]: "
            )
            if user_val.lower() == "y":
                rapid = True
            else:
                print_info("Rapid mode *disabled*.")

        series_ids = series_list_to_ids(all_series, root_dir, max_episodes)

    # Save state file
    if not (resume or rapid):
        update_state_file(sonarr_url, api_key, series_ids, 0)

    # Search for file upgrades
    initial_length = len(series_ids)
    initial_episode_count = total_episode_count(all_series, series_ids)
    start_time = datetime.now()
    failures = 0
    global MAX_FAILURES
    MAX_FAILURES = max(MAX_FAILURES, int(initial_length * 0.05))
    for series in all_series:
        if series["id"] not in series_ids:
            continue

        # Update progress
        remaining_episode_count = total_episode_count(all_series, series_ids)
        if initial_length != len(series_ids):
            time_estimate = create_time_estimate(
                initial_episode_count,
                remaining_episode_count,
                initial_length,
                len(series_ids),
                start_time,
            )
        msg(
            f"{series['title']} (#{series['id']})",
            f"{initial_length - len(series_ids) + 1} of {initial_length} ({(initial_length - len(series_ids)) / initial_length * 100:.2f}%)",
            f"{humanized_eta(time_estimate)} remaining (estimated)",
            type="success",
        )

        # Command Sonarr to perform a series search
        command_search_url = sonarr_url + "/api/v3/command?apikey=" + api_key
        command_search_parameters = {
            "name": "SeriesSearch",
            "seriesId": int(series["id"]),
        }
        command_search_status = 404
        for _ in range(MAX_CONNECTION_RETRIES):
            with contextlib.suppress(Exception):
                command_search_response = requests.post(
                    command_search_url, json=command_search_parameters
                )
                command_search_status = command_search_response.status_code
                if http_success(command_search_status):
                    break
            msg(
                series["title"],
                f"Search command failed! Retrying in {CONNECTION_RETRY_TIMEOUT} seconds...",
                type="warning",
            )
            sleep(CONNECTION_RETRY_TIMEOUT)

        # If the search failed, continue to the next series (as long as we haven't exceeded max failures)
        if not http_success(command_search_status):
            failures += 1
            if failures > MAX_FAILURES:
                print_error("Maximum failures reached. Aborting.")
                return False
            continue

        command_search_id = json.loads(command_search_response.content)["id"]

        # Wait for the search to complete (if not in rapid mode)
        if not rapid and not wait_for_completion(
            sonarr_url, api_key, command_search_id, series, series_ids, time_estimate
        ):
            failures += 1
            if failures > MAX_FAILURES:
                print_error("Maximum failures reached. Aborting.")
                return False

    # Update state file
    Path("download_state.json").unlink(missing_ok=True)
    return True


def wait_for_completion(
    sonarr_url: str,
    api_key: str,
    command_search_id: int,
    series: dict,
    series_ids: list,
    time_estimate: int,
) -> bool:
    """Check for completion of a Sonarr command."""
    completion_url = f"{sonarr_url}/api/v3/command/{command_search_id}?apikey={api_key}"
    start_time = datetime.now()

    while True:
        # Wait in between checks
        sleep(5)
        completion_status = 404
        connection_retries = 0

        # Fetch status from the API endpoint
        while not http_success(completion_status):
            with contextlib.suppress(Exception):
                completion_response = requests.get(completion_url)
                completion_status = completion_response.status_code
                if http_success(completion_status):
                    break

            connection_retries = connection_retries + 1
            if connection_retries > MAX_CONNECTION_RETRIES:
                msg(
                    series["title"],
                    "Checking completion status exceeded maximum connection retries.",
                    type="error",
                )
                return False

            msg(
                series["title"],
                f"Completion check failed! Retrying in {CONNECTION_RETRY_TIMEOUT} seconds...",
                type="warning",
            )
            sleep(CONNECTION_RETRY_TIMEOUT)
            continue

        # Parse the status and handle accordingly
        status = json.loads(completion_response.content)["status"]
        if status in {"failed", "cancelled", "orphaned", "aborted"}:
            msg(
                series["title"],
                "Sonarr reported the search ended in failure!",
                type="error",
            )
            return False
        if status == "completed":
            return True
        if (datetime.now() - start_time).total_seconds() > MAX_TIMEOUT:
            msg(
                series["title"],
                f"Series search did not finish within {humanized_eta(MAX_TIMEOUT)}! Continuing to next series...",
                type="warning",
            )
            return True

        # The command made it into Sonarr's search queue at least one time, so we can remove it from the pending list
        with contextlib.suppress(ValueError):
            series_ids.remove(series["id"])
            update_state_file(sonarr_url, api_key, series_ids, time_estimate)


def series_list_to_ids(series_list: list, root_dir: str, max_episodes: int) -> list:
    """Convert a list of series to a list of series IDs based on user preferences."""
    series_ids = []
    for series in series_list:
        if series["path"][: len(root_dir)] == root_dir:
            if not series.get("statistics"):
                msg(series["title"], "No statistics found! Skipping...", type="warning")
                continue
            episode_count = series["statistics"]["episodeFileCount"]
            if episode_count > int(max_episodes):
                msg(
                    series["title"],
                    "Has more episodes than the limit. Skipping...",
                    type="info",
                )
                continue

            series_ids.append(series["id"])

    return series_ids


def update_state_file(
    sonarr_url: str, api_key: str, series_ids: list, time_estimate: int
) -> None:
    """Update the downloader state file."""
    state = {
        "sonarr_url": sonarr_url,
        "api_key": api_key,
        "series_ids": series_ids,
        "time_estimate": time_estimate,
    }
    with open("downloader_state.json", "w") as f:
        json.dump(state, f)


def total_episode_count(series_list: list, series_ids: list) -> int:
    """Calculate the total episode count for a list of series IDs."""
    total = 0
    for series in series_list:
        if series["id"] in series_ids:
            total = total + series["statistics"]["totalEpisodeCount"]
    return total


def create_time_estimate(
    initial_episode_count: int,
    remaining_episode_count: int,
    initial_series_count: int,
    remaining_series_count: int,
    start_time: datetime,
) -> int:
    """Create a time estimate based on episodes and series processed."""
    # Calculate time estimates based on episodes
    episode_based_time_estimate = (
        (datetime.now() - start_time).total_seconds()
        / (initial_episode_count - remaining_episode_count + 1)
    ) * remaining_episode_count
    # Calculate time estimates based on series
    series_based_time_estimate = (
        (datetime.now() - start_time).total_seconds()
        / (initial_series_count - remaining_series_count + 1)
    ) * remaining_series_count
    # Return the average of both estimates
    return int((episode_based_time_estimate + series_based_time_estimate) / 2)


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

    # Remove decimal places
    days = round(days)
    hr = round(hr)
    min = round(min)
    sec = round(sec)

    # Build the time string
    if days > 0:
        time_parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hr > 0:
        time_parts.append(f"{hr} hour{'s' if hr != 1 else ''}")
    if min > 0:
        time_parts.append(f"{min} minute{'s' if min != 1 else ''}")
    if sec > 0:
        time_parts.append(f"{sec} second{'s' if sec != 1 else ''}")

    # If no time parts were added, return "TBD"
    if not time_parts:
        return "TBD"

    # Limit to two largest time units
    if len(time_parts) > 2:
        time_parts = time_parts[:2]

    return " ".join(time_parts)


def http_success(status_code: int) -> bool:
    """Check if an HTTP status code indicates success."""
    return status_code >= 200 and status_code < 300


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


if __name__ == "__main__":
    try:
        if content_redownloader():
            print_info("Script successfully completed.")
        else:
            print_error("Script failed to complete.")
    except KeyboardInterrupt:
        print_error("Script terminated by `KeyboardInterrupt`.")
