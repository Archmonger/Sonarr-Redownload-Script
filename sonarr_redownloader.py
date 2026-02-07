import argparse
import atexit
import contextlib
import json
import logging
import os
import sys
import threading
import time
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
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Grid, Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    Log,
    ProgressBar,
    Select,
    Static,
)

# Set up logging for file (console logging will be handled by Textual)
DATA_PATH = Path("redownloader_data")
DATA_PATH.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[FileHandler(DATA_PATH / "redownloader.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# Globals
MAX_SEARCH_WAIT = 21600  # Time in seconds
MAX_CONNECTION_RETRIES = 10  # Number of HTTP connection retries allowed
INITIAL_CONNECTION_RETRY_DELAY = 10  # Initial timeout in seconds
CHECK_STATUS_INTERVAL = 10  # Time in seconds between status checks
MAX_FAILURE_RATIO = 0.05  # 5% of total series can fail before aborting
SUSPICIOUS_THRESHOLD = 10  # Seconds; searches completing faster are suspicious
STOP_EVENT = threading.Event()
PAUSE_EVENT = threading.Event()
SPEED_SETTINGS = {
    1: (
        "Safe",
        "Your setting will perform one search sequentially. Slow but very unlikely to generate problems.",
    ),
    2: (
        "Balanced",
        "Your setting will perform two searches concurrently. This leaves Sonarr's third worker slot open, allowing it to do other tasks.",
    ),
    3: (
        "Fast",
        "Your setting will perform three searches concurrently. Maximizes Sonarr's worker limit. Likely to hit indexer rate limits, and will slow down Sonarr's ability to do other tasks.",
    ),
    4: (
        "Turbo",
        "Your setting will immediately submit all searches to Sonarr ('fire and forget'). Very likely to hit indexer rate limits. This may prevent Sonarr from doing anything else for several days!",
    ),
}


class TextualLogHandler(logging.Handler):
    """Redirects log messages to a Textual Log widget."""

    def __init__(self, log_widget: Log):
        super().__init__()
        self.log_widget = log_widget

    def emit(self, record):
        msg = self.format(record)
        if threading.current_thread() is threading.main_thread():
            self.log_widget.write_line(msg)
        else:
            self.log_widget.app.call_from_thread(self.log_widget.write_line, msg)


def humanized_eta(seconds: int, default: str = "") -> str:
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

    return " ".join(time_parts[:2]) if time_parts else default


def filter_series(
    series_list: List[Dict],
    root_dir: str,
    max_eps: float,
    min_year: int = 0,
    max_year: int = 0,
    genre: str = "",
    status: str = "All",
    series_type: str = "All",
    monitored_only: bool = False,
) -> List[int]:
    queue = []
    for s in series_list:
        if root_dir and not s.get("path", "").startswith(root_dir):
            continue
        stats = s.get("statistics")
        if not stats:
            continue
        if stats["episodeFileCount"] > max_eps:
            continue

        if min_year > 0 and s.get("year", 0) < min_year:
            continue
        if max_year > 0 and s.get("year", 0) > max_year:
            continue

        if genre:
            target_genre = genre.lower()
            current_genres = [g.lower() for g in s.get("genres", [])]
            if all(target_genre not in g for g in current_genres):
                continue

        if status != "All" and s.get("status", "").lower() != status.lower():
            continue

        if (
            series_type != "All"
            and s.get("seriesType", "").lower() != series_type.lower()
        ):
            continue

        if monitored_only and not s.get("monitored", False):
            continue

        queue.append(s["id"])
    return queue


def http_success(status_code: int) -> bool:
    return 200 <= status_code < 300


class StateManager:
    STATE_FILE = DATA_PATH / "redownloader.json"

    def __init__(self, dummy_mode: bool = False):
        self.dummy_mode = dummy_mode
        self.sonarr_url: str = ""
        self.api_key: str = ""
        self.time_estimate: int = 0
        self.speed: Literal[0, 1, 2, 3, 4] = 0
        self.completed: Set[int] = set()
        self.suspicious: Set[int] = set()
        self.failures: Set[int] = set()
        self.queue: Set[int] = set()

    def sanitize_queue(self) -> None:
        """Ensure no overlap between queue and completed/failures."""
        if self.queue:
            original_len = len(self.queue)
            self.queue -= self.completed
            self.queue -= self.failures
            if len(self.queue) != original_len:
                logger.info(
                    f"Removed {original_len - len(self.queue)} duplicate/completed items from queue."
                )

    def requeue_failures(self) -> None:
        """Move failed items back to the queue."""
        if self.failures:
            self.queue.update(self.failures)
            self.failures.clear()

    def requeue_suspicious(self) -> None:
        """Move suspicious items back to the queue and remove from completed."""
        if self.suspicious:
            self.queue.update(self.suspicious)
            self.completed.difference_update(self.suspicious)
            self.suspicious.clear()

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

                    self.sanitize_queue()

                    # Determine if there's any state to resume
                    if self.queue or self.failures or self.suspicious:
                        return True
            except Exception:
                logger.error("State file corrupted.")
                # Don't raise, just treat as empty
        return False

    def save(self) -> None:
        if self.dummy_mode:
            logger.info("Dummy mode: Skipping state save.")
            return

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
    def __init__(self, url: str, api_key: str, dummy_mode: bool = False):
        if not dummy_mode and not url:
            raise ValueError("Sonarr URL cannot be empty.")
        if not dummy_mode and not api_key:
            raise ValueError("Sonarr API key cannot be empty.")
        self.url = url.rstrip("/") if url else ""
        self.api_key = api_key
        self.dummy_mode = dummy_mode

    def _request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[requests.Response]:
        url = f"{self.url}/api/v3{endpoint}"
        params = kwargs.get("params", {})
        params["apikey"] = self.api_key
        kwargs["params"] = params
        for x in range(MAX_CONNECTION_RETRIES):
            # Check stop event first
            if STOP_EVENT.is_set():
                return None

            delay = INITIAL_CONNECTION_RETRY_DELAY * ((x + 1) * 2)
            with contextlib.suppress(Exception):
                response = requests.request(method, url, **kwargs)
                if http_success(response.status_code):
                    return response

            logger.warning(
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

    def get_episode_files(self, series_id: int) -> List[Dict]:
        response = self._request("GET", "/episodefile", params={"seriesId": series_id})
        return response.json() if response else []

    def search_series(self, series_id: int) -> Optional[int]:
        """Triggers a SeriesSearch and returns the command ID."""
        if self.dummy_mode:
            logger.info(f"Dummy mode: Simulate search for series {series_id}")
            return 999999

        payload = {"name": "SeriesSearch", "seriesId": series_id}
        response = self._request("POST", "/command", json=payload)
        return response.json().get("id") if response else None

    def wait_for_search(self, command_id: int, series_title: str) -> tuple[bool, bool]:
        if self.dummy_mode:
            logger.info(f"Dummy mode: Simulating search for {series_title}")
            time.sleep(1.5)
            return True, False

        start_time = datetime.now()
        success_flag = False
        suspicious_flag = False

        # Keep iterating until `check_search_completion` completion/time-out, or STOP_EVENT abort.
        while not STOP_EVENT.is_set() and not STOP_EVENT.wait(CHECK_STATUS_INTERVAL):
            result = self.check_search_completion(command_id, series_title, start_time)
            if (
                result is True
                and (datetime.now() - start_time).total_seconds() < SUSPICIOUS_THRESHOLD
            ):
                logger.warning(f"[{series_title}] Search finished suspiciously fast...")
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
        humanized_duration = humanized_eta(duration, default="0 seconds")

        if not status:
            logger.error(f"[{series_title}] Failed to get command status.")
            return False  # Failed communication

        if status in {"failed", "cancelled", "orphaned", "aborted"}:
            logger.error(f"[{series_title}] Search concluded with status '{status}'.")
            return False  # Failed command

        if status == "completed":
            logger.info(f"[{series_title}] Search completed in {humanized_duration}.")
            return True  # Success

        if (datetime.now() - start_time).total_seconds() > MAX_SEARCH_WAIT:
            logger.warning(
                f"[{series_title}] Search took longer than {humanized_eta(MAX_SEARCH_WAIT)}. Moving on..."
            )
            return True

        return None  # Still running


# --- UI Screens ---

class ProgressModal(ModalScreen):
    """Modal to show progress of a long-running task."""

    def __init__(self, title: str, total: int):
        super().__init__()
        self.title = title
        self.total = total

    def compose(self) -> ComposeResult:
        with Container(classes="modal"):
            yield Label(self.title or "", classes="heading")
            yield ProgressBar(total=self.total, show_eta=True, id="progress")
            yield Label("Processing...", id="status_label")

    def update_progress(self, advance: float, message: str = ""):
        pb = self.query_one(ProgressBar)
        pb.advance(advance)
        if message:
            self.query_one("#status_label", Label).update(message)


class QuestionModal(ModalScreen[bool]):
    """A simple Yes/No modal."""

    def __init__(self, question: str, description: str = ""):
        super().__init__()
        self.question = question
        self.description = description

    def compose(self) -> ComposeResult:
        with Container(classes="modal"):
            yield Label(self.question, classes="question", shrink=True)
            if self.description:
                yield Label(self.description, classes="help-text", shrink=True)
            with Horizontal(classes="buttons"):
                yield Button("Yes", variant="primary", id="yes")
                yield Button("No", variant="error", id="no")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "yes")


class FilterModal(ModalScreen):
    """Modal for re-filtering queue during resume."""

    def __init__(self, state_manager: StateManager):
        super().__init__()
        self.state = state_manager

    def compose(self) -> ComposeResult:
        with Container(classes="modal"):
            yield Label("Filter Remaining Items", classes="heading")
            yield Label("Monitored Only:")
            yield Select(
                [("Yes", True), ("No", False)],
                value=True,
                id="monitored",
                allow_blank=False,
            )
            yield Label("Status:")
            yield Select.from_values(
                ["All", "Continuing", "Ended", "Upcoming", "Deleted"],
                value="All",
                id="status",
            )
            yield Label("Series Type:")
            yield Select.from_values(
                ["All", "Standard", "Daily", "Anime"], value="All", id="series_type"
            )
            yield Label("Directory (Optional):")
            yield Input(placeholder="e.g. /tv", id="root_dir")
            yield Label("Max Episode Count (Optional):")
            yield Input(placeholder="e.g. 100", id="max_eps", type="number")
            yield Label("Min Year (Optional):")
            yield Input(placeholder="e.g. 1990", id="min_year", type="number")
            yield Label("Max Year (Optional):")
            yield Input(placeholder="e.g. 2025", id="max_year", type="number")
            yield Label("Genre (Optional):")
            yield Input(placeholder="e.g. Action", id="genre")
            yield Label("\n[b]Advanced Filters (Slow)[/b]", classes="heading")
            yield Label("Only include series with 'Cutoff Unmet' files:")
            yield Select(
                [("Yes", True), ("No", False)],
                value=False,
                id="cutoff_unmet",
                allow_blank=False,
            )
            yield Label("Only include series with negative 'Custom Format Score' files:")
            yield Select(
                [("Yes", True), ("No", False)],
                value=False,
                id="negative_score",
                allow_blank=False,
            )

            with Horizontal(classes="buttons"):
                yield Button("Apply", variant="primary", id="apply")
                yield Button("Cancel", variant="error", id="cancel")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return

        if event.button.id != "apply":
            return

        root_dir = self.query_one("#root_dir", Input).value
        max_eps_str = self.query_one("#max_eps", Input).value
        min_year_str = self.query_one("#min_year", Input).value
        max_year_str = self.query_one("#max_year", Input).value
        genre = self.query_one("#genre", Input).value
        status = self.query_one("#status", Select).value
        series_type = self.query_one("#series_type", Select).value
        monitored_val = self.query_one("#monitored", Select).value
        monitored = cast(bool, monitored_val)
        cutoff_unmet = cast(bool, self.query_one("#cutoff_unmet", Select).value)
        negative_score = cast(bool, self.query_one("#negative_score", Select).value)

        try:
            max_eps = int(max_eps_str) if max_eps_str else float("inf")
        except ValueError:
            max_eps = float("inf")

        try:
            min_year = int(min_year_str) if min_year_str else 0
        except ValueError:
            min_year = 0

        try:
            max_year = int(max_year_str) if max_year_str else 0
        except ValueError:
            max_year = 0

        if cutoff_unmet or negative_score:
            loading_screen = ProgressModal("Deep Scanning Filters...", 100) # Total will be updated
        else:
            loading_screen = AlertModal("Connecting to Sonarr...", dismissable=False)
        
        self.app.push_screen(loading_screen)
        self.run_connection_and_filter(
            root_dir,
            max_eps,
            min_year,
            max_year,
            genre,
            status,
            series_type,
            monitored,
            cutoff_unmet,
            negative_score,
            loading_screen,
        )

    @work(thread=True)
    def run_connection_and_filter(
        self,
        root_dir,
        max_eps,
        min_year,
        max_year,
        genre,
        status,
        series_type,
        monitored,
        cutoff_unmet,
        negative_score,
        loading_modal,
    ):
        try:
            self.app: SonarrRedownloader
            client = SonarrClient(
                self.state.sonarr_url,
                self.state.api_key,
                dummy_mode=self.app.state_manager.dummy_mode,
            )
            series = client.get_all_series()
        except Exception as e:
            self.app.call_from_thread(loading_modal.dismiss, None)
            self.app.call_from_thread(
                self.app.push_screen, AlertModal(f"Connection failed: {str(e)}")
            )
            return

        # Filter
        queue = filter_series(
            series,
            root_dir,
            max_eps,
            min_year,
            max_year,
            genre,
            status,
            series_type,
            monitored,
        )

        if cutoff_unmet or negative_score:
            if isinstance(loading_modal, ProgressModal):
                def set_total():
                    loading_modal.query_one(ProgressBar).update(total=len(queue), progress=0)
                self.app.call_from_thread(set_total)

            filtered_queue = []
            for i, sid in enumerate(queue):
                try:
                    eps = client.get_episode_files(sid)
                    include = False
                    for ep in eps:
                        if cutoff_unmet and ep.get("qualityCutoffNotMet"):
                            include = True
                            break
                        if negative_score and ep.get("customFormatScore", 0) < 0:
                            include = True
                            break
                    if include:
                        filtered_queue.append(sid)
                except Exception as e:
                    logger.error(f"Error filtering series {sid}: {e}")

                if isinstance(loading_modal, ProgressModal):
                    self.app.call_from_thread(
                        loading_modal.update_progress, 1, f"Checking {i + 1}/{len(queue)}"
                    )
            
            queue = filtered_queue

        self.app.call_from_thread(loading_modal.dismiss, None)

        # Update state for resume (subtract known states)
        valid_ids = set(queue)
        valid_ids -= self.state.completed
        valid_ids -= self.state.failures
        valid_ids -= self.state.suspicious
        self.state.queue = valid_ids
        self.state.save()

        # Start main processing
        self.app.call_from_thread(self.dismiss, (client, series))


class StartupScreen(Screen[str]):
    """Screen to ask about resuming, starting new, or quitting."""

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(classes="config-container"):
            yield Label(
                "Previous session detected. What would you like to do?",
                classes="question",
            )
            with Horizontal(classes="buttons"):
                yield Button("Resume", variant="success", id="resume")
                yield Button("Start New", variant="primary", id="new")
                yield Button("Quit", variant="error", id="quit")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id)


class AlertModal(ModalScreen[None]):
    """A simple OK modal."""

    def __init__(self, message: str, dismissable: bool = True):
        super().__init__()
        self.message = message
        self.dismissable = dismissable

    def compose(self) -> ComposeResult:
        with Container(classes="modal"):
            yield Label(self.message, classes="question")
            if self.dismissable:
                yield Button("OK", variant="primary", id="ok")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(None)


class ResumeScreen(Screen[tuple[bool, bool, int, Optional[tuple]]]):
    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", priority=True),
    ]

    def __init__(
        self,
        state_manager: StateManager,
        failed_count: int,
        suspicious_count: int,
        current_speed: int,
    ):
        super().__init__()
        self.state_manager = state_manager
        self.failed_count = failed_count
        self.suspicious_count = suspicious_count
        self.current_speed = current_speed

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(classes="config-container"):
            yield Label("Resume Options", classes="heading")
            if self.failed_count > 0:
                yield Label(f"Re-insert {self.failed_count} failed items into queue:")
                yield Select(
                    [("Yes", True), ("No", False)],
                    value=False,
                    id="failed",
                    allow_blank=False,
                )
            if self.suspicious_count > 0:
                yield Label(
                    f"Re-insert {self.suspicious_count} suspicious items into queue:"
                )
                yield Select(
                    [("Yes", True), ("No", False)],
                    value=False,
                    id="suspicious",
                    allow_blank=False,
                )

            yield Label("\nSpeed:")
            yield Label(
                SPEED_SETTINGS[self.current_speed or 2][1],
                id="speed_desc",
                classes="help-text",
            )
            yield Select(
                [(v[0], k) for k, v in SPEED_SETTINGS.items()],
                value=self.current_speed or 2,
                id="speed",
                allow_blank=False,
            )

            with Horizontal(classes="buttons"):
                yield Button("Continue", variant="primary", id="submit")
                yield Button("Edit Queue", variant="default", id="edit")
                yield Button("Quit", variant="error", id="quit")
        yield Footer()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "speed" and event.value != Select.BLANK:
            val = cast(int, event.value)
            desc = SPEED_SETTINGS[val][1]
            self.query_one("#speed_desc", Label).update(desc)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "edit":
            res = await self.app.push_screen_wait(FilterModal(self.state_manager))
            if res:
                # FilterModal succeeded and returned (client, series)
                failed = (
                    cast(bool, self.query_one("#failed", Select).value)
                    if self.failed_count > 0
                    else False
                )
                suspicious = (
                    cast(bool, self.query_one("#suspicious", Select).value)
                    if self.suspicious_count > 0
                    else False
                )
                # Use current speed from state since FilterModal might have updated it
                speed = self.state_manager.speed
                self.dismiss((failed, suspicious, speed, res))
            return

        if event.button.id == "submit":
            failed = (
                cast(bool, self.query_one("#failed", Select).value)
                if self.failed_count > 0
                else False
            )
            suspicious = (
                cast(bool, self.query_one("#suspicious", Select).value)
                if self.suspicious_count > 0
                else False
            )

            speed_val = self.query_one("#speed", Select).value
            speed = cast(int, speed_val) if speed_val != Select.BLANK else 2

            if speed == 4:
                confirm = await self.app.push_screen_wait(
                    QuestionModal(
                        "Turbo speed is DANGEROUS!",
                        "This speed queues everything at once! "
                        "This is difficult to stop once started, and most "
                        "indexers will rate limit or temporarily ban you.\n\n"
                        "Are you sure you want to proceed?",
                    )
                )
                if not confirm:
                    return

            self.dismiss((failed, suspicious, speed, None))
        elif event.button.id == "quit":
            await self.app.action_quit()


class ConfigurationScreen(Screen):
    """Screen for configuring connection and preferences."""

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", priority=True),
    ]

    def __init__(self, state_manager: StateManager):
        super().__init__()
        self.state = state_manager

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(classes="config-container"):
            yield Label("[b]Configuration[/b]", classes="heading")
            yield Label("Sonarr URL:")
            yield Input(
                value=self.state.sonarr_url,
                placeholder="http://localhost:8989",
                id="url",
            )
            yield Label("API Key:")
            yield Input(
                value=self.state.api_key, placeholder="API Key", password=True, id="key"
            )
            yield Label("Speed:")
            yield Label(
                SPEED_SETTINGS[self.state.speed or 2][1],
                id="speed_desc",
                classes="help-text",
            )
            yield Select(
                [(v[0], k) for k, v in SPEED_SETTINGS.items()],
                value=self.state.speed or 2,
                id="speed",
                allow_blank=False,
            )
            yield Label("\n[b]Filters[/b]", classes="heading")
            yield Label("Monitored Only:")
            yield Select(
                [("Yes", True), ("No", False)],
                value=True,
                id="monitored",
                allow_blank=False,
            )
            yield Label("Status:")
            yield Select.from_values(
                ["All", "Continuing", "Ended", "Upcoming", "Deleted"],
                value="All",
                id="status",
            )
            yield Label("Series Type:")
            yield Select.from_values(
                ["All", "Standard", "Daily", "Anime"], value="All", id="series_type"
            )
            yield Label("Directory (Optional):")
            yield Input(placeholder="e.g. /tv", id="root_dir")
            yield Label("Max Episode Count (Optional):")
            yield Input(placeholder="e.g. 100", id="max_eps", type="number")
            yield Label("Min Year (Optional):")
            yield Input(placeholder="e.g. 1990", id="min_year", type="number")
            yield Label("Max Year (Optional):")
            yield Input(placeholder="e.g. 2025", id="max_year", type="number")
            yield Label("Genre (Optional):")
            yield Input(placeholder="e.g. Action", id="genre")
            yield Label("\n[b]Advanced Filters (Slow)[/b]", classes="heading")
            yield Label("Only include series with 'Cutoff Unmet' files:")
            yield Select(
                [("Yes", True), ("No", False)],
                value=False,
                id="cutoff_unmet",
                allow_blank=False,
            )
            yield Label("Only include series with negative 'Custom Format Score' files:")
            yield Select(
                [("Yes", True), ("No", False)],
                value=False,
                id="negative_score",
                allow_blank=False,
            )

            yield Button("Start", variant="success", id="start", classes="submit-btn")
        yield Footer()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "speed" and event.value != Select.BLANK:
            val = cast(int, event.value)
            desc = SPEED_SETTINGS[val][1]
            self.query_one("#speed_desc", Label).update(desc)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "start":
            return

        # Save inputs
        url = self.query_one("#url", Input).value
        key = self.query_one("#key", Input).value
        root_dir = self.query_one("#root_dir", Input).value
        max_eps_str = self.query_one("#max_eps", Input).value
        speed_val = self.query_one("#speed", Select).value
        min_year_str = self.query_one("#min_year", Input).value
        max_year_str = self.query_one("#max_year", Input).value
        genre = self.query_one("#genre", Input).value
        status = self.query_one("#status", Select).value
        series_type = self.query_one("#series_type", Select).value
        monitored_val = self.query_one("#monitored", Select).value
        monitored = cast(bool, monitored_val)
        cutoff_unmet = cast(bool, self.query_one("#cutoff_unmet", Select).value)
        negative_score = cast(bool, self.query_one("#negative_score", Select).value)

        if not url or not key:
            self.app.push_screen(AlertModal("URL and API Key are required."))
            return

        self.state.sonarr_url = url
        self.state.api_key = key

        try:
            max_eps = int(max_eps_str) if max_eps_str else float("inf")
        except ValueError:
            max_eps = float("inf")

        try:
            min_year = int(min_year_str) if min_year_str else 0
        except ValueError:
            min_year = 0

        try:
            max_year = int(max_year_str) if max_year_str else 0
        except ValueError:
            max_year = 0

        speed = cast(int, speed_val) if speed_val != Select.BLANK else 2

        if speed == 4:
            confirm = await self.app.push_screen_wait(
                QuestionModal(
                    "Turbo speed is DANGEROUS!",
                    "This speed queues everything at once! "
                    "This is difficult to stop once started, and most "
                    "indexers will rate limit or temporarily ban you.\n\n"
                    "Are you sure you want to proceed?",
                )
            )
            if not confirm:
                return

        self.state.speed = cast(Literal[0, 1, 2, 3, 4], speed)

        # Reset queue if we are configuring from scratch (not resuming)
        # Logic: If we are here, we are not resuming, OR we are resuming but wanted to reconfig.
        # But the 'Resume' flow skips this screen. So we can assume new filters apply.
        # However, we need to fetch series first to filter.

        if cutoff_unmet or negative_score:
            loading_screen = ProgressModal("Deep Scanning Filters...", 100) # Total will be updated
        else:
            loading_screen = AlertModal("Connecting to Sonarr...", dismissable=False)
        
        self.app.push_screen(loading_screen)

        # Run connection check in worker to not block UI
        self.run_connection_and_filter(
            url,
            key,
            root_dir,
            max_eps,
            min_year,
            max_year,
            genre,
            status,
            series_type,
            monitored,
            cutoff_unmet,
            negative_score,
            loading_screen,
        )

    @work(thread=True)
    def run_connection_and_filter(
        self,
        url,
        key,
        root_dir,
        max_eps,
        min_year,
        max_year,
        genre,
        status,
        series_type,
        monitored,
        cutoff_unmet,
        negative_score,
        loading_modal,
    ):
        try:
            self.app: SonarrRedownloader
            client = SonarrClient(
                url, key, dummy_mode=self.app.state_manager.dummy_mode
            )
            series = client.get_all_series()
        except Exception as e:
            self.app.call_from_thread(loading_modal.dismiss, None)
            self.app.call_from_thread(
                self.app.push_screen, AlertModal(f"Connection failed: {str(e)}")
            )
            return

        # Filter
        queue = filter_series(
            series,
            root_dir,
            max_eps,
            min_year,
            max_year,
            genre,
            status,
            series_type,
            monitored,
        )

        if cutoff_unmet or negative_score:
            if isinstance(loading_modal, ProgressModal):
                def set_total():
                    loading_modal.query_one(ProgressBar).update(total=len(queue), progress=0)
                self.app.call_from_thread(set_total)

            filtered_queue = []
            for i, sid in enumerate(queue):
                try:
                    eps = client.get_episode_files(sid)
                    include = False
                    for ep in eps:
                        if cutoff_unmet and ep.get("qualityCutoffNotMet"):
                            include = True
                            break
                        if negative_score and ep.get("customFormatScore", 0) < 0:
                            include = True
                            break
                    if include:
                        filtered_queue.append(sid)
                except Exception as e:
                    logger.error(f"Error filtering series {sid}: {e}")

                if isinstance(loading_modal, ProgressModal):
                    self.app.call_from_thread(
                        loading_modal.update_progress, 1, f"Checking {i + 1}/{len(queue)}"
                    )
            
            queue = filtered_queue

        self.app.call_from_thread(loading_modal.dismiss, None)

        # Update state
        self.state.reset()
        self.state.queue = set(queue)
        self.state.save()

        # Start main processing
        self.app.call_from_thread(self.dismiss, (client, series))


class MainScreen(Screen):
    """The main processing screen."""

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", priority=True),
        Binding("shift+p", "toggle_pause", "Pause"),
        Binding("shift+e", "end", "End Task"),
    ]

    def update_progress_text(self):
        progress = self.query_one("#progress_bar", ProgressBar)
        if not progress:
            return

        # Explicitly calculate percentage to ensure 0-100 scale
        if progress.total and progress.total > 0:
            percent = (progress.progress / progress.total) * 100
        else:
            percent = 0

        eta = (
            humanized_eta(self.state.time_estimate, default="Calculating time")
            + " remaining"
        )
        if STOP_EVENT.is_set():
            eta += " (STOPPED)"
        elif PAUSE_EVENT.is_set():
            eta += " (PAUSED)"

        self.query_one("#percentage_text", Static).update(
            f"{percent:.1f}% Complete\n{eta}"
        )

    def action_end(self):
        if not STOP_EVENT.is_set():
            logger.info("Stopping current task...")
        STOP_EVENT.set()
        self.update_progress_text()

    def action_toggle_pause(self):
        if STOP_EVENT.is_set():
            logger.info("Cannot pause/resume while stopped.")
        elif PAUSE_EVENT.is_set():
            PAUSE_EVENT.clear()
            if self.last_pause_start:
                paused_duration = (
                    datetime.now() - self.last_pause_start
                ).total_seconds()
                self.total_paused_seconds += paused_duration
                self.last_pause_start = None
            logger.info("Resumed by user.")
        else:
            PAUSE_EVENT.set()
            self.last_pause_start = datetime.now()
            logger.info("Paused by user.")
        self.update_progress_text()

    def __init__(
        self, state_manager: StateManager, client: SonarrClient, all_series: List[Dict]
    ):
        super().__init__()
        self.state = state_manager
        self.client = client
        self.all_series = all_series
        self.max_failures = 0

        # Progress Tracking
        self.initial_queue_length = len(self.state.queue)
        self.start_time = datetime.now()

        # Pause Tracking
        self.total_paused_seconds = 0.0
        self.last_pause_start = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(classes="main-container"):
            with Container(classes="status-box"):
                with Grid(classes="status-grid"):
                    with Vertical(classes="status-item box-total"):
                        yield Label("Remaining", classes="status-label")
                        yield Static("0", id="remaining_count", classes="status-value")

                    with Vertical(classes="status-item box-completed"):
                        yield Label("Completed", classes="status-label")
                        yield Static("0", id="completed_count", classes="status-value")


                    with Vertical(classes="status-item box-failed"):
                        yield Label("Failed", classes="status-label")
                        yield Static(
                            "0", id="failed_count", classes="status-value failure"
                        )

                    with Vertical(classes="status-item box-suspicious"):
                        yield Label("Suspicious", classes="status-label")
                        yield Static("0", id="suspicious_count", classes="status-value")

                yield Static("", id="percentage_text", classes="percentage-text")
                yield ProgressBar(
                    total=100, show_eta=False, show_percentage=False, id="progress_bar"
                )

            yield Log(id="log_view")

        yield Footer()

    def on_mount(self):
        # Setup logging
        log_widget = self.query_one("#log_view", Log)
        handler = TextualLogHandler(log_widget)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

        # Calc max failures
        if self.state.queue:
            self.max_failures = round(len(self.state.queue) * MAX_FAILURE_RATIO)

        self.start_processing()

    @work(thread=True)
    def start_processing(self):
        STOP_EVENT.clear()
        initial_queue_len = len(self.state.queue)
        if initial_queue_len == 0:
            logger.info("No series to process.")
            self.app.call_from_thread(self.finished_processing)
            return

        active_futures: Dict[Future, int] = {}
        current_failures = 0
        total_episodes = self._calculate_total_episodes(self.state.queue)

        with ThreadPoolExecutor(max_workers=3) as executor:
            for position, series_id in enumerate(self.state.queue.copy()):
                # Exit during a stop event
                if STOP_EVENT.is_set():
                    break

                # Infinitely wait during 'PAUSE'
                # The user might also issue a `stop` while paused, so we must check that.
                while PAUSE_EVENT.is_set() and not STOP_EVENT.is_set():
                    time.sleep(0.5)

                if current_failures > self.max_failures:
                    logger.error("Too many failures. Aborting.")
                    break

                series = self._get_series_by_id(series_id)
                if not series:
                    logger.warning(f"Series ID {series_id} has been removed from Sonarr. Skipping...")
                    self.state.queue.remove(series_id)
                    self.state.save()
                    continue

                # Update UI
                self.app.call_from_thread(
                    self.update_status,
                    len(self.state.completed),
                    len(self.state.failures),
                    len(self.state.suspicious),
                    initial_queue_len,
                    total_episodes,
                )

                # Trigger Search
                try:
                    command_id = self.client.search_series(series_id)
                except Exception as e:
                    logger.error(f"Error triggering search for {series['title']}: {e}")
                    current_failures += 1
                    continue

                if not command_id:
                    logger.error(f"[{series['title']}] No command ID returned.")
                    current_failures += 1
                    continue

                logger.info(f"[{series['title']}] Search started.")

                if self.state.speed == 4:
                    continue

                f = executor.submit(
                    self.client.wait_for_search, command_id, series["title"]
                )
                active_futures[f] = series_id

                if len(active_futures) >= self.state.speed:
                    current_failures += self._check_failure(
                        active_futures, initial_queue_len, total_episodes
                    )

            # Wait for remaining
            while (
                active_futures
                and current_failures <= self.max_failures
                and not STOP_EVENT.is_set()
            ):
                current_failures += self._check_failure(
                    active_futures, initial_queue_len, total_episodes
                )

        self.app.call_from_thread(self.finished_processing)

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
        # Sometimes 'SeriesSearch' falls back to searching for individual episodes.
        # Since we don't know if this will occur, we must average several estimate
        # methods to improve accuracy.
        remaining_searches = len(self.state.queue)
        if remaining_searches < queue_length:
            remaining_eps = self._calculate_total_episodes(self.state.queue)
            elapsed = (datetime.now() - start_time).total_seconds()

            # Adjust for pause time
            elapsed -= self.total_paused_seconds
            if PAUSE_EVENT.is_set() and self.last_pause_start:
                elapsed -= (datetime.now() - self.last_pause_start).total_seconds()
            if elapsed <= 0:
                return

            # Time estimate based on remaining episodes
            eps_completed = episode_count - remaining_eps
            est_ep = (
                (elapsed / eps_completed) * remaining_eps if eps_completed > 0 else 0
            )

            # Time estimate based on remaining series count
            session_completed = queue_length - remaining_searches
            if session_completed > 0:
                est_series = (elapsed / session_completed) * remaining_searches
            else:
                est_series = 0

            # Set the value within the state storage
            new_estimate = 0
            if est_ep > 0 and est_series > 0:
                new_estimate = int((est_ep + est_series) / 2)
            elif est_ep > 0:
                new_estimate = int(est_ep)
            elif est_series > 0:
                new_estimate = int(est_series)

            # Average with previous estimate for smoothing, if possible
            if new_estimate > 0:
                if self.state.time_estimate > 0:
                    # Weight recent estimate lower to reduce volatility
                    self.state.time_estimate = int(
                        self.state.time_estimate * 0.8 + new_estimate * 0.2
                    )
                else:
                    self.state.time_estimate = new_estimate

    def _check_failure(
        self,
        futures: Dict[Future, int],
        initial_queue_len: int,
        initial_total_episodes: int,
    ) -> int:
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

                if series_id in self.state.queue:
                    self.state.queue.remove(series_id)
            except Exception as e:
                failures += 1
                logger.error(f"Error in future result: {e}")

        self.state.save()

        self.app.call_from_thread(
            self.update_status,
            len(self.state.completed),
            len(self.state.failures),
            len(self.state.suspicious),
            initial_queue_len,
            initial_total_episodes,
        )
        return failures

    def update_status(
        self,
        completed: int,
        failed: int,
        suspicious: int,
        initial_queue_len: int,
        initial_total_ep: int,
    ):
        total_items = (
            len(self.state.completed) + len(self.state.failures) + len(self.state.queue)
        )

        progress = self.query_one("#progress_bar", ProgressBar)
        progress.total = total_items
        progress.progress = completed + failed

        self.query_one("#remaining_count", Static).update(str(len(self.state.queue)))
        self.query_one("#completed_count", Static).update(str(completed))
        self.query_one("#failed_count", Static).update(str(failed))
        self.query_one("#suspicious_count", Static).update(str(suspicious))

        if not PAUSE_EVENT.is_set():
            self._calculate_time_estimate(
                initial_queue_len, initial_total_ep, self.start_time
            )

        self.update_progress_text()

    async def finished_processing(self):
        logger.info("Processing complete!")
        retry = False
        if self.state.failures and await self.app.push_screen_wait(
            QuestionModal("Retry failed items?")
        ):
            self.state.requeue_failures()
            retry = True

        if (
            not retry
            and self.state.suspicious
            and await self.app.push_screen_wait(
                QuestionModal("Retry suspicious items?")
            )
        ):
            self.state.requeue_suspicious()
            retry = True

        if retry:
            self.state.save()
            self.query_one("#log_view", Log).write_line("--- RETRYING ---")
            self.start_processing()
        else:
            self.query_one("#log_view", Log).write_line(
                "All Done. You may now exit the application."
            )
            self.app.bell()


class SonarrRedownloader(App):
    CSS = """
    Screen {
        layout: vertical;
    }

    ModalScreen {
        align: center middle;
    }
    
    .modal {
        background: $surface;
        padding: 2;
        border: heavy $accent;
        width: 60;
        height: auto;
        align: center middle;
    }
    
    .question {
        text_align: center;
        margin-bottom: 2;
    }
    
    .buttons {
        align: center middle;
        height: auto;
    }
    
    Button {
        margin: 1;
    }
    
    .config-container {
        padding: 2;
        width: 100%;
        height: 1fr;
        align: center middle;
        background: $surface;
        overflow-y: auto;
    }

    .config-container > * {
        width: 100%;
        max-width: 100;
    }

    
    .heading {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
        color: $accent-lighten-2;
    }
    
    .help-text {
        color: $text-muted;
    }
    
    .submit-btn {
        margin-top: 2;
    }

    Checkbox {
        border: none;
        padding-left: 0;
    }
   
    .status-box {
        height: auto;
        padding: 1;
        margin-bottom: 1;
        background: $surface;
    }

    .status-grid {
        grid-size: 4;
        grid-gutter: 1;
        height: auto;
    }

    .status-item {
        padding: 1;
        border: none;
        height: auto;
        text-align: center;
    }
    
    .box-completed {
        background: $success-darken-3;
        color: $text;
        border-left: wide $success;
    }
    
    .box-failed {
        background: $error-darken-3;
        color: $text;
        border-left: wide $error;
    }

    .box-suspicious {
        background: $accent-darken-3;
        color: $text;
        border-left: wide $accent;
    }
    
    .box-total {
        background: $primary-darken-3;
        color: $text;
        border-left: wide $primary;
    }

    .status-label {
        color: $text-disabled;
        text-align: center;
        width: 100%;
        margin-bottom: 1;
    }
    
    .status-value {
        text-align: center;
        text-style: bold;
        width: 100%;
        color: $text;
    }

    .failure {
        color: $error-lighten-1;
    }

    ProgressBar, ProgressBar Bar {
        width: 1fr;
    }

    Bar > .bar--indeterminate {
        color: $primary;
        background: $secondary;
    }

    Bar > .bar--bar {
        color: $primary;
        background: $primary 30%;
    }

    Bar > .bar--complete {
        color: $error;
    }

    .percentage-text {
        text-align: center;
        margin-top: 1;
        color: $text-muted;
    }
    
    Log {
        height: 1fr;
        background: $surface;
        padding: 1;
        scrollbar-size: 0 1;
        scrollbar-color:  $primary;
        scrollbar-background: $secondary;
        scrollbar-corner-color: $secondary;
    }
    """

    def __init__(self, dummy_mode: bool = False):
        super().__init__()
        self.state_manager = StateManager(dummy_mode=dummy_mode)

    def on_mount(self):
        self.check_setup()

    async def action_quit(self):
        STOP_EVENT.set()
        self.exit()

    @work
    async def check_setup(self):
        has_state = self.state_manager.load()

        if has_state:
            action = await self.push_screen_wait(StartupScreen())
            if action == "quit":
                await self.action_quit()
                return

            should_resume = action == "resume"
            if should_resume:
                # Ask about re-queueing failed/suspicious
                (
                    requeue_failed,
                    requeue_suspicious,
                    new_speed,
                    ready_data,
                ) = await self.push_screen_wait(
                    ResumeScreen(
                        self.state_manager,
                        len(self.state_manager.failures),
                        len(self.state_manager.suspicious),
                        self.state_manager.speed or 2,
                    )
                )

                self.state_manager.speed = cast(Literal[0, 1, 2, 3, 4], new_speed)

                if requeue_failed:
                    self.state_manager.requeue_failures()

                if requeue_suspicious:
                    self.state_manager.requeue_suspicious()

                if requeue_failed or requeue_suspicious:
                    self.state_manager.save()

                if ready_data:
                    client, series = ready_data
                    self.push_screen(MainScreen(self.state_manager, client, series))
                    return

                # Try connecting with saved credentials
                try:
                    client = SonarrClient(
                        self.state_manager.sonarr_url,
                        self.state_manager.api_key,
                        dummy_mode=self.state_manager.dummy_mode,
                    )
                    series = (
                        client.get_all_series()
                    )  # Just to verify connectivity and get series for titles
                    self.push_screen(MainScreen(self.state_manager, client, series))
                    return
                except Exception as e:
                    await self.push_screen_wait(
                        AlertModal(f"Could not resume: {e}. Please reconfigure.")
                    )

        # If not resumed or failed to resume
        res = await self.push_screen_wait(ConfigurationScreen(self.state_manager))
        if res:
            client, series = res
            self.push_screen(MainScreen(self.state_manager, client, series))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sonarr Redownloader TUI")
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run in dummy mode (no actual searches, no state saving)",
    )
    args = parser.parse_args()

    # Simple file lock to prevent multiple instances
    LOCK_FILE = DATA_PATH / "redownloader.lock"
    if LOCK_FILE.exists():
        try:
            pid = LOCK_FILE.read_text().strip()
            print(f"Error: Another instance is running (PID {pid}).")
            delete_lockfile = input(
                "If you are sure no other instance is running, type 'Y' to continue: "
            )
            if delete_lockfile.lower() == "y":
                LOCK_FILE.unlink()
                print("Lock file deleted. Continuing...")
            else:
                sys.exit(1)
        except Exception:
            print(
                "Error: Lock file 'redownloader.lock' exists. Another instance may be running."
            )
            sys.exit(1)

    with open(LOCK_FILE, "x") as f:
        f.write(str(os.getpid()))

    def cleanup_lock():
        """Remove the lock file on exit."""
        if LOCK_FILE.exists():
            with contextlib.suppress(Exception):
                LOCK_FILE.unlink()

    atexit.register(cleanup_lock)

    app = SonarrRedownloader(dummy_mode=args.dummy)
    try:
        app.run()
    finally:
        STOP_EVENT.set()
        cleanup_lock()
        sys.exit(0)
