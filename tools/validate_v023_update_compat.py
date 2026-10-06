from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from rocketcat_shell.update_manifest import (  # noqa: E402
    MANAGED_DIRECTORIES,
    MANAGED_FILES,
    MANIFEST_NAME,
    inspect_and_extract_zip,
)
from rocketcat_shell.updates import UpdateService  # noqa: E402
from tools.stress_v022_full_stack import FakeOneBot, FakeRocketChat  # noqa: E402


OFFICIAL_V022_URL = (
    "https://github.com/Creeper3222/RocketCat/releases/download/"
    "v0.2.2/RocketCatShell-v0.2.2.zip"
)
OFFICIAL_V022_SHA256 = (
    "879288ea53f7a48331589c8772a2e590bff89373eb2bf203d4d4860620cf87de"
)
OFFICIAL_V023_URL = (
    "https://github.com/Creeper3222/RocketCat/releases/download/"
    "v0.2.3/RocketCatShell-v0.2.3.zip"
)
OFFICIAL_V023_SHA256 = (
    "2054c27ed14a22d945828a6a6c6c421afb106166aa699ecbb41dcb90b70ec7f4"
)
OFFICIAL_OLD_RELEASES = {
    "v0.2.2": (OFFICIAL_V022_URL, OFFICIAL_V022_SHA256),
    "v0.2.3": (OFFICIAL_V023_URL, OFFICIAL_V023_SHA256),
}
GITHUB_REPOSITORY = "Creeper3222/RocketCat"
GITHUB_RELEASE_API = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/releases/tags"
ALLOWED_RELEASE_HOSTS = {
    "github.com",
    "objects.githubusercontent.com",
    "release-assets.githubusercontent.com",
}
TERMINAL_TRANSACTION_STATES = {
    "completed",
    "failed",
    "rolled_back",
    "recovery_required",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _download(
    url: str,
    destination: Path,
    *,
    expected_sha256: str,
    expected_size: int = 0,
) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "github.com":
        raise RuntimeError("release download URL is not an official GitHub HTTPS URL")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "RocketCatShell/v0.2.4-update-validation"},
    )
    digest = hashlib.sha256()
    total = 0
    with urllib.request.urlopen(request, timeout=90) as response, destination.open(
        "xb"
    ) as output:
        final_url = urllib.parse.urlsplit(response.geturl())
        if (
            final_url.scheme.lower() != "https"
            or (final_url.hostname or "").lower() not in ALLOWED_RELEASE_HOSTS
        ):
            raise RuntimeError("release download left the official TLS hosts")
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            digest.update(chunk)
            output.write(chunk)
    actual = digest.hexdigest()
    if actual.lower() != expected_sha256.lower():
        raise RuntimeError(
            f"release SHA-256 mismatch: {actual} != {expected_sha256}"
        )
    if expected_size > 0 and total != expected_size:
        raise RuntimeError(f"release size mismatch: {total} != {expected_size}")
    return actual


def _official_release_asset(tag: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{GITHUB_RELEASE_API}/{urllib.parse.quote(tag, safe='')}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "RocketCatShell/v0.2.4-update-validation",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if payload.get("tag_name") != tag or payload.get("draft"):
            raise RuntimeError(f"official release metadata does not match {tag}")
        release_url = str(payload.get("html_url") or "")
        prerelease = bool(payload.get("prerelease"))
        asset = next(
            (
                item
                for item in payload.get("assets") or []
                if isinstance(item, dict)
                and item.get("name") == f"RocketCatShell-{tag}.zip"
            ),
            None,
        )
    except urllib.error.HTTPError as exc:
        if exc.code not in {403, 429}:
            raise
        feed = UpdateService._fallback_release_feed()
        payload = next(
            (item for item in feed if item.get("tag_name") == tag),
            None,
        )
        if payload is None:
            raise RuntimeError(f"official release feed does not contain {tag}") from exc
        release_url = str(payload.get("html_url") or "")
        prerelease = bool(payload.get("prerelease"))
        asset = payload.get("asset")
    expected_name = f"RocketCatShell-{tag}.zip"
    expected_url = UpdateService.official_asset_url(tag)
    digest = str((asset or {}).get("digest") or "").lower()
    if (
        asset is None
        or asset.get("name") != expected_name
        or asset.get("browser_download_url", asset.get("url")) != expected_url
        or not digest.startswith("sha256:")
        or len(digest) != len("sha256:") + 64
        or any(character not in "0123456789abcdef" for character in digest.split(":", 1)[-1])
    ):
        raise RuntimeError(f"official release asset metadata is invalid for {tag}")
    try:
        size = int(asset.get("size") or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"official release asset size is invalid for {tag}") from exc
    if size < 0:
        raise RuntimeError(f"official release asset size is invalid for {tag}")
    return {
        "tag": tag,
        "url": expected_url,
        "name": expected_name,
        "sha256": digest.split(":", 1)[1],
        "size": size,
        "prerelease": prerelease,
        "release_url": release_url,
    }


async def _wait_until(predicate, *, timeout: float, interval: float = 0.1) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise TimeoutError("condition did not become true before timeout")


class HeaderObservingOneBot(FakeOneBot):
    def __init__(self) -> None:
        self.headers: list[dict[str, str]] = []
        super().__init__()

    async def websocket(self, request):
        self.headers.append(
            {
                "authorization": str(request.headers.get("Authorization") or ""),
                "x_self_id": str(request.headers.get("X-Self-ID") or ""),
                "user_agent": str(request.headers.get("User-Agent") or ""),
            }
        )
        return await super().websocket(request)

    def reset_observations(self) -> None:
        super().reset_observations()
        self.message_events = 0
        self.events: list[dict[str, Any]] = []

    def _observe_message(self, self_id: int, payload: dict[str, Any]) -> None:
        self.message_events += 1
        self.events.append(dict(payload))
        super()._observe_message(self_id, payload)

    async def send_action(
        self,
        action: str,
        params: dict[str, Any],
        *,
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        async with self.client_lock:
            socket = next(iter(self.clients.values()), None)
        if socket is None:
            raise RuntimeError("OneBot client is not connected")
        echo = f"compat-{time.time_ns()}"
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self.pending_actions[echo] = (time.perf_counter(), future)
        try:
            await socket.send_json({"action": action, "params": params, "echo": echo})
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self.pending_actions.pop(echo, None)


@dataclass
class ValidationReport:
    started_at: str
    root: str
    source_release: dict[str, Any] = field(default_factory=dict)
    target_release: dict[str, Any] = field(default_factory=dict)
    bots: dict[str, Any] = field(default_factory=dict)
    transactions: list[dict[str, Any]] = field(default_factory=list)
    protected_state: dict[str, Any] = field(default_factory=dict)
    runtime_checks: list[str] = field(default_factory=list)
    passed: bool = False
    error: str = ""
    completed_at: str = ""

    def public_mapping(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "root": self.root,
            "source_release": self.source_release,
            "target_release": self.target_release,
            "bots": self.bots,
            "transactions": self.transactions,
            "protected_state": self.protected_state,
            "runtime_checks": self.runtime_checks,
            "passed": self.passed,
            "error": self.error,
        }


class CompatibilityHarness:
    def __init__(
        self,
        *,
        root: Path,
        source_tag: str,
        target_tag: str,
        password: str,
    ) -> None:
        self.root = root
        self.source_tag = source_tag
        self.target_tag = target_tag
        self.password = password
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.archive = root / f"RocketCatShell-{source_tag}.zip"
        self.extract_root = root / "source-extracted"
        self.target_archive = root / f"RocketCatShell-{target_tag}-reference.zip"
        self.target_extract_root = root / "target-reference-extracted"
        self.install_root: Path | None = None
        self.target_reference_root: Path | None = None
        self.launch_process: asyncio.subprocess.Process | None = None
        self.launch_log_handle: Any = None
        self.fake_rc = FakeRocketChat()
        self.fake_onebot_initial = HeaderObservingOneBot()
        self.report = ValidationReport(
            started_at=datetime.now().astimezone().isoformat(),
            root=str(root),
        )
        self._last_transaction_id = ""
        self._pre_update_message_id = ""
        self._protected_paths: dict[str, Path] = {}
        self._source_onebot_self_id = 0
        self._target_health_payload: dict[str, Any] = {}

    async def prepare(self) -> None:
        expected_release = OFFICIAL_OLD_RELEASES.get(self.source_tag)
        if expected_release is None:
            raise RuntimeError(f"unsupported source release: {self.source_tag}")
        source_asset = await asyncio.to_thread(_official_release_asset, self.source_tag)
        if source_asset["prerelease"]:
            raise RuntimeError(f"source release must be stable: {self.source_tag}")
        source_url, expected_sha256 = expected_release
        if (source_asset["url"], source_asset["sha256"].lower()) != (
            source_url,
            expected_sha256.lower(),
        ):
            raise RuntimeError(
                f"official {self.source_tag} metadata does not match its pinned URL or SHA-256"
            )
        self.root.mkdir(parents=True, exist_ok=False)
        actual = await asyncio.to_thread(
            _download,
            source_url,
            self.archive,
            expected_sha256=expected_sha256,
            expected_size=int(source_asset["size"]),
        )
        candidate_root, manifest = await asyncio.to_thread(
            inspect_and_extract_zip,
            self.archive,
            self.extract_root,
            expected_tag=self.source_tag,
        )
        self.install_root = candidate_root
        self.report.source_release = {
            "tag": self.source_tag,
            "url": source_url,
            "sha256": actual,
            "size": self.archive.stat().st_size,
            "asset_size": source_asset["size"],
            "prerelease": source_asset["prerelease"],
            "release_url": source_asset["release_url"],
            "manifest_version": manifest.get("version"),
            "manifest_files": len(manifest.get("files") or []),
        }
        target_asset = await asyncio.to_thread(
            _official_release_asset,
            self.target_tag,
        )
        if not target_asset["prerelease"]:
            raise RuntimeError(f"{self.target_tag} must be a prerelease for this workflow")
        self.target_reference_root, target_manifest = await self._download_target_reference(
            target_asset
        )
        self.report.target_release = {
            **target_asset,
            "asset_size": target_asset["size"],
            "size": self.target_archive.stat().st_size,
            "manifest_version": target_manifest.get("version"),
            "manifest_files": len(target_manifest.get("files") or []),
        }
        shell_settings = {
            "webui_host": "127.0.0.1",
            "webui_port": self.port,
            "webui_access_password": self.password,
            "auto_open_browser": False,
        }
        _atomic_json(candidate_root / "config" / "shell.json", shell_settings)
        await self._write_protected_sentinels()

    async def _download_target_reference(
        self,
        asset: dict[str, Any],
    ) -> tuple[Path, dict[str, Any]]:
        actual = await asyncio.to_thread(
            _download,
            str(asset["url"]),
            self.target_archive,
            expected_sha256=str(asset["sha256"]),
            expected_size=int(asset["size"]),
        )
        if actual != asset["sha256"]:
            raise RuntimeError("target release reference digest changed during download")
        candidate_root, manifest = await asyncio.to_thread(
            inspect_and_extract_zip,
            self.target_archive,
            self.target_extract_root,
            expected_tag=self.target_tag,
        )
        return candidate_root, manifest

    async def _write_protected_sentinels(self) -> None:
        root = self._require_install_root()
        sentinels = {
            "config/compat-preserved.bin": b"config-persistent-sentinel-v024\0",
            "data/bots/compat-preserved.bin": b"bot-runtime-persistent-sentinel-v024\0",
            "data/plugins/user_compat_plugin/compat-preserved.bin": b"user-plugin-sentinel\0",
            "data/plugin_data/compat-preserved.bin": b"plugin-data-sentinel\0",
        }
        for relative, payload in sentinels.items():
            path = root / Path(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        identity_database = root / "data" / "user_identity" / "compat-preserved.sqlite3"
        identity_database.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(identity_database) as connection:
            connection.execute(
                "CREATE TABLE compat_state (marker TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO compat_state(marker, payload) VALUES (?, ?)",
                ("upgrade-validation", "identity-database-sentinel-v024"),
            )
        sentinels["data/user_identity/compat-preserved.sqlite3"] = b""
        self._protected_paths = {
            relative: root / Path(relative) for relative in sentinels
        }
        self.report.protected_state["before"] = {
            relative: _sha256(path) for relative, path in self._protected_paths.items()
        }

    async def start_fakes(self) -> None:
        await self.fake_rc.start()
        await self.fake_onebot_initial.start()

    async def start_source(self) -> None:
        root = self._require_install_root()
        log_path = self.root / "launcher-output.log"
        self.launch_log_handle = log_path.open("ab", buffering=0)
        self.launch_process = await asyncio.create_subprocess_exec(
            "cmd.exe",
            "/d",
            "/c",
            str(root / "launcher.bat"),
            "--no-browser",
            cwd=str(root),
            stdin=subprocess.DEVNULL,
            stdout=self.launch_log_handle,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        await self.wait_health(self.source_tag, timeout=600)

    async def wait_health(
        self,
        version: str,
        *,
        timeout: float = 300,
        transaction_id: str = "",
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last_error = ""
        while time.monotonic() < deadline:
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=3),
                    trust_env=False,
                ) as session:
                    async with session.get(f"{self.base_url}/api/health") as response:
                        payload = await response.json()
                        if (
                            response.status == 200
                            and payload.get("status") == "ok"
                            and payload.get("version") == version
                            and (
                                not transaction_id
                                or (
                                    payload.get("transaction_id")
                                    or payload.get("update_transaction")
                                )
                                == transaction_id
                            )
                        ):
                            return payload
                        last_error = f"health={response.status} {payload}"
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                last_error = repr(exc)
            await asyncio.sleep(0.5)
        raise TimeoutError(f"health did not reach {version}: {last_error}")

    async def session(self) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession(
            cookie_jar=aiohttp.CookieJar(unsafe=True),
            timeout=aiohttp.ClientTimeout(total=120),
            trust_env=False,
        )
        response = await session.post(
            f"{self.base_url}/api/login",
            json={"password": self.password},
        )
        try:
            payload = await response.json()
            if response.status != 200:
                raise RuntimeError(f"login failed: {response.status} {payload}")
        finally:
            response.release()
        return session

    async def api(
        self,
        session: aiohttp.ClientSession,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        expected: int = 200,
    ) -> dict[str, Any]:
        async with session.request(
            method,
            f"{self.base_url}{path}",
            json=payload,
        ) as response:
            body = await response.json()
            if response.status != expected:
                raise RuntimeError(
                    f"{method} {path} failed: {response.status} {body}"
                )
            return body

    async def bot_from_api(
        self,
        session: aiohttp.ClientSession,
        bot_id: str,
    ) -> dict[str, Any]:
        # v0.2.2 has no GET /api/bots/{id}; use the list route shared by all
        # releases in this compatibility matrix.
        payload = await self.api(session, "GET", "/api/bots")
        item = next(
            (
                candidate
                for candidate in payload.get("items") or []
                if str(candidate.get("id") or candidate.get("bot_id") or "")
                == bot_id
            ),
            None,
        )
        if item is None:
            raise RuntimeError(f"Bot {bot_id} is missing from GET /api/bots")
        return item

    async def create_legacy_bots(self) -> dict[str, str]:
        if self._require_install_root().joinpath(
            "config", "onebot_transports.json"
        ).exists():
            raise RuntimeError("v0.2.2 unexpectedly created onebot_transports.json")
        async with await self.session() as session:
            payloads = [
                {
                    "name": "Compat Active",
                    "enabled": True,
                    "server_url": self.fake_rc.base_url,
                    "username": "compat-active",
                    "password": "synthetic-password",
                    "e2ee_password": "",
                    "onebot_ws_url": self.fake_onebot_initial.ws_url,
                    "onebot_access_token": "initial-token",
                    "reconnect_delay": 0.2,
                    "max_reconnect_attempts": 5,
                    "skip_own_messages": False,
                    "debug": True,
                },
                {
                    "name": "Compat Empty Token",
                    "enabled": False,
                    "server_url": "http://127.0.0.1:3000",
                    "username": "compat-disabled-a",
                    "password": "synthetic-password",
                    "onebot_ws_url": "ws://127.0.0.1:6199/ws/",
                    "onebot_access_token": "",
                    "skip_own_messages": True,
                    "debug": False,
                },
                {
                    "name": "Compat Custom Token",
                    "enabled": False,
                    "server_url": "http://127.0.0.1:3000",
                    "username": "compat-disabled-b",
                    "password": "synthetic-password",
                    "onebot_ws_url": "wss://onebot.invalid/ws",
                    "onebot_access_token": "custom-token",
                    "skip_own_messages": False,
                    "debug": True,
                },
            ]
            identifiers: dict[str, str] = {}
            for item in payloads:
                result = await self.api(session, "POST", "/api/bots", payload=item)
                created = result["item"]
                identifiers[item["name"]] = str(created["id"])
        await self.fake_rc.wait_clients(1, timeout=30)
        await self.fake_onebot_initial.wait_clients(1, timeout=30)
        if not any(
            item["authorization"] == "Bearer initial-token"
            for item in self.fake_onebot_initial.headers
        ):
            raise RuntimeError("v0.2.2 did not send the expected OneBot token")
        await self.fake_onebot_initial.send_actions("get_login_info")
        if self.fake_onebot_initial.action_failures:
            raise RuntimeError("v0.2.2 OneBot action round-trip failed")
        await self._send_test_event("before-update")
        self.report.runtime_checks.append("v0.2.2 event/action round-trip")
        await self._record_source_persistence(identifiers["Compat Active"])
        root = self._require_install_root()
        if (root / "config" / "onebot_transports.json").exists():
            raise RuntimeError("legacy install created a v0.2.3 transport file")
        self.report.bots["ids"] = identifiers
        self.report.bots["legacy_before_update"] = self._redacted_legacy_bots()
        return identifiers

    async def create_v023_bots(self) -> dict[str, str]:
        if not self._require_install_root().joinpath(
            "config", "onebot_transports.json"
        ).is_file():
            raise RuntimeError("v0.2.3 did not include onebot_transports.json")
        async with await self.session() as session:
            result = await self.api(
                session,
                "POST",
                "/api/bots",
                payload={
                    "name": "Compat Active",
                    "enabled": True,
                    "server_url": self.fake_rc.base_url,
                    "username": "compat-active",
                    "password": "synthetic-password",
                    "e2ee_password": "",
                    "onebot_transport": {
                        "type": "websocket-client",
                        "settings": {
                            "url": self.fake_onebot_initial.ws_url,
                            "message_post_format": "string",
                            "report_self_message": False,
                            "reconnect_interval_ms": 1700,
                            "heartbeat_interval_ms": 9000,
                            "access_token": "initial-token",
                            "debug": True,
                        },
                    },
                },
            )
        identifier = str(result["item"]["id"])
        await self.fake_rc.wait_clients(1, timeout=30)
        await self.fake_onebot_initial.wait_clients(1, timeout=30)
        if not any(
            item["authorization"] == "Bearer initial-token"
            for item in self.fake_onebot_initial.headers
        ):
            raise RuntimeError("v0.2.3 did not send the expected OneBot token")
        await self.fake_onebot_initial.send_actions("get_login_info")
        if self.fake_onebot_initial.action_failures:
            raise RuntimeError("v0.2.3 OneBot action round-trip failed")
        await self._send_test_event("before-update")
        self.report.runtime_checks.append("v0.2.3 event/action round-trip")
        self.report.bots["ids"] = {"Compat Active": identifier}
        self.report.bots["source_transport_settings"] = {
            "message_post_format": "string",
            "reconnect_interval_ms": 1700,
            "heartbeat_interval_ms": 9000,
        }
        await self._record_source_persistence(identifier)
        return {"Compat Active": identifier}

    async def _record_source_persistence(self, active_bot_id: str) -> None:
        root = self._require_install_root()
        async with await self.session() as session:
            item = await self.bot_from_api(session, active_bot_id)
        if int(item.get("onebot_self_id") or 0) <= 0:
            raise RuntimeError("source runtime did not establish its persistent identity mapping")
        self._source_onebot_self_id = int(item["onebot_self_id"])
        self.report.bots["source_onebot_self_id"] = self._source_onebot_self_id
        bot_config = json.loads((root / "config" / "bots.json").read_text(encoding="utf-8"))
        active_config = next(
            (
                candidate
                for candidate in bot_config.get("bots") or []
                if str(candidate.get("id") or candidate.get("bot_id") or "")
                == active_bot_id
            ),
            None,
        )
        if active_config is None:
            raise RuntimeError("source bot was not persisted in bots.json")
        if "forward_messages_to_thread" in active_config:
            raise RuntimeError("source release unexpectedly persisted the v0.2.4 thread setting")
        self.report.bots["source_missing_thread_setting"] = True
        marker_path = root / "data" / "bots" / active_bot_id / "compat-runtime-preserved.bin"
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_bytes(b"per-bot-runtime-persistent-sentinel-v024\0")
        relative = marker_path.relative_to(root).as_posix()
        self._protected_paths[relative] = marker_path
        before = self.report.protected_state["before"]
        before[relative] = _sha256(marker_path)
        identity_scope_path = root / "data" / "bots" / active_bot_id / "identity_scope.json"
        if not identity_scope_path.is_file():
            raise RuntimeError("source runtime did not persist its identity database scope")
        identity_scope = json.loads(identity_scope_path.read_text(encoding="utf-8"))
        database_path = Path(str(identity_scope.get("database_path") or ""))
        if not database_path.is_file():
            raise RuntimeError("source runtime identity database is missing")
        self.report.protected_state["runtime_identity_database"] = {
            "relative_path": database_path.relative_to(root).as_posix(),
            "size": database_path.stat().st_size,
        }

    async def create_source_bots(self) -> dict[str, str]:
        if self.source_tag == "v0.2.2":
            return await self.create_legacy_bots()
        if self.source_tag == "v0.2.3":
            return await self.create_v023_bots()
        raise RuntimeError(f"unsupported source release: {self.source_tag}")

    async def _send_test_event(self, marker: str) -> None:
        target = self.fake_onebot_initial
        before = target.message_events
        timestamp = int(time.time() * 1000)
        delivered = await self.fake_rc.inject(
            {
                "_id": f"compat-{marker}-{timestamp}",
                "rid": "room-0",
                "msg": f"compat {marker}",
                "ts": {"$date": timestamp},
                "_updatedAt": {"$date": timestamp},
                "u": {"_id": "compat-user", "username": "compat-user", "name": "Compat User"},
            }
        )
        if delivered < 1:
            raise RuntimeError(f"Rocket.Chat event was not delivered: {marker}")
        await _wait_until(lambda: target.message_events > before, timeout=15)
        if marker == "before-update":
            event = target.events[-1]
            message_id = str(event.get("message_id") or "")
            if not message_id:
                raise RuntimeError("OneBot event did not include a message_id")
            self._pre_update_message_id = message_id
            self.report.bots["pre_update_message_id"] = message_id

    def _redacted_legacy_bots(self) -> list[dict[str, Any]]:
        root = self._require_install_root()
        payload = json.loads(
            (root / "config" / "bots.json").read_text(encoding="utf-8")
        )
        result = []
        for bot in payload.get("bots") or []:
            result.append(
                {
                    "id": bot.get("id"),
                    "name": bot.get("name"),
                    "enabled": bool(bot.get("enabled")),
                    "onebot_ws_url": bot.get("onebot_ws_url"),
                    "has_token": bool(bot.get("onebot_access_token")),
                    "skip_own_messages": bool(bot.get("skip_own_messages")),
                    "debug": bool(bot.get("debug")),
                }
            )
        return result

    async def switch_target(self) -> dict[str, Any]:
        async with await self.session() as session:
            releases = await self.api(
                session,
                "GET",
                "/api/updates/releases?refresh=true",
            )
            if releases.get("stale"):
                raise RuntimeError("old installation could not refresh official release metadata")
            candidate = next(
                (
                    item
                    for item in releases.get("releases") or []
                    if item.get("tag_name") == self.target_tag
                ),
                None,
            )
            if candidate is None:
                raise RuntimeError(f"update candidate is unavailable: {self.target_tag}")
            asset = candidate.get("asset") or {}
            expected = self.report.target_release
            checks = {
                "action": candidate.get("action") == "update",
                "prerelease": candidate.get("prerelease") is True,
                "name": asset.get("name") == expected.get("name"),
                "digest": asset.get("digest") == f"sha256:{expected.get('sha256')}",
                "size": int(asset.get("size") or 0) == int(expected.get("asset_size") or 0),
            }
            if not all(checks.values()):
                raise RuntimeError(f"old installation release metadata mismatch: {checks}")
            transaction = await self.api(
                session,
                "POST",
                "/api/updates/switch",
                payload={"tag_name": self.target_tag},
            )
        transaction_id = str(transaction.get("transaction_id") or "")
        if not transaction_id:
            raise RuntimeError("update transaction did not return an id")
        self._last_transaction_id = transaction_id
        self._target_health_payload = await self.wait_health(
            self.target_tag,
            transaction_id=transaction_id,
            timeout=600,
        )
        async with await self.session() as session:
            deadline = time.monotonic() + 300
            while True:
                final = await self.api(
                    session,
                    "GET",
                    f"/api/updates/transactions/{transaction_id}",
                )
                if final.get("status") in TERMINAL_TRANSACTION_STATES:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"update transaction did not finish: {transaction_id}")
                await asyncio.sleep(0.25)
        if (
            final.get("status") != "completed"
            or final.get("action") != "update"
            or final.get("target_tag") != self.target_tag
        ):
            raise RuntimeError(f"update transaction failed: {final}")
        self.report.transactions.append(
            {
                key: final.get(key)
                for key in (
                    "transaction_id",
                    "status",
                    "stage",
                    "current_version",
                    "target_version",
                    "target_tag",
                    "action",
                )
            }
        )
        return final

    async def assert_managed_source_matches_target(self) -> None:
        installed = self._require_install_root()
        reference = self.target_reference_root
        if reference is None:
            raise RuntimeError("target reference archive was not verified")
        manifest_path = reference / MANIFEST_NAME
        target_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        declared = {
            str(item["path"]): item
            for item in target_manifest.get("files") or []
        }
        actual_paths: set[str] = set()
        for relative in MANAGED_DIRECTORIES:
            directory = installed / Path(relative)
            if not directory.is_dir():
                raise RuntimeError(f"managed source directory is missing after update: {relative}")
            for path in directory.rglob("*"):
                if not path.is_file():
                    continue
                child = path.relative_to(installed).as_posix()
                parts = Path(child).parts
                if "__pycache__" in parts or path.suffix.lower() == ".pyc":
                    continue
                actual_paths.add(child)
        for relative in MANAGED_FILES:
            path = installed / Path(relative)
            if not path.is_file():
                raise RuntimeError(f"managed source file is missing after update: {relative}")
            actual_paths.add(relative)
        expected_paths = set(declared)
        if actual_paths != expected_paths:
            raise RuntimeError(
                "managed source path set differs from v0.2.4 manifest: "
                f"extra={sorted(actual_paths - expected_paths)[:5]} "
                f"missing={sorted(expected_paths - actual_paths)[:5]}"
            )
        mismatches = []
        for relative, metadata in declared.items():
            path = installed / Path(relative)
            if (
                path.stat().st_size != int(metadata["size"])
                or _sha256(path) != str(metadata["sha256"]).lower()
            ):
                mismatches.append(relative)
        if mismatches:
            raise RuntimeError(f"managed source content differs from v0.2.4: {mismatches[:10]}")
        installed_manifest = installed / MANIFEST_NAME
        if not installed_manifest.is_file() or _sha256(installed_manifest) != _sha256(manifest_path):
            raise RuntimeError("installed update-manifest.json differs from official v0.2.4")
        self.report.runtime_checks.append("installed managed file list, sizes and SHA-256 match official v0.2.4")
        self.report.protected_state["managed_source"] = {
            "manifest_files": len(declared),
            "verified_files": len(actual_paths),
            "manifest_sha256": _sha256(installed_manifest),
            "ignored_runtime_bytecode_files": sum(
                1
                for relative in MANAGED_DIRECTORIES
                for path in (installed / Path(relative)).rglob("*.pyc")
                if path.is_file()
            ),
        }

    async def assert_source_transport_state(self, bot_ids: dict[str, str]) -> None:
        root = self._require_install_root()
        transport_path = root / "config" / "onebot_transports.json"
        payload = json.loads(transport_path.read_text(encoding="utf-8"))
        if payload.get("format_version") != 1:
            raise RuntimeError("upgrade did not persist supported OneBot transport format")
        transports = payload.get("transports") or {}
        if self.source_tag == "v0.2.2":
            expected = {
                "Compat Active": (self.fake_onebot_initial.ws_url, "initial-token", True, True),
                "Compat Empty Token": ("ws://127.0.0.1:6199/ws/", "", False, False),
                "Compat Custom Token": ("wss://onebot.invalid/ws", "custom-token", True, True),
            }
            for name, bot_id in bot_ids.items():
                transport = transports.get(bot_id) or {}
                settings = transport.get("settings") or {}
                url, token, report_self, debug = expected[name]
                checks = {
                    "type": transport.get("type") == "websocket-client",
                    "url": settings.get("url") == url,
                    "token": settings.get("access_token") == token,
                    "report_self": settings.get("report_self_message") is report_self,
                    "debug": settings.get("debug") is debug,
                    "format": settings.get("message_post_format") == "array",
                    "reconnect": settings.get("reconnect_interval_ms") == 5000,
                    "heartbeat": settings.get("heartbeat_interval_ms") == 30000,
                }
                if not all(checks.values()):
                    raise RuntimeError(f"v0.2.2 transport migration mismatch for {name}: {checks}")
            self.report.runtime_checks.append("v0.2.2 legacy Bot settings migrated to transport registry")
            return

        active_id = bot_ids["Compat Active"]
        transport = transports.get(active_id) or {}
        settings = transport.get("settings") or {}
        checks = {
            "type": transport.get("type") == "websocket-client",
            "url": settings.get("url") == self.fake_onebot_initial.ws_url,
            "token": settings.get("access_token") == "initial-token",
            "report_self": settings.get("report_self_message") is False,
            "debug": settings.get("debug") is True,
            "format": settings.get("message_post_format") == "string",
            "reconnect": settings.get("reconnect_interval_ms") == 1700,
            "heartbeat": settings.get("heartbeat_interval_ms") == 9000,
        }
        if not all(checks.values()):
            raise RuntimeError(f"v0.2.3 transport settings changed during update: {checks}")
        self.report.runtime_checks.append("v0.2.3 WebSocket transport settings preserved")

    async def assert_target_runtime_and_persistence(self, bot_ids: dict[str, str]) -> None:
        active_bot_id = bot_ids["Compat Active"]
        async with await self.session() as session:
            item = await self.bot_from_api(session, active_bot_id)
        checks = {
            "target_health": self._target_health_payload.get("version") == self.target_tag,
            "bot_enabled": item.get("enabled") is True,
            "websocket_client": item.get("onebot_transport_type") == "websocket-client",
            "thread_setting_default": item.get("forward_messages_to_thread") is False,
            "identity_mapping": int(item.get("onebot_self_id") or 0) == self._source_onebot_self_id,
        }
        if not all(checks.values()):
            raise RuntimeError(f"target bot/API persistence checks failed: {checks}")

        root = self._require_install_root()
        target_python = root / ".venv" / "Scripts" / "python.exe"
        requirement_checker = root / "tools" / "check_requirements.py"
        requirements_file = root / "requirements.txt"
        if not target_python.is_file():
            raise RuntimeError("updated installation did not retain its virtual environment")
        dependency_check = await asyncio.to_thread(
            subprocess.run,
            [
                str(target_python),
                str(requirement_checker),
                str(requirements_file),
            ],
            cwd=str(root),
            capture_output=True,
            text=True,
            check=False,
        )
        if dependency_check.returncode != 0:
            detail = (dependency_check.stdout + dependency_check.stderr).strip()[-2000:]
            raise RuntimeError(f"updated v0.2.4 dependency check failed: {detail}")
        direct_imports = await asyncio.to_thread(
            subprocess.run,
            [
                str(target_python),
                "-c",
                "import aiohttp, cryptography, fastapi, orjson, psutil, multipart, winpty, uvicorn, websockets",
            ],
            cwd=str(root),
            capture_output=True,
            text=True,
            check=False,
        )
        if direct_imports.returncode != 0:
            detail = (direct_imports.stdout + direct_imports.stderr).strip()[-2000:]
            raise RuntimeError(f"updated v0.2.4 runtime dependency import failed: {detail}")
        self.report.runtime_checks.append("updated virtual environment satisfies requirements and imports runtime dependencies")
        registry = json.loads((root / "config" / "bots.json").read_text(encoding="utf-8"))
        active_config = next(
            (
                candidate
                for candidate in registry.get("bots") or []
                if str(candidate.get("id") or candidate.get("bot_id") or "") == active_bot_id
            ),
            None,
        )
        if active_config is None or active_config.get("forward_messages_to_thread") is not False:
            raise RuntimeError("v0.2.4 did not persist the missing thread setting as false")
        await self.assert_source_transport_state(bot_ids)

        identity_scope_path = root / "data" / "bots" / active_bot_id / "identity_scope.json"
        identity_scope = json.loads(identity_scope_path.read_text(encoding="utf-8"))
        database_path = Path(str(identity_scope.get("database_path") or "")).resolve()
        identity_root = (root / "data" / "user_identity").resolve()
        try:
            database_path.relative_to(identity_root)
        except ValueError as exc:
            raise RuntimeError("persisted identity database path escaped the isolated data root") from exc
        if not database_path.is_file():
            raise RuntimeError("persistent identity database disappeared during update")
        with sqlite3.connect(database_path) as connection:
            integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        if integrity.lower() != "ok":
            raise RuntimeError(f"persistent identity database integrity check failed: {integrity}")
        if int(identity_scope.get("onebot_self_id") or 0) != self._source_onebot_self_id:
            raise RuntimeError("identity scope mapping changed during update")
        self.report.protected_state["runtime_identity_database_after"] = {
            "relative_path": database_path.relative_to(root).as_posix(),
            "size": database_path.stat().st_size,
            "integrity_check": integrity,
        }

        await self.fake_rc.wait_clients(1, timeout=45)
        await self.fake_onebot_initial.wait_clients(1, timeout=45)
        login = await self.fake_onebot_initial.send_action("get_login_info", {})
        if login.get("status") != "ok" or int(login.get("retcode") or 0) != 0:
            raise RuntimeError(f"v0.2.4 OneBot action did not survive update: {login}")
        if login.get("data", {}).get("user_id") != self._source_onebot_self_id:
            raise RuntimeError("post-update OneBot identity differs from pre-update identity")
        self.report.runtime_checks.append("v0.2.4 OneBot action and identity mapping are live")

        if not self._pre_update_message_id:
            raise RuntimeError("pre-update OneBot message id was not recorded")
        message = await self.fake_onebot_initial.send_action(
            "get_msg",
            {"message_id": int(self._pre_update_message_id)},
        )
        if (
            message.get("status") != "ok"
            or int(message.get("retcode") or 0) != 0
            or str((message.get("data") or {}).get("message_id") or "")
            != self._pre_update_message_id
        ):
            raise RuntimeError(f"pre-update message mapping was not readable after update: {message}")
        self.report.runtime_checks.append("pre-update message mapping remains readable through get_msg")

        await self._send_test_event("after-update")
        self.report.runtime_checks.append("post-update Rocket.Chat event reaches the OneBot client")

    async def assert_protected_state(self) -> None:
        root = self._require_install_root()
        before = self.report.protected_state["before"]
        after = {
            relative: _sha256(path)
            for relative, path in self._protected_paths.items()
        }
        self.report.protected_state["after"] = after
        self.report.protected_state["unchanged"] = before == after
        if before != after:
            changed = sorted(
                relative
                for relative in set(before) | set(after)
                if before.get(relative) != after.get(relative)
            )
            raise RuntimeError(f"persistent sentinel data changed during update: {changed}")
        identity_sentinel = root / "data" / "user_identity" / "compat-preserved.sqlite3"
        with sqlite3.connect(identity_sentinel) as connection:
            row = connection.execute(
                "SELECT payload FROM compat_state WHERE marker = ?",
                ("upgrade-validation",),
            ).fetchone()
        if row != ("identity-database-sentinel-v024",):
            raise RuntimeError("persistent identity database sentinel row was damaged")
        self.report.runtime_checks.append("configuration, identity, bot runtime and user plugin sentinels preserved")

    async def run(self) -> None:
        await self.prepare()
        await self.start_fakes()
        await self.start_source()
        bot_ids = await self.create_source_bots()
        await self.switch_target()
        await self.assert_managed_source_matches_target()
        await self.assert_target_runtime_and_persistence(bot_ids)
        await self.assert_protected_state()

    async def stop(self) -> None:
        await self._stop_isolated_service()
        await self.fake_onebot_initial.stop()
        await self.fake_rc.stop()
        if self.launch_log_handle is not None:
            self.launch_log_handle.close()
            self.launch_log_handle = None
        if self.install_root is not None:
            await asyncio.sleep(0.5)
            lingering = await asyncio.to_thread(self._find_isolated_processes)
            self.report.protected_state["isolated_processes_stopped"] = not lingering
            if lingering:
                self.report.protected_state["lingering_processes"] = lingering

    async def _stop_isolated_service(self) -> None:
        root = self.install_root
        if root is None:
            return
        candidates: set[int] = set()
        transaction_root = root / "data" / "update" / "transactions"
        if transaction_root.is_dir():
            for path in transaction_root.glob("*/transaction.json"):
                with contextlib.suppress(OSError, ValueError, json.JSONDecodeError):
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    for key in ("target_pid", "rollback_pid", "helper_pid"):
                        value = int(payload.get(key) or 0)
                        if value > 0:
                            candidates.add(value)
        if self.launch_process is not None and self.launch_process.returncode is None:
            candidates.add(self.launch_process.pid)

        processes: dict[int, psutil.Process] = {}
        for pid in candidates:
            with contextlib.suppress(psutil.Error):
                process = psutil.Process(pid)
                processes[pid] = process
                for child in process.children(recursive=True):
                    processes[child.pid] = child
        ordered = sorted(
            processes.values(),
            key=lambda item: item.pid in candidates,
        )
        for process in ordered:
            with contextlib.suppress(psutil.Error):
                process.terminate()
        if processes:
            _, alive = await asyncio.to_thread(
                psutil.wait_procs,
                ordered,
                timeout=10,
            )
            for process in alive:
                with contextlib.suppress(psutil.Error):
                    process.kill()
            if alive:
                await asyncio.to_thread(psutil.wait_procs, alive, timeout=5)

    def _find_isolated_processes(self) -> list[dict[str, Any]]:
        root = self._require_install_root().resolve()
        lingering: list[dict[str, Any]] = []
        for process in psutil.process_iter(["pid", "name", "cwd", "cmdline"]):
            with contextlib.suppress(psutil.Error, OSError):
                cwd = Path(str(process.info.get("cwd") or "")).resolve()
                if cwd == root or root in cwd.parents:
                    lingering.append(
                        {
                            "pid": process.info.get("pid"),
                            "name": process.info.get("name"),
                        }
                    )
        return lingering

    def _require_install_root(self) -> Path:
        if self.install_root is None:
            raise RuntimeError("isolated installation has not been prepared")
        return self.install_root

    def write_report(self) -> None:
        self.report.completed_at = datetime.now().astimezone().isoformat()
        _atomic_json(self.root / "compatibility-report.json", self.report.public_mapping())
        lines = [
            f"# RocketCatShell {self.source_tag} → {self.target_tag} update validation",
            "",
            f"- Result: {'PASS' if self.report.passed else 'FAIL'}",
            f"- Started: {self.report.started_at}",
            f"- Completed: {self.report.completed_at}",
            f"- Source asset SHA-256: `{self.report.source_release.get('sha256', '-')}`",
            f"- Target asset SHA-256: `{self.report.target_release.get('sha256', '-')}`",
            f"- Transactions: {len(self.report.transactions)}",
            f"- Protected state unchanged: {self.report.protected_state.get('unchanged', False)}",
            f"- Managed files verified: `{(self.report.protected_state.get('managed_source') or {}).get('verified_files', 0)}`",
            "",
            "## Transactions",
            "",
        ]
        for item in self.report.transactions:
            lines.append(
                f"- `{item.get('action')}` → `{item.get('target_version')}`: "
                f"`{item.get('status')}` (`{item.get('transaction_id')}`)"
            )
        lines.extend(["", "## Runtime checks", ""])
        lines.extend(f"- {item}" for item in self.report.runtime_checks)
        if self.report.error:
            lines.extend(["", "## Error", "", f"`{self.report.error}`"])
        (self.root / "compatibility-report.md").write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate an official old Windows release updating to v0.2.4"
    )
    parser.add_argument(
        "--test-root",
        default=str(PROJECT_ROOT.parents[1] / "test"),
    )
    parser.add_argument(
        "--source-tag",
        required=True,
        choices=tuple(OFFICIAL_OLD_RELEASES),
        help="official installed release to upgrade from",
    )
    parser.add_argument(
        "--target-tag",
        default="v0.2.4",
        choices=("v0.2.4",),
        help="release to validate against (fixed for this compatibility matrix)",
    )
    parser.add_argument("--password", default="rocketcat-compat-test-only")
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    source_short = str(args.source_tag).replace(".", "").replace("v", "")
    root = Path(args.test_root).resolve() / f"v024-update-v{source_short}-{timestamp}"
    harness = CompatibilityHarness(
        root=root,
        source_tag=str(args.source_tag),
        target_tag=str(args.target_tag),
        password=str(args.password),
    )
    exit_code = 1
    try:
        await harness.run()
        harness.report.passed = True
        exit_code = 0
    except Exception as exc:
        harness.report.error = repr(exc)
        raise
    finally:
        stop_error = ""
        try:
            await harness.stop()
        except Exception as exc:
            stop_error = repr(exc)
        if stop_error:
            harness.report.error = harness.report.error or f"isolated service shutdown failed: {stop_error}"
            harness.report.passed = False
            exit_code = 1
        if harness.install_root is not None and harness.report.protected_state.get(
            "isolated_processes_stopped"
        ) is not True:
            harness.report.passed = False
            harness.report.error = harness.report.error or "isolated RocketCatShell process remained after shutdown"
            exit_code = 1
        if root.exists():
            harness.write_report()
        print(json.dumps(harness.report.public_mapping(), ensure_ascii=False, indent=2))
    return exit_code


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
