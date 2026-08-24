from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from rocketcat_shell.update_manifest import inspect_and_extract_zip  # noqa: E402
from tools.stress_v022_full_stack import FakeOneBot, FakeRocketChat  # noqa: E402


OFFICIAL_V022_URL = (
    "https://github.com/Creeper3222/RocketCat/releases/download/"
    "v0.2.2/RocketCatShell-v0.2.2.zip"
)
OFFICIAL_V022_SHA256 = (
    "879288ea53f7a48331589c8772a2e590bff89373eb2bf203d4d4860620cf87de"
)
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


def _download(url: str, destination: Path) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "RocketCatShell/v0.2.3-compat-validation"},
    )
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=90) as response, destination.open(
        "xb"
    ) as output:
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            output.write(chunk)
    return digest.hexdigest()


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

    def _observe_message(self, self_id: int, payload: dict[str, Any]) -> None:
        self.message_events += 1
        super()._observe_message(self_id, payload)


@dataclass
class ValidationReport:
    started_at: str
    root: str
    official_v022: dict[str, Any] = field(default_factory=dict)
    candidate_v023: dict[str, Any] = field(default_factory=dict)
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
            "official_v022": self.official_v022,
            "candidate_v023": self.candidate_v023,
            "bots": self.bots,
            "transactions": self.transactions,
            "protected_state": self.protected_state,
            "runtime_checks": self.runtime_checks,
            "passed": self.passed,
            "error": self.error,
        }


class CompatibilityHarness:
    def __init__(self, *, root: Path, password: str) -> None:
        self.root = root
        self.password = password
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.archive = root / "RocketCatShell-v0.2.2.zip"
        self.extract_root = root / "extracted"
        self.install_root: Path | None = None
        self.launch_process: asyncio.subprocess.Process | None = None
        self.launch_log_handle: Any = None
        self.fake_rc = FakeRocketChat()
        self.fake_onebot_initial = HeaderObservingOneBot()
        self.fake_onebot_edited = HeaderObservingOneBot()
        self.report = ValidationReport(
            started_at=datetime.now().astimezone().isoformat(),
            root=str(root),
        )
        self._last_transaction_id = ""

    async def prepare(self, *, v022_url: str, expected_sha256: str) -> None:
        self.root.mkdir(parents=True, exist_ok=False)
        actual = await asyncio.to_thread(_download, v022_url, self.archive)
        if actual.lower() != expected_sha256.lower():
            raise RuntimeError(
                f"official v0.2.2 SHA-256 mismatch: {actual} != {expected_sha256}"
            )
        candidate_root, manifest = await asyncio.to_thread(
            inspect_and_extract_zip,
            self.archive,
            self.extract_root,
            expected_tag="v0.2.2",
        )
        self.install_root = candidate_root
        self.report.official_v022 = {
            "url": v022_url,
            "sha256": actual,
            "size": self.archive.stat().st_size,
            "manifest_version": manifest.get("version"),
            "manifest_files": len(manifest.get("files") or []),
        }
        shell_settings = {
            "webui_host": "127.0.0.1",
            "webui_port": self.port,
            "webui_access_password": self.password,
            "auto_open_browser": False,
        }
        _atomic_json(candidate_root / "config" / "shell.json", shell_settings)
        await self._write_protected_sentinels()

    async def _write_protected_sentinels(self) -> None:
        root = self._require_install_root()
        sentinels = {
            "config/compat-preserved.bin": b"config-v023-compat-sentinel\0",
            "data/bots/compat-preserved.bin": b"bot-data-v023-compat-sentinel\0",
            "data/plugins/user_compat_plugin/compat-preserved.bin": b"user-plugin-sentinel\0",
            "data/plugin_data/compat-preserved.bin": b"plugin-data-sentinel\0",
            "data/user_identity/compat-preserved.bin": b"identity-sentinel\0",
        }
        for relative, payload in sentinels.items():
            path = root / Path(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        self.report.protected_state["before"] = {
            relative: _sha256(root / Path(relative)) for relative in sentinels
        }

    async def start_fakes(self) -> None:
        await self.fake_rc.start()
        await self.fake_onebot_initial.start()
        await self.fake_onebot_edited.start()

    async def start_v022(self) -> None:
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
        await self.wait_health("v0.2.2", timeout=600)

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
        root = self._require_install_root()
        if (root / "config" / "onebot_transports.json").exists():
            raise RuntimeError("legacy install created a v0.2.3 transport file")
        self.report.bots["ids"] = identifiers
        self.report.bots["legacy_before_update"] = self._redacted_legacy_bots()
        return identifiers

    async def _send_test_event(self, marker: str) -> None:
        target = (
            self.fake_onebot_edited
            if marker == "after-v022-edit"
            else self.fake_onebot_initial
        )
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

    async def switch(self, target: str, expected_action: str) -> dict[str, Any]:
        async with await self.session() as session:
            releases = await self.api(session, "GET", "/api/updates/releases")
            candidate = next(
                (item for item in releases.get("releases") or [] if item.get("tag_name") == target),
                None,
            )
            if candidate is None:
                raise RuntimeError(f"update candidate is unavailable: {target}")
            if candidate.get("action") != expected_action:
                raise RuntimeError(
                    f"unexpected update action for {target}: {candidate.get('action')}"
                )
            if target == "v0.2.3":
                self.report.candidate_v023 = {
                    "prerelease": bool(candidate.get("prerelease")),
                    "asset": candidate.get("asset"),
                }
            transaction = await self.api(
                session,
                "POST",
                "/api/updates/switch",
                payload={"tag_name": target},
            )
        transaction_id = str(transaction.get("transaction_id") or "")
        if not transaction_id:
            raise RuntimeError("update transaction did not return an id")
        self._last_transaction_id = transaction_id
        await self.wait_health(target, transaction_id=transaction_id)
        async with await self.session() as session:
            deadline = time.monotonic() + 180
            while True:
                final = await self.api(
                    session,
                    "GET",
                    f"/api/updates/transactions/{transaction_id}",
                )
                if final.get("status") in TERMINAL_TRANSACTION_STATES:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"update transaction did not finish: {transaction_id}"
                    )
                await asyncio.sleep(0.25)
        if final.get("status") != "completed" or final.get("action") != expected_action:
            raise RuntimeError(f"update transaction failed: {final}")
        public = {
            key: final.get(key)
            for key in (
                "transaction_id",
                "status",
                "stage",
                "current_version",
                "target_version",
                "action",
            )
        }
        self.report.transactions.append(public)
        return final

    async def assert_v023_migration(self, bot_ids: dict[str, str]) -> None:
        root = self._require_install_root()
        transport_path = root / "config" / "onebot_transports.json"
        payload = json.loads(transport_path.read_text(encoding="utf-8"))
        if payload.get("format_version") != 1:
            raise RuntimeError("unexpected transport format version")
        expected = {
            "Compat Active": (self.fake_onebot_initial.ws_url, True, True, True),
            "Compat Empty Token": ("ws://127.0.0.1:6199/ws/", False, False, False),
            "Compat Custom Token": ("wss://onebot.invalid/ws", True, True, True),
        }
        for name, bot_id in bot_ids.items():
            transport = payload["transports"][bot_id]
            if transport.get("type") != "websocket-client":
                raise RuntimeError(f"{name} did not migrate to websocket-client")
            settings = transport["settings"]
            url, has_token, report_self, debug = expected[name]
            checks = {
                "url": settings.get("url") == url,
                "token": bool(settings.get("access_token")) == has_token,
                "report_self": bool(settings.get("report_self_message")) == report_self,
                "debug": bool(settings.get("debug")) == debug,
                "format": settings.get("message_post_format") == "array",
                "reconnect": settings.get("reconnect_interval_ms") == 5000,
                "heartbeat": settings.get("heartbeat_interval_ms") == 30000,
            }
            if not all(checks.values()):
                raise RuntimeError(f"{name} migration mismatch: {checks}")
        await self.fake_rc.wait_clients(1, timeout=30)
        await self.fake_onebot_initial.wait_clients(1, timeout=30)
        await self.fake_onebot_initial.send_actions("get_login_info")
        if self.fake_onebot_initial.action_failures:
            raise RuntimeError("v0.2.3 migrated action round-trip failed")
        await self._send_test_event("after-update")
        self.report.runtime_checks.append("v0.2.3 migrated event/action round-trip")

    async def set_v023_only_settings(self, active_bot_id: str) -> str:
        async with await self.session() as session:
            current = await self.api(session, "GET", f"/api/bots/{active_bot_id}")
            item = current["item"]
            settings = dict(item["onebot_transport"]["settings"])
            settings.update(
                message_post_format="string",
                reconnect_interval_ms=1700,
                heartbeat_interval_ms=9000,
            )
            await self.api(
                session,
                "PUT",
                f"/api/bots/{active_bot_id}",
                payload={
                    "onebot_transport": {
                        "type": "websocket-client",
                        "settings": settings,
                    }
                },
            )
        transport_path = self._require_install_root() / "config" / "onebot_transports.json"
        return _sha256(transport_path)

    async def assert_v022_rollback(
        self,
        active_bot_id: str,
        transport_hash: str,
    ) -> None:
        root = self._require_install_root()
        transport_path = root / "config" / "onebot_transports.json"
        if _sha256(transport_path) != transport_hash:
            raise RuntimeError("v0.2.2 changed onebot_transports.json during rollback")
        async with await self.session() as session:
            current = await self.api(session, "GET", "/api/bots")
        item = next(
            (
                candidate
                for candidate in current.get("items") or []
                if str(candidate.get("id") or "") == active_bot_id
            ),
            None,
        )
        if item is None:
            raise RuntimeError("v0.2.2 bot list did not contain the active client")
        if item.get("onebot_ws_url") != self.fake_onebot_initial.ws_url:
            raise RuntimeError("v0.2.2 did not read the projected WebSocket URL")
        if item.get("onebot_access_token") != "initial-token":
            raise RuntimeError("v0.2.2 did not read the projected token")
        if bool(item.get("skip_own_messages")):
            raise RuntimeError("v0.2.2 did not read the projected self-message setting")
        if not bool(item.get("debug")):
            raise RuntimeError("v0.2.2 did not read the projected debug setting")
        await self.fake_onebot_initial.wait_clients(1, timeout=30)
        self.report.runtime_checks.append("v0.2.2 rollback projection and runtime")

    async def edit_in_v022(self, active_bot_id: str) -> None:
        async with await self.session() as session:
            await self.api(
                session,
                "PUT",
                f"/api/bots/{active_bot_id}",
                payload={
                    "onebot_ws_url": self.fake_onebot_edited.ws_url,
                    "onebot_access_token": "edited-token",
                    "skip_own_messages": True,
                    "debug": False,
                },
            )
        await self.fake_onebot_edited.wait_clients(1, timeout=30)
        if not any(
            item["authorization"] == "Bearer edited-token"
            for item in self.fake_onebot_edited.headers
        ):
            raise RuntimeError("edited v0.2.2 token was not used by the runtime")
        await self.fake_onebot_edited.send_actions("get_login_info")
        if self.fake_onebot_edited.action_failures:
            raise RuntimeError("edited v0.2.2 action round-trip failed")
        await self._send_test_event("after-v022-edit")
        self.report.runtime_checks.append("v0.2.2 edited event/action round-trip")

    async def assert_reupgrade(self, active_bot_id: str) -> None:
        async with await self.session() as session:
            current = await self.api(session, "GET", f"/api/bots/{active_bot_id}")
        settings = current["item"]["onebot_transport"]["settings"]
        checks = {
            "url": settings.get("url") == self.fake_onebot_edited.ws_url,
            "token": settings.get("access_token") == "edited-token",
            "report_self": settings.get("report_self_message") is False,
            "debug": settings.get("debug") is False,
            "format": settings.get("message_post_format") == "string",
            "reconnect": settings.get("reconnect_interval_ms") == 1700,
            "heartbeat": settings.get("heartbeat_interval_ms") == 9000,
        }
        if not all(checks.values()):
            raise RuntimeError(f"re-upgrade compatibility mismatch: {checks}")
        await self.fake_onebot_edited.wait_clients(1, timeout=30)
        self.report.runtime_checks.append("v0.2.3 re-upgrade merged old and new settings")

    async def assert_protected_state(self) -> None:
        root = self._require_install_root()
        before = self.report.protected_state["before"]
        after = {relative: _sha256(root / Path(relative)) for relative in before}
        self.report.protected_state["after"] = after
        self.report.protected_state["unchanged"] = before == after
        if before != after:
            raise RuntimeError("protected sentinel state changed during update transactions")

    async def run(self, *, v022_url: str, expected_sha256: str) -> None:
        await self.prepare(v022_url=v022_url, expected_sha256=expected_sha256)
        await self.start_fakes()
        await self.start_v022()
        bot_ids = await self.create_legacy_bots()
        active_bot_id = bot_ids["Compat Active"]

        await self.switch("v0.2.3", "update")
        await self.assert_v023_migration(bot_ids)
        transport_hash = await self.set_v023_only_settings(active_bot_id)

        await self.switch("v0.2.2", "rollback")
        await self.assert_v022_rollback(active_bot_id, transport_hash)
        await self.edit_in_v022(active_bot_id)

        await self.switch("v0.2.3", "update")
        await self.assert_reupgrade(active_bot_id)
        await self.assert_protected_state()

    async def stop(self) -> None:
        await self._stop_isolated_service()
        await self.fake_onebot_edited.stop()
        await self.fake_onebot_initial.stop()
        await self.fake_rc.stop()
        if self.launch_log_handle is not None:
            self.launch_log_handle.close()
            self.launch_log_handle = None

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

    def _require_install_root(self) -> Path:
        if self.install_root is None:
            raise RuntimeError("isolated installation has not been prepared")
        return self.install_root

    def write_report(self) -> None:
        self.report.completed_at = datetime.now().astimezone().isoformat()
        _atomic_json(self.root / "compatibility-report.json", self.report.public_mapping())
        lines = [
            "# RocketCatShell v0.2.3 WebSocket compatibility report",
            "",
            f"- Result: {'PASS' if self.report.passed else 'FAIL'}",
            f"- Started: {self.report.started_at}",
            f"- Completed: {self.report.completed_at}",
            f"- Official v0.2.2 SHA-256: `{self.report.official_v022.get('sha256', '-')}`",
            f"- Transactions: {len(self.report.transactions)}",
            f"- Protected state unchanged: {self.report.protected_state.get('unchanged', False)}",
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
        description="Validate v0.2.2 -> v0.2.3 -> v0.2.2 -> v0.2.3 compatibility"
    )
    parser.add_argument(
        "--test-root",
        default=str(PROJECT_ROOT.parents[1] / "test"),
    )
    parser.add_argument("--v022-url", default=OFFICIAL_V022_URL)
    parser.add_argument("--v022-sha256", default=OFFICIAL_V022_SHA256)
    parser.add_argument("--password", default="rocketcat-compat-test-only")
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    root = Path(args.test_root).resolve() / f"v023-compat-{timestamp}"
    harness = CompatibilityHarness(root=root, password=str(args.password))
    exit_code = 1
    try:
        await harness.run(
            v022_url=str(args.v022_url),
            expected_sha256=str(args.v022_sha256),
        )
        harness.report.passed = True
        exit_code = 0
    except Exception as exc:
        harness.report.error = repr(exc)
        raise
    finally:
        with contextlib.suppress(Exception):
            await harness.stop()
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
