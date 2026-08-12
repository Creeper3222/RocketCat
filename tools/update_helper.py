from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from ctypes import wintypes
from pathlib import Path, PurePosixPath
from typing import Any


PRODUCT_NAME = "RocketCatShell"
MANAGED_DIRECTORIES = (
    "rocketcat_shell",
    "data/plugins/rocketcat_plugin_adapt_iamthinking",
    "data/plugins/rocketcat_plugin_built_in_command",
)
MANAGED_FILES = (
    "LICENSE",
    "README.md",
    "CHANGELOG.md",
    "launcher.bat",
    "requirements.txt",
    "tools/check_requirements.py",
    "tools/migrate_user_identity.py",
    "tools/update_helper.py",
)
MANIFEST_NAME = "update-manifest.json"
TRANSACTION_FILES = (*MANAGED_FILES, MANIFEST_NAME)
MANAGED_PATHS = (*MANAGED_DIRECTORIES, *TRANSACTION_FILES)
INCOMPLETE_STAGES = frozenset(
    {
        "prepared",
        "helper_started",
        "waiting_for_shutdown",
        "forcing_shutdown",
        "backing_up",
        "backup_complete",
        "replacing",
        "starting_target",
        "checking_target",
        "rolling_back",
    }
)
TERMINAL_STATUSES = frozenset(
    {"completed", "rolled_back", "failed"}
)
TRANSACTION_ID_PATTERN = re.compile(r"^[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 75.0
FORCED_SHUTDOWN_TIMEOUT_SECONDS = 15.0
HEALTH_TIMEOUT_SECONDS = 120.0
CURRENT_HEALTH_TIMEOUT_SECONDS = 5.0
PROCESS_TIME_TOLERANCE_SECONDS = 0.01
WINDOWS_EPOCH_OFFSET_SECONDS = 11_644_473_600
DEFAULT_WEBUI_PORT = 5751


def _read(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("transaction file is not an object")
    return payload


def _write(path: Path, payload: dict[str, Any], **changes: Any) -> dict[str, Any]:
    payload.update(changes, updated_at=time.time())
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return payload


def _prevalidate_transaction_file_path(transaction_file: Path) -> None:
    path = transaction_file.absolute()
    try:
        transaction_root = path.parent
        transactions_root = transaction_root.parent
        update_root = transactions_root.parent
        data_root = update_root.parent
        state_root = data_root.parent
    except IndexError as exc:
        raise RuntimeError("transaction file path is invalid") from exc
    if (
        path.name != "transaction.json"
        or not TRANSACTION_ID_PATTERN.fullmatch(transaction_root.name)
        or transactions_root.name != "transactions"
        or update_root.name != "update"
        or data_root.name != "data"
    ):
        raise RuntimeError("transaction file path is invalid")
    _assert_path_components_no_symlinks(state_root, path)


def _safe_transaction_context(
    transaction_file: Path,
    payload: dict[str, Any],
    *,
    require_candidate: bool = True,
) -> tuple[Path, Path, Path, Path]:
    supplied_transaction_file = transaction_file.absolute()
    transaction_id = str(payload.get("transaction_id") or "")
    if not TRANSACTION_ID_PATTERN.fullmatch(transaction_id):
        raise RuntimeError("transaction id is invalid")
    state_root = Path(str(payload.get("state_root") or "")).resolve(strict=True)
    source_root = Path(str(payload.get("source_root") or "")).resolve(strict=True)
    if source_root != state_root:
        raise RuntimeError("source and state roots must identify the same installation")
    expected_transaction_file = (
        state_root
        / "data"
        / "update"
        / "transactions"
        / transaction_id
        / "transaction.json"
    )
    if os.path.normcase(str(supplied_transaction_file)) != os.path.normcase(
        str(expected_transaction_file.absolute())
    ):
        raise RuntimeError("transaction file is outside the installation update directory")
    _assert_path_components_no_symlinks(state_root, expected_transaction_file)
    transaction_file = supplied_transaction_file.resolve(strict=True)
    transactions_root = (state_root / "data" / "update" / "transactions").resolve()
    transaction_root = transaction_file.parent.resolve()
    if (
        transaction_file.name != "transaction.json"
        or transaction_root.name != transaction_id
        or transaction_root.parent != transactions_root
    ):
        raise RuntimeError("transaction file is outside the installation update directory")
    expected_candidate_path = transaction_root / "candidate" / PRODUCT_NAME
    _assert_path_components_no_symlinks(transaction_root, expected_candidate_path)
    expected_candidate = expected_candidate_path.resolve(strict=require_candidate)
    candidate_root = Path(str(payload.get("candidate_root") or "")).resolve(
        strict=require_candidate
    )
    if candidate_root != expected_candidate:
        raise RuntimeError("candidate root does not match the transaction")
    backup_root = transaction_root / "backup"
    _assert_path_components_no_symlinks(transaction_root, backup_root)
    if require_candidate and (backup_root.exists() or backup_root.is_symlink()):
        raise RuntimeError("transaction backup already exists before apply")
    return source_root, candidate_root, backup_root, transaction_root


def _assert_path_components_no_symlinks(root: Path, path: Path) -> None:
    root = root.absolute()
    path = path.absolute()
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("update path escaped the installation root") from exc
    cursor = root
    if cursor.is_symlink():
        raise RuntimeError("symbolic links are forbidden in update paths")
    for component in relative.parts:
        cursor /= component
        if cursor.is_symlink():
            raise RuntimeError(
                f"symbolic links are forbidden in update paths: {component}"
            )


def _managed_path(root: Path, relative: str) -> Path:
    if relative not in MANAGED_PATHS:
        raise RuntimeError(f"path is outside the frozen update contract: {relative}")
    pure = PurePosixPath(relative)
    expected = root.joinpath(*pure.parts)
    if expected.parent == expected or root not in expected.parents:
        raise RuntimeError("managed path escaped the installation root")
    _assert_path_components_no_symlinks(root, expected)
    return expected


def _assert_no_symlinks(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeError(f"symbolic links are forbidden in update paths: {path.name}")
    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_symlink():
                raise RuntimeError(
                    f"symbolic links are forbidden in update paths: {child.name}"
                )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_candidate(candidate_root: Path, payload: dict[str, Any]) -> None:
    entries = payload.get("candidate_files")
    if not isinstance(entries, list) or not entries:
        raise RuntimeError("transaction is missing the candidate file contract")
    expected: dict[str, tuple[int, str]] = {}
    expected_folded: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError("candidate file contract is invalid")
        relative = str(entry.get("path") or "").replace("\\", "/")
        if not (
            relative in MANAGED_FILES
            or any(
                relative.startswith(directory + "/")
                for directory in MANAGED_DIRECTORIES
            )
        ):
            raise RuntimeError("candidate contains a path outside the update contract")
        folded = relative.casefold()
        if folded in expected_folded:
            raise RuntimeError("candidate contains colliding Windows paths")
        expected_folded.add(folded)
        raw_size = entry.get("size")
        if isinstance(raw_size, bool) or not isinstance(raw_size, int):
            raise RuntimeError("candidate file size is invalid")
        size = raw_size
        digest = str(entry.get("sha256") or "").lower()
        if size < 0 or not SHA256_PATTERN.fullmatch(digest):
            raise RuntimeError("candidate file digest is invalid")
        expected[relative] = (size, digest)

    actual: dict[str, Path] = {}
    _assert_no_symlinks(candidate_root)
    for path in candidate_root.rglob("*"):
        if path.is_file():
            relative = path.relative_to(candidate_root).as_posix()
            if relative == "update-manifest.json":
                continue
            actual[relative] = path
    if set(actual) != set(expected):
        raise RuntimeError("candidate file list changed after package validation")
    for relative, (size, digest) in expected.items():
        path = actual[relative]
        if path.stat().st_size != size or _sha256(path) != digest:
            raise RuntimeError(f"candidate file changed after validation: {relative}")
    for relative in MANAGED_PATHS:
        if not _managed_path(candidate_root, relative).exists():
            raise RuntimeError(f"candidate managed path is missing: {relative}")


def _validate_source_installation(source_root: Path) -> None:
    for relative in MANAGED_PATHS:
        path = _managed_path(source_root, relative)
        if not path.exists():
            continue
        expected_type_matches = (
            path.is_dir() if relative in MANAGED_DIRECTORIES else path.is_file()
        )
        if not expected_type_matches:
            raise RuntimeError(f"managed source path has the wrong type: {relative}")
        _assert_no_symlinks(path)
    for critical_file in ("launcher.bat", "tools/update_helper.py"):
        if not _managed_path(source_root, critical_file).is_file():
            raise RuntimeError(
                f"critical recovery file is missing: {critical_file}"
            )


def _validate_health_urls(values: object) -> list[str]:
    if not isinstance(values, list) or not values:
        raise RuntimeError("transaction health URL is unavailable")
    normalized: list[str] = []
    for value in values:
        try:
            parsed = urllib.parse.urlsplit(str(value or ""))
            port = parsed.port
        except ValueError as exc:
            raise RuntimeError("transaction health URL is invalid") from exc
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or port is None
            or not 1 <= port <= 65535
        ):
            raise RuntimeError("transaction health URL must be loopback HTTP")
        normalized.append(str(value).rstrip("/"))
    return list(dict.fromkeys(normalized))


def _validate_configured_health_port(source_root: Path, urls: list[str]) -> None:
    settings_path = source_root / "config" / "shell.json"
    configured_port = DEFAULT_WEBUI_PORT
    if settings_path.is_file():
        try:
            settings = _read(settings_path)
            raw_port = settings.get("webui_port", DEFAULT_WEBUI_PORT)
            if isinstance(raw_port, bool):
                raise ValueError
            configured_port = int(raw_port)
        except (OSError, ValueError, json.JSONDecodeError, RuntimeError) as exc:
            raise RuntimeError("current WebUI port configuration is invalid") from exc
    url_ports = {urllib.parse.urlsplit(url).port for url in urls}
    if configured_port not in url_ports:
        raise RuntimeError(
            "the configured WebUI port differs from the running health endpoint; "
            "restart RocketCatShell before switching versions"
        )


def _windows_process_create_time(pid: int) -> float | None:
    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return None
    try:
        created = wintypes.FILETIME()
        exited = wintypes.FILETIME()
        kernel = wintypes.FILETIME()
        user = wintypes.FILETIME()
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(created),
            ctypes.byref(exited),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return None
        ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
        return ticks / 10_000_000 - WINDOWS_EPOCH_OFFSET_SECONDS
    finally:
        kernel32.CloseHandle(handle)


def _process_create_time(pid: int) -> float | None:
    if pid <= 0:
        return None
    if os.name == "nt":
        return _windows_process_create_time(pid)
    try:
        import psutil

        return float(psutil.Process(pid).create_time())
    except (ImportError, OSError):
        try:
            os.kill(pid, 0)
        except OSError:
            return None
        return 0.0


def _pid_matches(pid: int, expected_create_time: float) -> bool:
    actual = _process_create_time(pid)
    if actual is None:
        return False
    if os.name != "nt" and expected_create_time == 0:
        return True
    return abs(actual - expected_create_time) <= PROCESS_TIME_TOLERANCE_SECONDS


def _wait_for_exact_pid(pid: int, created_at: float, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_matches(pid, created_at):
            return True
        time.sleep(0.25)
    return not _pid_matches(pid, created_at)


def _terminate_windows_exact_process(pid: int, created_at: float) -> bool:
    process_terminate = 0x0001
    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(
        process_terminate | process_query_limited_information,
        False,
        pid,
    )
    if not handle:
        return False
    try:
        created = wintypes.FILETIME()
        exited = wintypes.FILETIME()
        kernel = wintypes.FILETIME()
        user = wintypes.FILETIME()
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(created),
            ctypes.byref(exited),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return False
        ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
        actual = ticks / 10_000_000 - WINDOWS_EPOCH_OFFSET_SECONDS
        if abs(actual - created_at) > PROCESS_TIME_TOLERANCE_SECONDS:
            return False
        return bool(kernel32.TerminateProcess(handle, 1))
    finally:
        kernel32.CloseHandle(handle)


def _terminate_exact_pid_tree(pid: int, created_at: float) -> bool:
    if not _pid_matches(pid, created_at):
        return False
    if os.name == "nt":
        try:
            import psutil
        except ImportError:
            return False
        try:
            targets: list[tuple[int, float]] = []
            descendants = psutil.Process(pid).children(recursive=True)
            descendant_pids = {child.pid for child in descendants}
            protected = {os.getpid()}
            if os.getpid() in descendant_pids:
                protected.update(
                    child.pid for child in psutil.Process().children(recursive=True)
                )
            for child in reversed(descendants):
                if child.pid in protected:
                    continue
                try:
                    targets.append((child.pid, float(child.create_time())))
                except psutil.NoSuchProcess:
                    continue
                except psutil.AccessDenied:
                    return False
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            return False
        for child_pid, child_created_at in targets:
            _terminate_windows_exact_process(child_pid, child_created_at)
        _terminate_windows_exact_process(pid, created_at)
        all_targets = [*targets, (pid, created_at)]
        deadline = time.time() + FORCED_SHUTDOWN_TIMEOUT_SECONDS
        while time.time() < deadline:
            if not any(
                _pid_matches(target_pid, target_created_at)
                for target_pid, target_created_at in all_targets
            ):
                return True
            time.sleep(0.25)
        return not any(
            _pid_matches(target_pid, target_created_at)
            for target_pid, target_created_at in all_targets
        )
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    return True


def _stop_service_process(pid: int, created_at: float) -> tuple[bool, bool]:
    if _wait_for_exact_pid(pid, created_at, GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS):
        return True, False
    if not _pid_matches(pid, created_at):
        return True, False
    forced = _terminate_exact_pid_tree(pid, created_at)
    return forced, True


def _copy_path(source: Path, target: Path) -> None:
    _assert_no_symlinks(source)
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def _remove_managed(root: Path, relative: str) -> None:
    path = _managed_path(root, relative)
    if not path.exists() and not path.is_symlink():
        return
    _assert_no_symlinks(path)
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _backup(source_root: Path, backup_root: Path) -> None:
    if backup_root.exists():
        raise RuntimeError("transaction backup already exists")
    backup_root.mkdir(parents=True, exist_ok=False)
    presence: dict[str, bool] = {}
    for relative in MANAGED_PATHS:
        source = _managed_path(source_root, relative)
        presence[relative] = source.exists()
        if source.exists():
            _copy_path(source, _managed_path(backup_root, relative))
    presence_file = backup_root / "presence.json"
    temporary = presence_file.with_name(f"{presence_file.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(presence, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, presence_file)


def _install(source_root: Path, candidate_root: Path) -> None:
    for relative in MANAGED_DIRECTORIES:
        candidate = _managed_path(candidate_root, relative)
        if not candidate.exists():
            raise RuntimeError(f"candidate managed path is missing: {relative}")
        _remove_managed(source_root, relative)
        _copy_path(candidate, _managed_path(source_root, relative))
    for relative in TRANSACTION_FILES:
        candidate = _managed_path(candidate_root, relative)
        target = _managed_path(source_root, relative)
        if not candidate.is_file():
            raise RuntimeError(f"candidate managed path is missing: {relative}")
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(candidate, target)


def _restore(source_root: Path, backup_root: Path) -> None:
    presence_file = backup_root / "presence.json"
    if not presence_file.is_file():
        raise RuntimeError("transaction backup presence record is missing")
    presence = _read(presence_file)
    if set(presence) != set(MANAGED_PATHS) or not all(
        isinstance(value, bool) for value in presence.values()
    ):
        raise RuntimeError("transaction backup presence record is invalid")
    restore_sources: dict[str, Path] = {}
    for relative in MANAGED_PATHS:
        if presence.get(relative) is not True:
            continue
        backup = _managed_path(backup_root, relative)
        expected_type_matches = (
            backup.is_dir() if relative in MANAGED_DIRECTORIES else backup.is_file()
        )
        if not expected_type_matches:
            raise RuntimeError(f"transaction backup is incomplete: {relative}")
        _assert_no_symlinks(backup)
        restore_sources[relative] = backup

    restore_staging = backup_root.parent / "restore-staging"
    if restore_staging.is_symlink():
        raise RuntimeError("transaction restore staging cannot be a symbolic link")
    if restore_staging.exists():
        if not restore_staging.is_dir():
            raise RuntimeError("transaction restore staging has the wrong type")
        shutil.rmtree(restore_staging)
    restore_staging.mkdir(parents=True, exist_ok=False)
    for relative in TRANSACTION_FILES:
        if presence.get(relative) is not True:
            continue
        source = restore_sources[relative]
        staged = _managed_path(restore_staging, relative)
        staged.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, staged)
        if staged.stat().st_size != source.stat().st_size or _sha256(staged) != _sha256(
            source
        ):
            raise RuntimeError(f"transaction restore staging failed: {relative}")

    for relative in MANAGED_DIRECTORIES:
        _remove_managed(source_root, relative)
        if presence.get(relative) is True:
            _copy_path(
                restore_sources[relative],
                _managed_path(source_root, relative),
            )
    for relative in TRANSACTION_FILES:
        target = _managed_path(source_root, relative)
        if presence.get(relative) is not True:
            _remove_managed(source_root, relative)
            continue
        staged = _managed_path(restore_staging, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, target)
    shutil.rmtree(restore_staging)


def _service_command(root: Path) -> list[str]:
    launcher = _managed_path(root, "launcher.bat")
    if os.name != "nt" or not launcher.is_file():
        raise RuntimeError("RocketCatShell update transactions require launcher.bat on Windows")
    return ["cmd.exe", "/d", "/c", str(launcher), "--no-browser"]


def _start_service(root: Path, transaction_id: str) -> subprocess.Popen[Any]:
    env = {
        **os.environ,
        "ROCKETCATSHELL_UPDATE_TRANSACTION": transaction_id,
    }
    return subprocess.Popen(
        _service_command(root),
        cwd=str(root),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        creationflags=(
            getattr(subprocess, "CREATE_NO_WINDOW", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        ),
    )


def _terminate_started_process(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    created_at = _process_create_time(process.pid)
    if created_at is not None:
        _terminate_exact_pid_tree(process.pid, created_at)
    try:
        process.wait(timeout=FORCED_SHUTDOWN_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()


def _health_matches(
    urls: list[str],
    expected_version: str,
    transaction_id: str | None,
    timeout: float = HEALTH_TIMEOUT_SECONDS,
) -> bool:
    deadline = time.time() + timeout
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.time() < deadline:
        for url in urls:
            try:
                request = urllib.request.Request(
                    url.rstrip("/") + "/api/health",
                    headers={"Cache-Control": "no-cache"},
                )
                with opener.open(request, timeout=1) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if (
                    payload.get("status") == "ok"
                    and payload.get("product") == PRODUCT_NAME
                    and payload.get("version") == expected_version
                    and (
                        transaction_id is None
                        or payload.get("update_transaction") == transaction_id
                    )
                ):
                    return True
            except (OSError, ValueError, urllib.error.URLError):
                pass
        time.sleep(1)
    return False


def _current_health_matches(urls: list[str], expected_version: str) -> bool:
    return _health_matches(
        urls,
        expected_version,
        None,
        timeout=CURRENT_HEALTH_TIMEOUT_SECONDS,
    )


def apply_transaction(transaction_file: Path) -> int:
    transaction_file = transaction_file.absolute()
    try:
        _prevalidate_transaction_file_path(transaction_file)
    except Exception:
        return 2
    payload = _read(transaction_file)
    if payload.get("status") != "prepared" or payload.get("stage") != "prepared":
        return 2
    try:
        source_root, candidate_root, backup_root, _ = _safe_transaction_context(
            transaction_file,
            payload,
        )
        _validate_candidate(candidate_root, payload)
        _validate_source_installation(source_root)
        urls = _validate_health_urls(payload.get("health_urls"))
        _validate_configured_health_port(source_root, urls)
        if not _current_health_matches(urls, str(payload.get("current_version") or "")):
            raise RuntimeError("current service did not pass the loopback health check")
    except Exception as exc:
        try:
            _write(
                transaction_file,
                payload,
                status="failed",
                stage="preflight_failed",
                error=str(exc),
                completed_at=time.time(),
            )
        except Exception:
            pass
        return 2
    old_version = str(payload["current_version"])
    target_version = str(payload["target_version"])
    transaction_id = str(payload["transaction_id"])
    old_pid = int(payload["service_pid"])
    old_created_at = float(payload["service_create_time"])
    latest_payload = _read(transaction_file)
    if (
        latest_payload != payload
        or latest_payload.get("status") != "prepared"
        or latest_payload.get("stage") != "prepared"
    ):
        return 2
    payload = latest_payload
    _write(
        transaction_file,
        payload,
        status="running",
        stage="helper_started",
        helper_pid=os.getpid(),
    )
    _write(transaction_file, payload, stage="waiting_for_shutdown")
    stopped, forced_shutdown = _stop_service_process(old_pid, old_created_at)
    if forced_shutdown:
        _write(
            transaction_file,
            payload,
            stage="forcing_shutdown",
            forced_shutdown=True,
        )
    if not stopped:
        _write(
            transaction_file,
            payload,
            status="failed",
            stage="shutdown_failed",
            error="service did not stop safely",
            completed_at=time.time(),
        )
        return 3

    target_process: subprocess.Popen[Any] | None = None
    replacement_started = False
    try:
        _write(transaction_file, payload, stage="backing_up")
        _backup(source_root, backup_root)
        _write(transaction_file, payload, stage="backup_complete")
        _write(transaction_file, payload, stage="replacing")
        replacement_started = True
        _install(source_root, candidate_root)
        _write(transaction_file, payload, stage="starting_target")
        target_process = _start_service(source_root, transaction_id)
        _write(
            transaction_file,
            payload,
            stage="checking_target",
            target_pid=target_process.pid,
        )
        if not _health_matches(urls, target_version, transaction_id):
            raise RuntimeError("target service failed the transaction health check")
        _write(
            transaction_file,
            payload,
            status="completed",
            stage="completed",
            completed_at=time.time(),
        )
        return 0
    except Exception as exc:
        _write(
            transaction_file,
            payload,
            status="rolling_back",
            stage="rolling_back",
            error=str(exc),
        )
        if target_process is not None:
            _terminate_started_process(target_process)
        if not replacement_started:
            try:
                previous = _start_service(source_root, transaction_id)
                if not _health_matches(urls, old_version, transaction_id):
                    _write(
                        transaction_file,
                        payload,
                        status="recovery_required",
                        stage="previous_version_restart_failed",
                        rollback_pid=previous.pid,
                    )
                    return 5
                _write(
                    transaction_file,
                    payload,
                    status="failed",
                    stage="pre_replacement_failed",
                    rollback_pid=previous.pid,
                    completed_at=time.time(),
                )
                return 3
            except Exception as restart_exc:
                _write(
                    transaction_file,
                    payload,
                    status="recovery_required",
                    stage="previous_version_restart_failed",
                    rollback_error=str(restart_exc),
                )
                return 6
        try:
            _restore(source_root, backup_root)
            rollback = _start_service(source_root, transaction_id)
            if not _health_matches(urls, old_version, transaction_id):
                _write(
                    transaction_file,
                    payload,
                    status="recovery_required",
                    stage="rollback_health_failed",
                    rollback_pid=rollback.pid,
                )
                return 5
            _write(
                transaction_file,
                payload,
                status="rolled_back",
                stage="rolled_back",
                rollback_pid=rollback.pid,
                completed_at=time.time(),
            )
            return 4
        except Exception as rollback_exc:
            _write(
                transaction_file,
                payload,
                status="recovery_required",
                stage="rollback_failed",
                rollback_error=str(rollback_exc),
            )
            return 6


def recover_transactions(state_root: Path, active_transaction: str = "") -> int:
    state_root = state_root.resolve(strict=True)
    transactions_root = state_root / "data" / "update" / "transactions"
    if not transactions_root.is_dir():
        return 0
    transaction_files = sorted(transactions_root.glob("*/transaction.json"))
    if not transaction_files:
        return 0
    try:
        _assert_path_components_no_symlinks(state_root, transactions_root)
    except Exception:
        return 2
    failures = 0
    for transaction_file in transaction_files:
        try:
            if transaction_file.is_symlink():
                raise RuntimeError("transaction file cannot be a symbolic link")
            payload = _read(transaction_file)
            if payload.get("transaction_id") == active_transaction:
                continue
            if (
                payload.get("status") in TERMINAL_STATUSES
                or (
                    payload.get("status") != "recovery_required"
                    and payload.get("status")
                    not in {"running", "rolling_back", "prepared"}
                    and payload.get("stage") not in INCOMPLETE_STAGES
                )
            ):
                continue
            source_root, _, backup_root, _ = _safe_transaction_context(
                transaction_file,
                payload,
                require_candidate=False,
            )
            if not (backup_root / "presence.json").is_file():
                if payload.get("stage") in {
                    "prepared",
                    "helper_started",
                    "waiting_for_shutdown",
                    "forcing_shutdown",
                    "backing_up",
                }:
                    _write(
                        transaction_file,
                        payload,
                        status="failed",
                        stage="recovery_not_required",
                        error="update stopped before code replacement",
                        completed_at=time.time(),
                    )
                    continue
                raise RuntimeError("an interrupted update has no valid recovery backup")
            _restore(source_root, backup_root)
            _write(
                transaction_file,
                payload,
                status="rolled_back",
                stage="startup_recovered",
                completed_at=time.time(),
            )
        except Exception as exc:
            failures += 1
            try:
                payload = _read(transaction_file)
                _write(
                    transaction_file,
                    payload,
                    status="recovery_required",
                    stage="startup_recovery_failed",
                    error=str(exc),
                )
            except Exception:
                pass
    return 2 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="RocketCatShell Windows update helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    apply_parser = subparsers.add_parser("apply")
    apply_parser.add_argument("transaction_file")
    recover_parser = subparsers.add_parser("recover")
    recover_parser.add_argument("state_root")
    recover_parser.add_argument("--active-transaction", default="")
    args = parser.parse_args(argv)
    if args.command == "apply":
        return apply_transaction(Path(args.transaction_file))
    return recover_transactions(Path(args.state_root), args.active_transaction)


if __name__ == "__main__":
    raise SystemExit(main())
