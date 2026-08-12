from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from rocketcat_shell.update_manifest import MANAGED_DIRECTORIES, MANAGED_FILES, MANIFEST_NAME


ROOT = Path(__file__).resolve().parents[2]
HELPER_PATH = ROOT / "tools/update_helper.py"


def load_helper():
    spec = importlib.util.spec_from_file_location("rocketcat_update_helper_test", HELPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(HELPER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


helper = load_helper()


def write_managed_tree(root: Path, marker: str, *, omit: set[str] | None = None) -> None:
    omitted = omit or set()
    for relative in helper.MANAGED_DIRECTORIES:
        directory = root / Path(relative)
        directory.mkdir(parents=True, exist_ok=True)
        if relative not in omitted:
            (directory / "payload.txt").write_text(
                f"{relative}:{marker}\n",
                encoding="utf-8",
            )
    for relative in helper.TRANSACTION_FILES:
        if relative in omitted:
            continue
        path = root / Path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{relative}:{marker}\n", encoding="utf-8")


def candidate_entries(root: Path) -> list[dict[str, object]]:
    entries = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.relative_to(root).as_posix() == "update-manifest.json":
            continue
        content = path.read_bytes()
        entries.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    return entries


class UpdateHelperPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="rocketcat-helper-test-")
        self.root = Path(self.temporary.name)
        self.source = self.root / "install"
        self.source.mkdir()
        write_managed_tree(self.source, "old", omit={"LICENSE"})
        self.candidate = self.root / "candidate"
        self.candidate.mkdir()
        write_managed_tree(self.candidate, "new")
        self.protected = {
            "config/settings.json": b"config",
            "logs/rocketcat.log": b"logs",
            "data/bots/bot.json": b"bot",
            "data/plugins/user_plugin/main.py": b"user plugin",
            "data/plugin_data/user/state.json": b"plugin data",
            ".venv/keep.txt": b"venv",
            "database.sqlite": b"database",
        }
        for relative, content in self.protected.items():
            path = self.source / Path(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_helper_frozen_contract_matches_runtime_validator(self) -> None:
        self.assertEqual(tuple(helper.MANAGED_DIRECTORIES), MANAGED_DIRECTORIES)
        self.assertEqual(tuple(helper.MANAGED_FILES), MANAGED_FILES)
        self.assertEqual(helper.MANIFEST_NAME, MANIFEST_NAME)
        self.assertEqual(
            tuple(helper.TRANSACTION_FILES),
            (*MANAGED_FILES, MANIFEST_NAME),
        )

    def test_backup_install_restore_preserves_protected_paths_and_absence(self) -> None:
        backup = self.root / "backup"
        helper._backup(self.source, backup)
        helper._install(self.source, self.candidate)
        self.assertIn("new", (self.source / "README.md").read_text(encoding="utf-8"))
        self.assertIn("new", (self.source / "CHANGELOG.md").read_text(encoding="utf-8"))
        self.assertIn(
            "new",
            (self.source / MANIFEST_NAME).read_text(encoding="utf-8"),
        )
        self.assertTrue((self.source / "LICENSE").is_file())
        for relative, content in self.protected.items():
            self.assertEqual((self.source / Path(relative)).read_bytes(), content)

        helper._restore(self.source, backup)
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))
        self.assertIn("old", (self.source / "CHANGELOG.md").read_text(encoding="utf-8"))
        self.assertIn(
            "old",
            (self.source / MANIFEST_NAME).read_text(encoding="utf-8"),
        )
        self.assertFalse((self.source / "LICENSE").exists())
        for relative, content in self.protected.items():
            self.assertEqual((self.source / Path(relative)).read_bytes(), content)

    def test_candidate_hashes_are_rechecked_before_replacement(self) -> None:
        payload = {"candidate_files": candidate_entries(self.candidate)}
        helper._validate_candidate(self.candidate, payload)
        (self.candidate / "README.md").write_text("tampered\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "changed after validation"):
            helper._validate_candidate(self.candidate, payload)

    def test_restore_validates_the_complete_backup_before_removing_source(self) -> None:
        backup = self.root / "backup-incomplete"
        helper._backup(self.source, backup)
        (backup / "README.md").unlink()
        original_readme = (self.source / "README.md").read_bytes()

        with self.assertRaisesRegex(RuntimeError, "backup is incomplete"):
            helper._restore(self.source, backup)

        self.assertEqual((self.source / "README.md").read_bytes(), original_readme)
        self.assertTrue((self.source / "rocketcat_shell").is_dir())

    def test_pid_reuse_mismatch_never_calls_termination(self) -> None:
        with (
            mock.patch.object(helper, "_process_create_time", return_value=20.0),
            mock.patch.object(helper, "_terminate_windows_exact_process") as terminate,
        ):
            self.assertFalse(helper._pid_matches(321, 10.0))
            self.assertFalse(helper._terminate_exact_pid_tree(321, 10.0))
        terminate.assert_not_called()

    def test_graceful_timeout_forces_only_the_recorded_process(self) -> None:
        with (
            mock.patch.object(helper, "_wait_for_exact_pid", return_value=False) as wait,
            mock.patch.object(helper, "_pid_matches", return_value=True),
            mock.patch.object(helper, "_terminate_exact_pid_tree", return_value=True) as terminate,
        ):
            stopped, forced = helper._stop_service_process(321, 10.0)
        self.assertTrue(stopped)
        self.assertTrue(forced)
        terminate.assert_called_once_with(321, 10.0)
        self.assertEqual(wait.call_args_list[0].args[:2], (321, 10.0))
        self.assertEqual(wait.call_count, 1)


class UpdateHelperTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="rocketcat-helper-transaction-")
        self.source = Path(self.temporary.name) / "install"
        self.source.mkdir()
        write_managed_tree(self.source, "old")
        self.transaction_id = "d" * 24
        self.transaction_root = (
            self.source / "data/update/transactions" / self.transaction_id
        )
        self.candidate = self.transaction_root / "candidate" / helper.PRODUCT_NAME
        self.candidate.mkdir(parents=True)
        write_managed_tree(self.candidate, "new")
        self.transaction_file = self.transaction_root / "transaction.json"
        self.payload = {
            "transaction_id": self.transaction_id,
            "status": "prepared",
            "stage": "prepared",
            "current_version": "v0.2.2",
            "target_version": "v0.2.3",
            "source_root": str(self.source),
            "state_root": str(self.source),
            "candidate_root": str(self.candidate),
            "candidate_files": candidate_entries(self.candidate),
            "service_pid": 100,
            "service_create_time": 123.0,
            "health_urls": ["http://127.0.0.1:5751"],
            "created_at": 1.0,
            "updated_at": 1.0,
        }
        self._write_payload()
        self.current_health = mock.patch.object(
            helper,
            "_current_health_matches",
            return_value=True,
        )
        self.current_health.start()

    def tearDown(self) -> None:
        self.current_health.stop()
        self.temporary.cleanup()

    def _write_payload(self) -> None:
        self.transaction_file.write_text(
            json.dumps(self.payload, ensure_ascii=False),
            encoding="utf-8",
        )

    def test_apply_success_replaces_only_managed_paths(self) -> None:
        protected = self.source / "config/keep.json"
        protected.parent.mkdir(parents=True)
        protected.write_text("persistent", encoding="utf-8")
        process = SimpleNamespace(pid=222)
        with (
            mock.patch.object(helper, "_stop_service_process", return_value=(True, False)),
            mock.patch.object(helper, "_start_service", return_value=process),
            mock.patch.object(helper, "_health_matches", return_value=True),
        ):
            result = helper.apply_transaction(self.transaction_file)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 0)
        self.assertEqual(transaction["status"], "completed")
        self.assertIn("new", (self.source / "README.md").read_text(encoding="utf-8"))
        self.assertEqual(protected.read_text(encoding="utf-8"), "persistent")

    def test_preflight_tamper_fails_before_requesting_shutdown(self) -> None:
        (self.candidate / "README.md").write_text("tampered\n", encoding="utf-8")
        with mock.patch.object(helper, "_stop_service_process") as stop_service:
            result = helper.apply_transaction(self.transaction_file)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 2)
        self.assertEqual(transaction["status"], "failed")
        self.assertEqual(transaction["stage"], "preflight_failed")
        stop_service.assert_not_called()
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))

    def test_terminal_transaction_cannot_be_applied_by_a_delayed_helper(self) -> None:
        self.payload.update(status="failed", stage="helper_start_timeout")
        self._write_payload()
        with mock.patch.object(helper, "_stop_service_process") as stop_service:
            result = helper.apply_transaction(self.transaction_file)
        self.assertEqual(result, 2)
        stop_service.assert_not_called()
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))

    def test_preparation_timeout_race_cannot_cross_the_shutdown_boundary(self) -> None:
        def mark_timed_out(*_args: object) -> bool:
            latest = json.loads(self.transaction_file.read_text(encoding="utf-8"))
            latest.update(status="failed", stage="helper_start_timeout")
            self.transaction_file.write_text(json.dumps(latest), encoding="utf-8")
            return True

        with (
            mock.patch.object(helper, "_current_health_matches", side_effect=mark_timed_out),
            mock.patch.object(helper, "_stop_service_process") as stop_service,
        ):
            result = helper.apply_transaction(self.transaction_file)
        self.assertEqual(result, 2)
        stop_service.assert_not_called()
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))

    def test_non_loopback_health_url_fails_before_shutdown(self) -> None:
        self.payload["health_urls"] = ["http://127.0.0.1:5751/redirect"]
        self._write_payload()
        with mock.patch.object(helper, "_stop_service_process") as stop_service:
            result = helper.apply_transaction(self.transaction_file)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 2)
        self.assertEqual(transaction["stage"], "preflight_failed")
        stop_service.assert_not_called()

    def test_pending_webui_port_change_fails_before_shutdown(self) -> None:
        settings_path = self.source / "config/shell.json"
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        settings_path.write_text(json.dumps({"webui_port": 6000}), encoding="utf-8")
        with mock.patch.object(helper, "_stop_service_process") as stop_service:
            result = helper.apply_transaction(self.transaction_file)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 2)
        self.assertEqual(transaction["stage"], "preflight_failed")
        self.assertIn("restart RocketCatShell", transaction["error"])
        stop_service.assert_not_called()

    def test_target_health_failure_restores_old_version(self) -> None:
        processes = [SimpleNamespace(pid=222), SimpleNamespace(pid=333)]
        with (
            mock.patch.object(helper, "_stop_service_process", return_value=(True, False)),
            mock.patch.object(helper, "_start_service", side_effect=processes),
            mock.patch.object(helper, "_health_matches", side_effect=[False, True]),
            mock.patch.object(helper, "_terminate_started_process"),
        ):
            result = helper.apply_transaction(self.transaction_file)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 4)
        self.assertEqual(transaction["status"], "rolled_back")
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))

    def test_backup_failure_restarts_old_version_without_destructive_restore(self) -> None:
        previous = SimpleNamespace(pid=333)
        with (
            mock.patch.object(helper, "_stop_service_process", return_value=(True, False)),
            mock.patch.object(helper, "_backup", side_effect=OSError("disk full")),
            mock.patch.object(helper, "_start_service", return_value=previous),
            mock.patch.object(helper, "_health_matches", return_value=True),
        ):
            result = helper.apply_transaction(self.transaction_file)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 3)
        self.assertEqual(transaction["status"], "failed")
        self.assertEqual(transaction["stage"], "pre_replacement_failed")
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))

    def test_startup_recovery_restores_interrupted_replacement(self) -> None:
        backup = self.transaction_root / "backup"
        helper._backup(self.source, backup)
        helper._install(self.source, self.candidate)
        self.payload.update(status="running", stage="replacing")
        self._write_payload()
        result = helper.recover_transactions(self.source)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 0)
        self.assertEqual(transaction["status"], "rolled_back")
        self.assertEqual(transaction["stage"], "startup_recovered")
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))

    def test_every_post_backup_crash_stage_restores_managed_files(self) -> None:
        stages = (
            "backup_complete",
            "replacing",
            "starting_target",
            "checking_target",
            "rolling_back",
        )
        for index, stage in enumerate(stages, start=1):
            with self.subTest(stage=stage):
                source = Path(self.temporary.name) / f"stage-{index}"
                source.mkdir()
                write_managed_tree(source, "old")
                protected = source / "config/keep.json"
                protected.parent.mkdir(parents=True)
                protected.write_text(f"persistent-{stage}", encoding="utf-8")
                transaction_id = f"{index:024x}"
                transaction_root = source / "data/update/transactions" / transaction_id
                candidate = transaction_root / "candidate" / helper.PRODUCT_NAME
                candidate.mkdir(parents=True)
                write_managed_tree(candidate, "new")
                backup = transaction_root / "backup"
                helper._backup(source, backup)
                if stage != "backup_complete":
                    helper._install(source, candidate)
                payload = {
                    "transaction_id": transaction_id,
                    "status": "rolling_back" if stage == "rolling_back" else "running",
                    "stage": stage,
                    "current_version": "v0.2.2",
                    "target_version": "v0.2.3",
                    "source_root": str(source),
                    "state_root": str(source),
                    "candidate_root": str(candidate),
                    "candidate_files": candidate_entries(candidate),
                    "service_pid": 100,
                    "service_create_time": 123.0,
                    "health_urls": ["http://127.0.0.1:5751"],
                }
                transaction_file = transaction_root / "transaction.json"
                transaction_file.write_text(json.dumps(payload), encoding="utf-8")
                self.assertEqual(helper.recover_transactions(source), 0)
                self.assertIn(
                    "old",
                    (source / "README.md").read_text(encoding="utf-8"),
                )
                self.assertEqual(
                    protected.read_text(encoding="utf-8"),
                    f"persistent-{stage}",
                )

    def test_startup_recovery_marks_pre_replacement_crash_failed(self) -> None:
        result = helper.recover_transactions(self.source)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 0)
        self.assertEqual(transaction["status"], "failed")
        self.assertEqual(transaction["stage"], "recovery_not_required")

    def test_startup_recovery_blocks_when_replacement_backup_is_missing(self) -> None:
        self.payload.update(status="running", stage="replacing")
        self._write_payload()
        result = helper.recover_transactions(self.source)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(result, 2)
        self.assertEqual(transaction["status"], "recovery_required")
        self.assertEqual(helper.recover_transactions(self.source), 2)

    def test_startup_recovery_retries_a_recovery_required_transaction(self) -> None:
        backup = self.transaction_root / "backup"
        helper._backup(self.source, backup)
        helper._install(self.source, self.candidate)
        self.payload.update(status="recovery_required", stage="rollback_failed")
        self._write_payload()

        self.assertEqual(helper.recover_transactions(self.source), 0)
        transaction = json.loads(self.transaction_file.read_text(encoding="utf-8"))
        self.assertEqual(transaction["status"], "rolled_back")
        self.assertEqual(transaction["stage"], "startup_recovered")
        self.assertIn("old", (self.source / "README.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
