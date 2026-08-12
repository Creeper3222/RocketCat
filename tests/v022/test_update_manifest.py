from __future__ import annotations

import json
import stat
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

from rocketcat_shell.update_manifest import (
    MANAGED_DIRECTORIES,
    MANAGED_FILES,
    MANIFEST_NAME,
    PRODUCT_NAME,
    UpdatePackageError,
    audit_release_source_contract,
    compare_tags,
    inspect_and_extract_zip,
    parse_tag,
    write_manifest,
)


def create_release_tree(root: Path, *, version: str = "v0.2.2", marker: str = "base") -> None:
    for relative in MANAGED_DIRECTORIES:
        (root / Path(relative)).mkdir(parents=True, exist_ok=True)
    for relative in MANAGED_FILES:
        path = root / Path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{relative}:{marker}\n", encoding="utf-8")
    (root / "rocketcat_shell" / "__init__.py").write_text(
        f'__version__ = "{version}"\n',
        encoding="utf-8",
    )
    (root / "data/plugins/rocketcat_plugin_adapt_iamthinking/main.py").write_text(
        f"ADAPTER = {marker!r}\n",
        encoding="utf-8",
    )
    (root / "data/plugins/rocketcat_plugin_built_in_command/main.py").write_text(
        f"COMMAND = {marker!r}\n",
        encoding="utf-8",
    )


def write_release_zip(
    tree: Path,
    output: Path,
    *,
    extra_entries: list[tuple[zipfile.ZipInfo | str, bytes]] | None = None,
) -> None:
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(tree.rglob("*")):
            if path.is_file():
                archive.write(path, f"{PRODUCT_NAME}/{path.relative_to(tree).as_posix()}")
        for name_or_info, content in extra_entries or []:
            archive.writestr(name_or_info, content)


class SemanticVersionTests(unittest.TestCase):
    def test_semver_orders_stable_and_prereleases(self) -> None:
        tags = ["v0.2.2", "v0.2.3-rc.2", "v0.2.3", "v0.2.3-rc.1"]
        self.assertEqual(
            sorted(tags, key=parse_tag, reverse=True),
            ["v0.2.3", "v0.2.3-rc.2", "v0.2.3-rc.1", "v0.2.2"],
        )
        self.assertGreater(compare_tags("v0.2.3", "v0.2.3-rc.9"), 0)

    def test_semver_rejects_noncanonical_prereleases(self) -> None:
        for tag in ("0.2.2", "v0.2", "v0.2.2-rc..1", "v0.2.2-01"):
            with self.subTest(tag=tag), self.assertRaises(UpdatePackageError):
                parse_tag(tag)


class UpdateManifestArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="rocketcat-manifest-test-")
        self.root = Path(self.temporary.name)
        self.tree = self.root / PRODUCT_NAME
        self.tree.mkdir()
        create_release_tree(self.tree)
        write_manifest(self.tree, version="v0.2.2", tag_name="v0.2.2")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _inspect(
        self,
        *,
        extra_entries: list[tuple[zipfile.ZipInfo | str, bytes]] | None = None,
        expected_tag: str = "v0.2.2",
    ):
        archive = self.root / "release.zip"
        write_release_zip(self.tree, archive, extra_entries=extra_entries)
        return inspect_and_extract_zip(
            archive,
            self.root / "extract",
            expected_tag=expected_tag,
        )

    def test_valid_archive_round_trip(self) -> None:
        candidate, manifest = self._inspect()
        self.assertEqual(manifest["version"], "v0.2.2")
        self.assertIn("CHANGELOG.md", manifest["managed_files"])
        self.assertTrue((candidate / "CHANGELOG.md").is_file())
        self.assertTrue((candidate / "tools/update_helper.py").is_file())
        self.assertNotIn("config", {path.parts[0] for path in candidate.rglob("*")})

    def test_rejects_versions_below_transaction_floor(self) -> None:
        with self.assertRaisesRegex(UpdatePackageError, "below v0.2.2"):
            self._inspect(expected_tag="v0.2.1")

    def test_rejects_hash_mismatch(self) -> None:
        (self.tree / "README.md").write_text("tampered after manifest\n", encoding="utf-8")
        with self.assertRaisesRegex(UpdatePackageError, "hash mismatch|size mismatch"):
            self._inspect()

    def test_rejects_internal_version_mismatch(self) -> None:
        (self.tree / "rocketcat_shell/__init__.py").write_text(
            '__version__ = "v0.2.3"\n',
            encoding="utf-8",
        )
        write_manifest(self.tree, version="v0.2.2", tag_name="v0.2.2")
        with self.assertRaisesRegex(UpdatePackageError, "internal runtime version"):
            self._inspect()

    def test_rejects_relaxed_source_contract(self) -> None:
        manifest_path = self.tree / MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["source_compatibility"]["minimum"] = "v0.1.0"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(UpdatePackageError, "source compatibility"):
            self._inspect()

    def test_rejects_relaxed_python_contract(self) -> None:
        manifest_path = self.tree / MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["python"]["minimum"] = "3.10"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(UpdatePackageError, "Python minimum"):
            self._inspect()

    def test_rejects_missing_required_managed_file(self) -> None:
        manifest_path = self.tree / MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"] = [
            entry for entry in manifest["files"] if entry["path"] != "LICENSE"
        ]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        (self.tree / "LICENSE").unlink()
        with self.assertRaisesRegex(UpdatePackageError, "missing required managed files"):
            self._inspect()

    def test_rejects_unsafe_and_windows_specific_paths(self) -> None:
        unsafe_names = (
            f"{PRODUCT_NAME}/../escape.txt",
            f"{PRODUCT_NAME}/rocketcat_shell/C:/drive.py",
            f"{PRODUCT_NAME}/rocketcat_shell/file.py:stream",
            f"{PRODUCT_NAME}/rocketcat_shell/CON.py",
        )
        for index, name in enumerate(unsafe_names):
            with self.subTest(name=name):
                archive = self.root / f"unsafe-{index}.zip"
                write_release_zip(self.tree, archive, extra_entries=[(name, b"bad")])
                with self.assertRaises(UpdatePackageError):
                    inspect_and_extract_zip(
                        archive,
                        self.root / f"unsafe-extract-{index}",
                        expected_tag="v0.2.2",
                    )

    def test_rejects_case_collisions_and_multiple_roots(self) -> None:
        cases = (
            [(f"{PRODUCT_NAME}/README.MD", b"collision")],
            [("OtherRoot/file.txt", b"second root")],
        )
        for index, entries in enumerate(cases):
            with self.subTest(entries=entries):
                archive = self.root / f"collision-{index}.zip"
                write_release_zip(self.tree, archive, extra_entries=entries)
                with self.assertRaises(UpdatePackageError):
                    inspect_and_extract_zip(
                        archive,
                        self.root / f"collision-extract-{index}",
                        expected_tag="v0.2.2",
                    )

    def test_rejects_empty_protected_and_unknown_directories(self) -> None:
        for index, name in enumerate(
            (f"{PRODUCT_NAME}/config/", f"{PRODUCT_NAME}/surprise/")
        ):
            with self.subTest(name=name):
                info = zipfile.ZipInfo(name)
                info.external_attr = (stat.S_IFDIR | 0o755) << 16
                archive = self.root / f"directory-{index}.zip"
                write_release_zip(self.tree, archive, extra_entries=[(info, b"")])
                with self.assertRaises(UpdatePackageError):
                    inspect_and_extract_zip(
                        archive,
                        self.root / f"directory-extract-{index}",
                        expected_tag="v0.2.2",
                    )

    def test_rejects_symbolic_links(self) -> None:
        info = zipfile.ZipInfo(f"{PRODUCT_NAME}/rocketcat_shell/linked.py")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        with self.assertRaisesRegex(UpdatePackageError, "symbolic link"):
            self._inspect(extra_entries=[(info, b"target.py")])


class ReleaseSourceAuditTests(unittest.TestCase):
    def _repository(self) -> tuple[tempfile.TemporaryDirectory, Path]:
        temporary = tempfile.TemporaryDirectory(prefix="rocketcat-source-audit-")
        root = Path(temporary.name)
        create_release_tree(root)
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        subprocess.run(
            ["git", "-C", str(root), "config", "user.email", "test@example.invalid"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(root), "config", "user.name", "RocketCat Test"],
            check=True,
        )
        subprocess.run(["git", "-C", str(root), "add", "."], check=True)
        subprocess.run(
            ["git", "-C", str(root), "commit", "-qm", "fixture"],
            check=True,
        )
        return temporary, root

    def test_clean_tracked_runtime_contract_passes(self) -> None:
        temporary, root = self._repository()
        try:
            tracked = audit_release_source_contract(root)
            self.assertIn("rocketcat_shell/__init__.py", tracked)
        finally:
            temporary.cleanup()

    def test_tracked_source_brand_asset_is_classified_as_source_only(self) -> None:
        temporary, root = self._repository()
        try:
            asset = root / "assets/logo.png"
            asset.parent.mkdir(parents=True)
            asset.write_bytes(b"source-only-brand")
            subprocess.run(
                ["git", "-C", str(root), "add", "assets/logo.png"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "commit", "-qm", "brand source"],
                check=True,
            )
            tracked = audit_release_source_contract(root)
            self.assertIn("assets/logo.png", tracked)
        finally:
            temporary.cleanup()

    def test_untracked_runtime_file_is_rejected(self) -> None:
        temporary, root = self._repository()
        try:
            (root / "rocketcat_shell/untracked.py").write_text("bad\n", encoding="utf-8")
            with self.assertRaisesRegex(UpdatePackageError, "uncommitted or untracked"):
                audit_release_source_contract(root)
        finally:
            temporary.cleanup()

    def test_new_unclassified_tracked_tool_is_rejected(self) -> None:
        temporary, root = self._repository()
        try:
            (root / "tools/unclassified.py").write_text("bad\n", encoding="utf-8")
            subprocess.run(
                ["git", "-C", str(root), "add", "tools/unclassified.py"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "commit", "-qm", "unknown tool"],
                check=True,
            )
            with self.assertRaisesRegex(UpdatePackageError, "not classified"):
                audit_release_source_contract(root)
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
