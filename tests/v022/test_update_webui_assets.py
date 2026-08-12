from __future__ import annotations

import re
import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STATIC_ROOT = ROOT / "rocketcat_shell/shell/static"


class IdCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []

    def handle_starttag(self, _tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.ids.extend(value for key, value in attrs if key == "id" and value)


class UpdateWebUiAssetTests(unittest.TestCase):
    def test_every_javascript_element_reference_exists_once(self) -> None:
        parser = IdCollector()
        parser.feed((STATIC_ROOT / "index.html").read_text(encoding="utf-8"))
        self.assertEqual(len(parser.ids), len(set(parser.ids)))
        javascript = (STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        references = set(re.findall(r"getElementById\('([^']+)'\)", javascript))
        self.assertEqual(references - set(parser.ids), set())

    def test_version_management_contract_is_present_and_windows_scoped(self) -> None:
        html = (STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        javascript = (STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        for identifier in (
            "versionManagementTitle",
            "updateReleaseModal",
            "updateConfirmModal",
            "updateRestartOverlay",
        ):
            self.assertIn(f'id="{identifier}"', html)
        for endpoint in (
            "/api/updates/status",
            "/api/updates/releases",
            "/api/updates/transactions/",
            "/api/updates/switch",
            "/api/health",
        ):
            self.assertIn(endpoint, javascript)
        self.assertIn("v0.2.1 及更早版本", html)
        self.assertIn("用户插件", html)

    def test_responsive_styles_have_balanced_blocks(self) -> None:
        css = (STATIC_ROOT / "styles.css").read_text(encoding="utf-8")
        self.assertEqual(css.count("{"), css.count("}"))
        self.assertIn("@media (max-width: 1120px)", css)
        self.assertIn("@media (max-width: 720px)", css)
        self.assertIn(".version-management-card", css)
        self.assertIn(".update-restart-overlay", css)

    def test_launcher_recovers_before_dependency_check(self) -> None:
        launcher = (ROOT / "launcher.bat").read_text(encoding="utf-8")
        recovery = launcher.index('"%PYTHON_CMD%" "%UPDATE_HELPER%" recover')
        dependency_check = launcher.index("Checking Python dependencies")
        self.assertLess(recovery, dependency_check)
        self.assertIn(
            'recover "%ROOT%." --active-transaction',
            launcher,
        )
        self.assertNotIn(
            'recover "%ROOT%" --active-transaction',
            launcher,
        )
        self.assertIn("ROCKETCATSHELL_UPDATE_TRANSACTION", launcher)
        self.assertIn("Startup has been stopped to protect this installation", launcher)

    def test_windows_pty_dependency_uses_python_314_wheel_line(self) -> None:
        requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        launcher = (ROOT / "launcher.bat").read_text(encoding="utf-8")
        self.assertIn('pywinpty>=3.0.5,<4; platform_system == "Windows"', requirements)
        self.assertNotIn("pywinpty>=2.0,<3", requirements)
        self.assertIn("--only-binary=pywinpty", launcher)


if __name__ == "__main__":
    unittest.main()
