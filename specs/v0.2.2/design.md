# RocketCatShell v0.2.2 Technical Design

## Architecture

- The adapter converts configured upstream numeric ID sets into four semantic states, then applies Rocket.Chat shortcodes and per-room typing membership.
- `rocketcat_shell.update_manifest` owns the frozen package contract, manifest generation, ZIP extraction, and validation.
- `rocketcat_shell.updates` owns official Release discovery, cache/rate-limit behavior, package preparation, and transaction creation.
- `tools/update_helper.py` runs outside the service process, owns exact-path backup/replacement, target health checks, rollback, and startup recovery.
- The existing FastAPI WebUI exposes authenticated update APIs plus one minimal unauthenticated health endpoint.

## Security and persistence

- Release selection is restricted to `Creeper3222/RocketCat`, exact versioned asset names, TLS URLs, and GitHub SHA-256 digests.
- Archive validation rejects unsafe Windows paths, symlinks, case collisions, unknown runtime entries, protected paths, oversize packages, and manifest/file mismatches.
- The helper operates on frozen managed paths only. All transaction state and backups live below `data/update/` and are never packaged.
- Transaction JSON is written atomically and public responses remove local paths and download URLs.

## WebUI design

- Purpose: make version risk and recovery status obvious to a Windows administrator.
- Direction: a softened industrial control panel integrated into RocketCatShell's existing pink glass visual language.
- Palette: `#f6f2f7`, `#221a24`, `#eb4f8c`, `#2f9f78`, `#d73d63`.
- Typography: existing Microsoft YaHei UI / PingFang SC stack.
- Layout: asymmetric desktop status/action card, stacked mobile layout, release modal, and full-screen restart overlay.

## Validation

- Unit and integration tests cover adapter state sequences, release discovery, archive validation, transaction recovery, API behavior, and protected-state preservation.
- Real Chromium desktop/mobile checks cover normal, available, modal, restart, rollback, and error states.
