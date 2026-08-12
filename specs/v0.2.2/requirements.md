# RocketCatShell v0.2.2 Requirements

## Introduction

RocketCatShell v0.2.2 introduces the first Windows Live version-management chain and aligns the built-in I Am Thinking adapter with upstream v0.2.0 while preserving all user-owned runtime state.

## Requirements

### Requirement 1 - Four-state I Am Thinking adaptation

**User Story:** As a Rocket.Chat operator, I want upstream thinking states represented by Rocket.Chat reactions and typing indicators without modifying AstrBot.

#### Acceptance Criteria

1. When upstream sends a configured thinking or tool emoji ID, RocketCatShell shall apply the configured Rocket.Chat shortcode and start or retain typing.
2. When upstream sends a configured error or done emoji ID, RocketCatShell shall apply the configured shortcode and stop typing immediately.
3. When an ID is unknown or belongs to multiple states, RocketCatShell shall reject the ambiguous action instead of guessing its meaning.
4. While multiple IDs represent one state, RocketCatShell shall make reaction and typing transitions idempotent across duplicate add/remove actions.

### Requirement 2 - Safe Windows version switching

**User Story:** As an administrator, I want to upgrade, roll back, or reinstall RocketCatShell from the WebUI with automatic recovery.

#### Acceptance Criteria

1. When version information is requested, RocketCatShell shall list only official GitHub Releases at or above v0.2.2 and shall label prereleases.
2. When an administrator selects a version, RocketCatShell shall verify the exact asset, digest, archive layout, manifest, compatibility, and every managed file before stopping the current service.
3. While replacing a version, RocketCatShell shall modify only the frozen managed paths and shall preserve configuration, runtime data, user plugins, logs, databases, snapshots, media, and `.venv`.
4. When the target version fails its health check, RocketCatShell shall restore and restart the previous version automatically.
5. When an interrupted replacement is detected at startup, the launcher shall recover it before normal dependency checks and application startup.

### Requirement 3 - Version-management WebUI

**User Story:** As an administrator, I want clear update status and recovery feedback inside the existing Basic Settings page.

#### Acceptance Criteria

1. When Basic Settings opens, the WebUI shall use cached release information and shall never install an update automatically.
2. When manual refresh is requested, the WebUI shall refresh within the server rate limit and show the last successful check time.
3. When a switch starts, the WebUI shall persist the transaction ID, show a restart overlay, and report completion, rollback, failure, or manual-recovery status after the service returns.
4. While rendered at desktop or mobile width, the version panel and modal shall remain readable and shall not create horizontal overflow.
