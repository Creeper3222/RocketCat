param(
    [string]$LiveRoot = "D:\git_test\RocketCatShell\v1\rocketcat_shell_rebuild",
    [string]$BotId = "bot_d9a1a3e8",
    [string]$AstrBotConfig = "D:\astrbot\astrbot\AstrBotLauncher-0.2.0\AstrBot\data\config\abconf_315b6620-b10c-4318-b01f-b09e19721d9c.json",
    [bool]$StartAfterDeploy = $false
)

$ErrorActionPreference = "Stop"
$WorkRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$LiveRoot = [System.IO.Path]::GetFullPath($LiveRoot)
$ExpectedLiveRoot = [System.IO.Path]::GetFullPath("D:\git_test\RocketCatShell\v1\rocketcat_shell_rebuild")
if ($LiveRoot -ne $ExpectedLiveRoot) {
    throw "Refusing unexpected deployment target: $LiveRoot"
}
if (!(Test-Path -LiteralPath $LiveRoot)) {
    throw "Windows live root does not exist: $LiveRoot"
}

$LockPath = Join-Path $LiveRoot "logs\rocketcat_shell.instance.lock"
if (Test-Path -LiteralPath $LockPath) {
    $rawLock = [System.IO.File]::ReadAllText($LockPath).Trim([char]0)
    try {
        $lockInfo = $rawLock | ConvertFrom-Json
        $runningProcess = Get-Process -Id ([int]$lockInfo.pid) -ErrorAction SilentlyContinue
        if ($null -ne $runningProcess) {
            throw "RocketCatShell is still running with PID $($lockInfo.pid). Stop it normally and retry."
        }
    } catch [System.Management.Automation.RuntimeException] {
        throw
    } catch {
        Write-Warning "Could not parse instance lock; treating it as stale: $($_.Exception.Message)"
    }
    Remove-Item -LiteralPath $LockPath -Force
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupRoot = "D:\vscode_project\rocketcat_user_hash_deploy_backup_$timestamp"
New-Item -ItemType Directory -Path $BackupRoot | Out-Null

$CodeFiles = @(
    "README.md",
    "rocketcat_shell\models.py",
    "rocketcat_shell\registry.py",
    "rocketcat_shell\bridge\config.py",
    "rocketcat_shell\bridge\hot_storage.py",
    "rocketcat_shell\bridge\id_map.py",
    "rocketcat_shell\bridge\onebot_actions.py",
    "rocketcat_shell\bridge\rocketchat_client.py",
    "rocketcat_shell\bridge\runtime.py",
    "rocketcat_shell\bridge\translator_inbound.py",
    "rocketcat_shell\bridge\user_identity.py",
    "rocketcat_shell\shell\manager.py",
    "rocketcat_shell\shell\webui.py",
    "rocketcat_shell\shell\static\app.js",
    "rocketcat_shell\shell\static\index.html",
    "rocketcat_shell\shell\static\styles.css",
    "tests\test_user_identity.py",
    "tools\migrate_user_identity.py",
    "tools\deploy_user_identity_windows_live.ps1"
)

$BackupFiles = @(
    "README.md",
    "rocketcat_shell\models.py",
    "rocketcat_shell\registry.py",
    "rocketcat_shell\bridge\config.py",
    "rocketcat_shell\bridge\hot_storage.py",
    "rocketcat_shell\bridge\id_map.py",
    "rocketcat_shell\bridge\onebot_actions.py",
    "rocketcat_shell\bridge\rocketchat_client.py",
    "rocketcat_shell\bridge\runtime.py",
    "rocketcat_shell\bridge\translator_inbound.py",
    "rocketcat_shell\shell\manager.py",
    "rocketcat_shell\shell\webui.py",
    "rocketcat_shell\shell\static\app.js",
    "rocketcat_shell\shell\static\index.html",
    "rocketcat_shell\shell\static\styles.css",
    "config\bots.json",
    "config\shell.json",
    "data\bots\$BotId\runtime.snapshot.bin",
    "data\bots\$BotId\runtime.journal.bin",
    "data\bots\$BotId\runtime_state.json"
)

foreach ($relativePath in $BackupFiles) {
    $source = Join-Path $LiveRoot $relativePath
    if (!(Test-Path -LiteralPath $source)) {
        continue
    }
    $destination = Join-Path $BackupRoot $relativePath
    New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
    Copy-Item -LiteralPath $source -Destination $destination -Force
}
$astrBackup = Join-Path $BackupRoot "astrbot\$(Split-Path -Leaf $AstrBotConfig)"
New-Item -ItemType Directory -Path (Split-Path -Parent $astrBackup) -Force | Out-Null
Copy-Item -LiteralPath $AstrBotConfig -Destination $astrBackup -Force

$Python = Join-Path $LiveRoot ".venv\Scripts\python.exe"
if (!(Test-Path -LiteralPath $Python)) {
    throw "Windows live Python is missing: $Python"
}

& $Python (Join-Path $WorkRoot "tools\migrate_user_identity.py") `
    --project-root $LiveRoot `
    --bot-id $BotId `
    --inject-synthetic `
    --anchor-user-id "6TZ4YPRbmhYwgFZuM" `
    --astrbot-config $AstrBotConfig
if ($LASTEXITCODE -ne 0) {
    throw "Identity migration failed before code deployment. Backup: $BackupRoot"
}

foreach ($relativePath in $CodeFiles) {
    $source = Join-Path $WorkRoot $relativePath
    if (!(Test-Path -LiteralPath $source)) {
        throw "Worktree deployment file is missing: $source"
    }
    $destination = Join-Path $LiveRoot $relativePath
    $resolvedDestination = [System.IO.Path]::GetFullPath($destination)
    if (!$resolvedDestination.StartsWith($LiveRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Deployment path escaped the live root: $resolvedDestination"
    }
    New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
    Copy-Item -LiteralPath $source -Destination $destination -Force
}

Push-Location $LiveRoot
try {
    & $Python -m unittest
    if ($LASTEXITCODE -ne 0) {
        throw "Windows live unit tests failed"
    }
    & $Python -m compileall -q rocketcat_shell
    if ($LASTEXITCODE -ne 0) {
        throw "Windows live compileall failed"
    }
    & $Python tools\check_requirements.py requirements.txt
    if ($LASTEXITCODE -ne 0) {
        throw "Windows live requirements audit failed"
    }
} finally {
    Pop-Location
}

Write-Host "Deployment and migration completed. Backup: $BackupRoot" -ForegroundColor Green
Write-Warning "Restart AstrBot so the updated admins_id list is loaded before enabling the bridge."
if ($StartAfterDeploy) {
    Start-Process -FilePath (Join-Path $LiveRoot "launcher.bat") `
        -WorkingDirectory $LiveRoot `
        -WindowStyle Hidden
    Write-Host "Windows live started." -ForegroundColor Green
}
