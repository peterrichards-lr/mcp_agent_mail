# Agent Mail: set up an agent on this Windows machine (push delivery, presence, attachments).
#
#   & ([scriptblock]::Create((irm https://<server>/install.ps1))) -Project github.com/owner/repo -Agent BlueLake -WriteHooks claude
#
# The PowerShell twin of /install.sh, with the same steps: it downloads the client scripts
# from the server that served it, signs you in through the browser to get a personal access
# token for this machine (no token is pasted or shown), writes the settings file outside your
# workspace (readable only by you), and prints, or with -WriteHooks merges into the current
# folder, the hook settings for Claude Code or Antigravity. Needs Python 3.9+ (the py
# launcher or python on PATH). Windows PowerShell 5.1 or PowerShell 7.
#
# Parameters:
#   -Project KEY         project key, e.g. github.com/owner/repo            (required)
#   -Agent NAME          this agent's name on the server                     (required)
#   -Name LABEL          token name shown on the server (default: host · project)
#   -NoBrowser           print the sign-in URL and paste the token (e.g. over SSH)
#   -WriteHooks WHICH    merge hook settings into .\ for claude or antigravity
param(
    [Parameter(Mandatory = $true)][string]$Project,
    [Parameter(Mandatory = $true)][string]$Agent,
    [string]$Name = "",
    [switch]$NoBrowser,
    [ValidateSet("", "claude", "antigravity")][string]$WriteHooks = ""
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version 3

# Filled in by the server that serves this file. The check matches the placeholder without
# its trailing underscores, so serving can't rewrite the check along with it (lfr-tunnel #1684).
$Server = "__AGENT_MAIL_SERVER__"
if (-not $Server -or $Server -like "*__AGENT_MAIL_SERVER*") {
    throw "This installer doesn't know its server. Fetch it from your server instead: & ([scriptblock]::Create((irm https://<your-agent-mail-server>/install.ps1))) -Project ... -Agent ..."
}
$OnWindows = $env:OS -eq "Windows_NT"

function Find-Python {
    # The real interpreter path, so hooks run without a launcher or a shell in between.
    $probe = "import sys; assert sys.version_info >= (3, 9); print(sys.executable)"
    foreach ($candidate in @(@("py", "-3"), @("python"), @("python3"))) {
        if (-not (Get-Command $candidate[0] -ErrorAction SilentlyContinue)) { continue }
        $launcherArgs = @($candidate | Select-Object -Skip 1)
        try { $found = & $candidate[0] @launcherArgs -c $probe 2>$null } catch { continue }
        # The Microsoft Store's python.exe stub prints nothing and fails until Python is installed.
        if ($LASTEXITCODE -eq 0 -and $found) { return ([string]($found | Select-Object -Last 1)).Trim() }
    }
    throw "Python 3.9 or newer is required. Install it from https://www.python.org/downloads/ (tick 'Add python.exe to PATH'), then run this again."
}

$Python = Find-Python
$HomeDir = if ($env:AGENT_MAIL_HOME) { $env:AGENT_MAIL_HOME } else { Join-Path $HOME ".agent-mail" }
$ConfigDir = if ($env:AGENT_MAIL_CONFIG_DIR) { $env:AGENT_MAIL_CONFIG_DIR } else { Join-Path (Join-Path $HOME ".config") "agent-mail" }
$Config = Join-Path $ConfigDir ((("{0}__{1}" -f $Project, $Agent) -replace '[^A-Za-z0-9._-]', '_') + ".json")
New-Item -ItemType Directory -Force -Path $HomeDir, $ConfigDir | Out-Null
if ($OnWindows) {
    # Only you (by SID, so domain accounts work too) may read the folder holding the token.
    $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    icacls $ConfigDir /inheritance:r /grant:r "*${sid}:(OI)(CI)F" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Couldn't restrict access to $ConfigDir." }
} elseif (Get-Command chmod -ErrorAction SilentlyContinue) {
    chmod 700 $ConfigDir
}

Write-Host "==> Downloading client scripts from $Server to $HomeDir"
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
foreach ($script in @("mail_wake.py", "agent_mail_files.py")) {
    $part = Join-Path $HomeDir "$script.part"
    Invoke-WebRequest -UseBasicParsing -UserAgent "mcp_agent_mail-installer" -Uri "$Server/client/$script" -OutFile $part
    Move-Item -Force $part (Join-Path $HomeDir $script)
}
$Wake = Join-Path $HomeDir "mail_wake.py"

Write-Host "==> Signing in to get an access token for this machine"
$loginArgs = @("--config", $Config, "--server", $Server, "--project", $Project, "--agent", $Agent)
if ($Name) { $loginArgs += @("--name", $Name) }
if ($NoBrowser) { $loginArgs += "--no-browser" }
& $Python $Wake login @loginArgs
if ($LASTEXITCODE -ne 0) { throw "Sign-in didn't complete; run this again." }

# mail_wake.py writes the hook settings itself (the same code serves install.sh), in the form
# each client needs on Windows, replacing any earlier Agent Mail hooks rather than duplicating them.
if ($WriteHooks) {
    Write-Host "==> $(if ($WriteHooks -eq 'claude') { 'Claude Code' } else { 'Antigravity' }) hooks"
    & $Python $Wake hooks $WriteHooks --config $Config --write
    if ($LASTEXITCODE -ne 0) { throw "Couldn't write the hook settings." }
} else {
    Write-Host "==> Hook settings (or rerun with -WriteHooks claude|antigravity inside the repo)"
    Write-Host "--- Claude Code: .claude\settings.local.json"
    & $Python $Wake hooks claude --config $Config
    Write-Host "--- Antigravity: .agents\hooks.json"
    & $Python $Wake hooks antigravity --config $Config
}

$quotedConfig = $Config -replace "'", "''"  # a ' in the path (C:\Users\O'Brien) doubles inside '...'
$tokenExpr = "`$((Get-Content -Raw '$quotedConfig' | ConvertFrom-Json).AGENT_MAIL_TOKEN)"
Write-Host ""
Write-Host "==> Connect the agent to the server's MCP endpoint (the token is read from the settings file,"
Write-Host "    so it never lands in your shell history). In PowerShell:"
Write-Host "    Claude Code:  claude mcp add --transport http agent-mail $Server/mcp/ --header `"Authorization: Bearer $tokenExpr`""
Write-Host "    Antigravity:  agy mcp add --header `"Authorization: Bearer $tokenExpr`" agent-mail $Server/mcp/"
Write-Host "Done. Settings: $Config"
