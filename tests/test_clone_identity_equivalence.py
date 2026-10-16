import subprocess
from pathlib import Path

from mcp_agent_mail.app import _resolve_project_identity
from mcp_agent_mail.config import get_settings


def _git(cwd: Path, *args: str) -> str:
    cp = subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True)
    return cp.stdout.strip()


def test_clones_share_same_project_uid_via_remote(tmp_path: Path, monkeypatch) -> None:
    """Two clones of one hosted repository share a project identity in git-remote mode.

    The default identity mode is "dir" (one project per folder), so this sets git-remote
    explicitly. The clones point origin at a hosted URL: a local-path remote isn't a hosted
    repository, so it never yields a remote fingerprint (the same rule as repository project keys).
    """
    monkeypatch.setenv("WORKTREES_ENABLED", "1")
    monkeypatch.setenv("PROJECT_IDENTITY_MODE", "git-remote")
    get_settings.cache_clear()
    remote = tmp_path / "remote.git"
    _git(tmp_path, "init", "--bare", str(remote))
    c1 = tmp_path / "clone1"
    c2 = tmp_path / "clone2"
    _git(tmp_path, "clone", str(remote), str(c1))
    _git(tmp_path, "clone", str(remote), str(c2))
    for clone in (c1, c2):
        _git(clone, "remote", "set-url", "origin", "https://github.com/owner/repo.git")
    id1 = _resolve_project_identity(str(c1))
    id2 = _resolve_project_identity(str(c2))
    assert id1["normalized_remote"] == id2["normalized_remote"] == "github.com/owner/repo"
    assert id1["project_uid"] == id2["project_uid"]
    assert id1["identity_mode_used"] == "git-remote"

    # A clone of a different repository is a different project. (A fresh clone: once resolved,
    # a clone's identity is pinned in .git/agent-mail/project-id and survives remote changes.)
    c3 = tmp_path / "clone3"
    _git(tmp_path, "clone", str(remote), str(c3))
    _git(c3, "remote", "set-url", "origin", "https://github.com/someone/else.git")
    assert _resolve_project_identity(str(c3))["project_uid"] != id1["project_uid"]

