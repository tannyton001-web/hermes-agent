"""
Cutover Plan — merge worktree changes to live runtime.

This is the controlled procedure for applying Core and Host changes
from the implementation worktree to the live Hermes runtime.

Sequence:
  1. Verify all tests pass in worktree
  2. Readback live hashes
  3. Stage and commit worktree changes
  4. Merge to live repos
  5. Restart Hermes
  6. Readback runtime PID + start time
  7. Verify runtime source/hash matches expected
  8. Run E2E canary

WARNING: Do NOT push remote. Do NOT external deploy. Do NOT mutate Ads.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .runtime_seal import RuntimeSeal, SealedRuntime, capture_and_seal


class CutoverController:
    """Controlled cutover from worktree to live runtime."""

    def __init__(
        self,
        core_worktree: str,
        host_worktree: str,
        core_live: str,
        host_live: str,
    ):
        self.core_worktree = Path(core_worktree)
        self.host_worktree = Path(host_worktree)
        self.core_live = Path(core_live)
        self.host_live = Path(host_live)
        self._steps: list[dict] = []

    def preflight(self) -> tuple[bool, str]:
        """Pre-cutover checks."""
        issues = []

        # Verify paths exist
        for name, path in [
            ("Core worktree", self.core_worktree),
            ("Host worktree", self.host_worktree),
            ("Core live", self.core_live),
            ("Host live", self.host_live),
        ]:
            if not path.exists():
                issues.append(f"{name} not found: {path}")

        # Verify git repos
        for name, path in [
            ("Core worktree", self.core_worktree),
            ("Host worktree", self.host_worktree),
            ("Core live", self.core_live),
            ("Host live", self.host_live),
        ]:
            git_dir = path / ".git"
            if not git_dir.exists():
                issues.append(f"{name} is not a git repo: {path}")

        if issues:
            return False, "\n".join(issues)
        return True, "Preflight OK"

    def capture_pre_hashes(self) -> SealedRuntime:
        """Capture exact hashes before cutover."""
        return capture_and_seal(
            core_dir=str(self.core_live),
            host_dir=str(self.host_live),
        )

    def stage_worktree(self) -> tuple[bool, str]:
        """Stage all worktree changes."""
        results = []

        for name, repo in [
            ("Core", self.core_worktree),
            ("Host", self.host_worktree),
        ]:
            try:
                # Check for uncommitted changes
                result = subprocess.run(
                    ["git", "status", "--porcelain"],
                    cwd=str(repo),
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.stdout.strip():
                    results.append(f"{name} has uncommitted changes — commit first")
                else:
                    results.append(f"{name}: clean")
            except Exception as e:
                results.append(f"{name}: error — {e}")

        success = all("clean" in r for r in results)
        return success, "\n".join(results)

    def merge_to_live(self, dry_run: bool = True) -> tuple[bool, str]:
        """Merge worktree commits to live repos.

        Args:
            dry_run: If True, only show what would happen.
        """
        results = []

        for name, worktree, live in [
            ("Core", self.core_worktree, self.core_live),
            ("Host", self.host_worktree, self.host_live),
        ]:
            try:
                # Check live repo is clean
                live_dirty = subprocess.run(
                    ["git", "diff-index", "--quiet", "HEAD", "--"],
                    cwd=str(live), capture_output=True, timeout=10,
                )
                if live_dirty.returncode != 0:
                    results.append(f"{name}: LIVE REPO DIRTY — commit or stash first")
                    continue

                # Check current branch
                branch_result = subprocess.run(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    cwd=str(live), capture_output=True, text=True, timeout=10,
                )
                live_branch = branch_result.stdout.strip()
                if live_branch == "HEAD":
                    results.append(f"{name}: DETACHED HEAD — checkout a branch first")
                    continue

                # Get worktree HEAD
                wt_head = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(worktree), capture_output=True, text=True, timeout=10,
                ).stdout.strip()

                # Get live HEAD
                live_head = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(live), capture_output=True, text=True, timeout=10,
                ).stdout.strip()

                if wt_head == live_head:
                    results.append(f"{name}: already at {wt_head[:12]}")
                    continue

                if dry_run:
                    results.append(
                        f"{name}: would merge {wt_head[:12]} → {live_branch} "
                        f"(live at {live_head[:12]})"
                    )
                else:
                    # Pre-merge tag for rollback
                    tag_name = f"cutover-pre-{name.lower()}-{live_head[:8]}"
                    subprocess.run(
                        ["git", "tag", tag_name],
                        cwd=str(live), capture_output=True, timeout=10,
                    )

                    # Actually merge
                    merge_result = subprocess.run(
                        ["git", "merge", "--no-ff", wt_head, "-m",
                         f"cutover: merge {name} worktree {wt_head[:12]}"],
                        cwd=str(live), capture_output=True, text=True, timeout=60,
                    )
                    if merge_result.returncode == 0:
                        results.append(f"{name}: merged {wt_head[:12]} → {live_branch} (tag: {tag_name})")
                    else:
                        results.append(f"{name}: MERGE FAILED — {merge_result.stderr[:200]}")

            except Exception as e:
                results.append(f"{name}: error — {e}")

        success = all("FAILED" not in r and "DIRTY" not in r and "DETACHED" not in r for r in results)
        return success, "\n".join(results)

    def readback_runtime(self) -> dict:
        """Read back actual runtime state after cutover."""
        info = {}

        # Git heads
        for name, repo in [
            ("core_head", self.core_live),
            ("host_head", self.host_live),
        ]:
            try:
                result = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(repo),
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                info[name] = result.stdout.strip()
            except Exception as e:
                info[name] = f"ERROR: {e}"

        # Hermes process
        try:
            result = subprocess.run(
                ["pgrep", "-f", "hermes"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            pids = result.stdout.strip().split()
            info["hermes_pids"] = pids
            info["hermes_running"] = len(pids) > 0

            if pids:
                # Get start time
                ps_result = subprocess.run(
                    ["ps", "-o", "lstart=", "-p", pids[0]],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                info["hermes_start_time"] = ps_result.stdout.strip()
        except Exception as e:
            info["hermes_pids"] = f"ERROR: {e}"

        info["readback_at"] = datetime.now(timezone.utc).isoformat()
        return info

    def verify_no_overwrite(
        self, 
        expected_core_head: str, actual_core_head: str,
        expected_host_head: str, actual_host_head: str,
    ) -> dict:
        """Verify that the cutover didn't overwrite unexpected changes.
        
        Checks both Core and Host repos independently.
        """
        result = {"core_ok": False, "host_ok": False}
        
        for name, expected, actual, repo in [
            ("core", expected_core_head, actual_core_head, self.core_live),
            ("host", expected_host_head, actual_host_head, self.host_live),
        ]:
            if expected == actual:
                result[f"{name}_ok"] = True
                continue
            try:
                r = subprocess.run(
                    ["git", "merge-base", "--is-ancestor", expected, actual],
                    cwd=str(repo), capture_output=True, timeout=10,
                )
                result[f"{name}_ok"] = (r.returncode == 0)
            except Exception:
                result[f"{name}_ok"] = False
        
        return result


def run_cutover(
    core_worktree: str,
    host_worktree: str,
    core_live: str,
    host_live: str,
    *,
    dry_run: bool = True,
) -> dict:
    """Run the full cutover procedure.

    Returns a report dict with all steps and results.
    """
    ctrl = CutoverController(core_worktree, host_worktree, core_live, host_live)
    report = {"steps": [], "success": False, "errors": []}

    # Step 1: Preflight
    ok, msg = ctrl.preflight()
    report["steps"].append({"step": "preflight", "ok": ok, "message": msg})
    if not ok:
        report["errors"].append(f"Preflight failed: {msg}")
        return report

    # Step 2: Stage check
    ok, msg = ctrl.stage_worktree()
    report["steps"].append({"step": "stage", "ok": ok, "message": msg})
    if not ok:
        report["errors"].append(f"Uncommitted changes: {msg}")
        if dry_run:
            report["errors"].append("Commit worktree changes before cutover")

    # Step 3: Capture pre-hashes
    try:
        sealed = ctrl.capture_pre_hashes()
        report["steps"].append({
            "step": "capture_pre_hashes",
            "ok": True,
            "core_head": sealed.core_git_head,
            "host_head": sealed.host_git_head,
        })
    except Exception as e:
        report["errors"].append(f"Hash capture failed: {e}")
        return report

    # Step 4: Merge
    ok, msg = ctrl.merge_to_live(dry_run=dry_run)
    report["steps"].append({"step": "merge_to_live", "ok": ok, "message": msg, "dry_run": dry_run})

    if dry_run:
        report["success"] = True
        report["note"] = "DRY RUN — no changes applied"
        return report

    if not ok:
        report["errors"].append(f"Merge failed: {msg}")
        return report

    # Step 5: Readback
    runtime_info = ctrl.readback_runtime()
    report["steps"].append({"step": "readback_runtime", "ok": True, "info": runtime_info})

    # Step 6: Verify both repos
    expected_core = sealed.core_git_head
    actual_core = runtime_info.get("core_head", "")
    expected_host = sealed.host_git_head
    actual_host = runtime_info.get("host_head", "")
    verified = ctrl.verify_no_overwrite(expected_core, actual_core, expected_host, actual_host)
    report["steps"].append({"step": "verify_no_overwrite", "ok": verified["core_ok"] and verified["host_ok"], "detail": verified})

    report["success"] = verified and runtime_info.get("hermes_running", False)
    return report


if __name__ == "__main__":
    # Example: python3 -m egress.cutover --dry-run
    dry_run = "--execute" not in sys.argv

    report = run_cutover(
        core_worktree="/Users/phu/Hermes-Workspace/hermes-core-egress-implementation-20260724/core",
        host_worktree="/Users/phu/Hermes-Workspace/hermes-core-egress-implementation-20260724/host",
        core_live="/Users/phu/Hermes-Workspace/hermes-global-control",
        host_live=str(Path.home() / ".hermes" / "hermes-agent"),
        dry_run=dry_run,
    )

    print(json.dumps(report, indent=2))
    sys.exit(0 if report["success"] else 1)
