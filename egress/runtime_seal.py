"""
Runtime Sealing — exact hash capture and verification.

At startup, captures the exact git HEAD and file hashes of the Core and Host.
These are sealed and cannot be overwritten during a task's execution.
Any mismatch between sealed hashes and live hashes at checkpoint time
indicates runtime drift and blocks PASS_SAFE.

Architecture:
  - seal() captures hashes at startup
  - verify() compares current state against sealed hashes
  - The seal is immutable once set (write-once)
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class SealedRuntime:
    """Immutable runtime hash snapshot."""
    core_git_head: str
    host_git_head: str
    core_dir: str
    host_dir: str
    sealed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    extra_hashes: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "core_git_head": self.core_git_head,
            "host_git_head": self.host_git_head,
            "core_dir": self.core_dir,
            "host_dir": self.host_dir,
            "sealed_at": self.sealed_at,
            "extra_hashes": self.extra_hashes,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


class RuntimeSeal:
    """Captures and verifies exact runtime hashes.

    Usage:
        seal = RuntimeSeal.capture(core_dir="...", host_dir="...")
        seal.seal()  # locks the snapshot
        ...
        ok, drift = seal.verify()  # check if anything changed
    """

    def __init__(self):
        self._sealed: Optional[SealedRuntime] = None
        self._locked = False

    def capture(
        self,
        core_dir: str,
        host_dir: str,
        *,
        extra_files: Optional[list[str]] = None,
    ) -> SealedRuntime:
        """Capture exact git HEADs and file hashes."""
        core_head = self._git_head(core_dir)
        host_head = self._git_head(host_dir)

        extra_hashes = {}
        if extra_files:
            for f in extra_files:
                p = Path(f)
                if p.exists():
                    extra_hashes[str(p)] = self._file_sha256(p)

        sealed = SealedRuntime(
            core_git_head=core_head or "UNKNOWN",
            host_git_head=host_head or "UNKNOWN",
            core_dir=str(Path(core_dir).resolve()),
            host_dir=str(Path(host_dir).resolve()),
            extra_hashes=extra_hashes,
        )
        return sealed

    def seal(self, snapshot: SealedRuntime) -> None:
        """Lock the seal — prevents further modification."""
        if self._locked:
            raise RuntimeError("RuntimeSeal is already locked")
        self._sealed = snapshot
        self._locked = True

    def verify(self) -> tuple[bool, list[str]]:
        """Compare current runtime against the sealed snapshot.

        Returns:
            (ok, drift_details): ok=True means everything matches.
        """
        if not self._sealed:
            return False, ["No sealed snapshot exists"]

        drift = []

        # Check Core git HEAD
        current_core = self._git_head(self._sealed.core_dir)
        if current_core != self._sealed.core_git_head:
            drift.append(
                f"CORE_HEAD_DRIFT: sealed={self._sealed.core_git_head[:12]} "
                f"current={current_core[:12] if current_core else 'UNKNOWN'}"
            )

        # Check Host git HEAD
        current_host = self._git_head(self._sealed.host_dir)
        if current_host != self._sealed.host_git_head:
            drift.append(
                f"HOST_HEAD_DRIFT: sealed={self._sealed.host_git_head[:12]} "
                f"current={current_host[:12] if current_host else 'UNKNOWN'}"
            )

        # Check for dirty worktrees (uncommitted changes)
        for name, d in [("Core", self._sealed.core_dir), ("Host", self._sealed.host_dir)]:
            dirty = self._git_dirty(d)
            if dirty:
                drift.append(f"{name}_DIRTY: uncommitted changes detected")

        # Check extra file hashes
        for path, sealed_hash in self._sealed.extra_hashes.items():
            p = Path(path)
            if not p.exists():
                drift.append(f"FILE_MISSING: {path}")
            else:
                current_hash = self._file_sha256(p)
                if current_hash != sealed_hash:
                    drift.append(
                        f"FILE_HASH_DRIFT: {path} sealed={sealed_hash[:12]} current={current_hash[:12]}"
                    )

        return len(drift) == 0, drift

    @property
    def is_sealed(self) -> bool:
        return self._locked and self._sealed is not None

    @property
    def snapshot(self) -> Optional[SealedRuntime]:
        return self._sealed

    @staticmethod
    def _git_head(directory: str) -> Optional[str]:
        """Get the current git HEAD commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None

    @staticmethod
    def _git_dirty(directory: str) -> bool:
        """Check if the working tree has uncommitted changes."""
        try:
            result = subprocess.run(
                ["git", "diff-index", "--quiet", "HEAD", "--"],
                cwd=directory,
                capture_output=True,
                timeout=10,
            )
            # Exit code 0 = clean, 1 = dirty
            return result.returncode != 0
        except Exception:
            return True  # Assume dirty on error

    @staticmethod
    def _file_sha256(path: Path) -> str:
        """Compute SHA-256 of a file."""
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()


# ── Global singleton ──────────────────────────────────────────────────

_runtime_seal: Optional[RuntimeSeal] = None


def get_runtime_seal() -> RuntimeSeal:
    global _runtime_seal
    if _runtime_seal is None:
        _runtime_seal = RuntimeSeal()
    return _runtime_seal


def capture_and_seal(core_dir: str, host_dir: str, **kwargs) -> SealedRuntime:
    """Convenience: capture and seal in one call."""
    seal = get_runtime_seal()
    snapshot = seal.capture(core_dir, host_dir, **kwargs)
    seal.seal(snapshot)
    return snapshot
