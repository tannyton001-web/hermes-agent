"""
Transactional Durable Outbox for Egress messages.

Every outbound message is first written durably to the outbox before
being dispatched. In case of crash or restart, undelivered messages can be
recovered and replayed. Completed deliveries are marked to support
idempotent recovery.

Storage: JSON files under ~/.hermes/egress/outbox/
Schema: one file per envelope, named {envelope_id}.json
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from .envelope import EmissionEnvelope


class DeliveryState(str, Enum):
    """Full delivery lifecycle states.
    
    Lifecycle:
      PREPARED → COMMITTED → CLAIMED → DISPATCHED
        → PROVIDER_ACCEPTED → USER_VISIBLE_ACKED  (success path)
        → RETRY_WAIT → CLAIMED                      (retry path)
        → FAILED → RETRY_WAIT | DEAD_LETTER         (failure path)
    
    Terminal states: USER_VISIBLE_ACKED, DEAD_LETTER
    """
    PREPARED = "PREPARED"              # envelope created, not yet committed
    COMMITTED = "COMMITTED"            # durably written to outbox
    CLAIMED = "CLAIMED"                # claimed by a dispatcher (lease/CAS)
    DISPATCHING = "DISPATCHING"        # in-flight to platform adapter
    PROVIDER_ACCEPTED = "PROVIDER_ACCEPTED"  # platform acknowledged receipt
    USER_VISIBLE_ACKED = "USER_VISIBLE_ACKED"  # confirmed user-visible (terminal)
    RETRY_WAIT = "RETRY_WAIT"          # backing off before retry
    FAILED = "FAILED"                  # permanent failure (can transition to DEAD_LETTER)
    DEAD_LETTER = "DEAD_LETTER"        # unrecoverable, logged for audit (terminal)

    # Legacy aliases for backward compatibility
    PENDING = "COMMITTED"   # PENDING was the old name for COMMITTED
    DELIVERED = "USER_VISIBLE_ACKED"  # DELIVERED was the old name
    
    @property
    def is_terminal(self) -> bool:
        return self in (DeliveryState.USER_VISIBLE_ACKED, DeliveryState.DEAD_LETTER)
    
    @property
    def is_retryable(self) -> bool:
        return self in (DeliveryState.FAILED, DeliveryState.RETRY_WAIT)


@dataclass
class OutboxEntry:
    envelope: EmissionEnvelope
    state: DeliveryState = DeliveryState.PENDING
    attempts: int = 0
    last_attempt_at: Optional[str] = None
    last_error: Optional[str] = None
    platform_result: Optional[dict] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["envelope"] = self.envelope.to_dict()
        d["state"] = self.state.value
        d["schema_version"] = OutboxStore.SCHEMA_VERSION
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "OutboxEntry":
        envelope = EmissionEnvelope.from_dict(data["envelope"])
        return cls(
            envelope=envelope,
            state=DeliveryState(data["state"]),
            attempts=data.get("attempts", 0),
            last_attempt_at=data.get("last_attempt_at"),
            last_error=data.get("last_error"),
            platform_result=data.get("platform_result"),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            updated_at=data.get("updated_at", datetime.now(timezone.utc).isoformat()),
        )


class OutboxStore:
    """File-based transactional outbox with atomic writes and flock."""

    SCHEMA_VERSION = "OUTBOX_V1"

    def __init__(self, base_dir: Optional[Path] = None):
        if base_dir is None:
            base_dir = Path.home() / ".hermes" / "egress" / "outbox"
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _entry_path(self, envelope_id: str) -> Path:
        return self.base_dir / f"{envelope_id}.json"

    def _lock_path(self, envelope_id: str) -> Path:
        return self.base_dir / f"{envelope_id}.lock"

    def write_entry(self, entry: OutboxEntry) -> bool:
        """Atomically write an outbox entry with file locking (read-modify-write)."""
        path = self._entry_path(entry.envelope.envelope_id)
        lock = self._lock_path(entry.envelope.envelope_id)

        # Atomic read-modify-write under cross-call lock
        try:
            with open(lock, "w") as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    # Read existing entry if any (for merge under lock)
                    existing = None
                    if path.exists():
                        try:
                            data = json.loads(path.read_text())
                            if data.get("schema_version") == self.SCHEMA_VERSION:
                                existing = OutboxEntry.from_dict(data)
                        except (json.JSONDecodeError, KeyError):
                            pass

                    # Merge or write new — KEEP attempts for ANY non-terminal
                    # existing state (audit #2: FAILED entries were overwritten
                    # by re-emits with attempts=0, so MAX_ATTEMPTS dead-letter
                    # never fired and failing envelopes retried forever).
                    if existing and not existing.state.is_terminal:
                        if existing.attempts > entry.attempts:
                            entry = existing
                        elif existing.attempts > 0:
                            entry.attempts = existing.attempts

                    entry.updated_at = datetime.now(timezone.utc).isoformat()
                    tmp_path = path.with_suffix(".tmp")
                    tmp_path.write_text(json.dumps(entry.to_dict(), indent=2, ensure_ascii=False))
                    tmp_path.rename(path)
                    return True
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        except Exception:
            return False

    def write_entry_idempotent(self, entry: OutboxEntry) -> tuple[bool, Optional[OutboxEntry]]:
        """Atomically claim-and-write: check idempotency AND write under one
        global lock — closes the TOCTOU window in coordinator.emit() where
        find_by_idempotency_key() (unlocked scan) and write_entry() were two
        separate steps: two concurrent emits with the same idempotency_key
        both passed the check and both wrote (audit #3 — reproduced with
        2x adapter.send() for one envelope).

        Returns (True, None) when written; (False, existing) when a terminal
        entry with the same idempotency_key already exists (skip dispatch).
        """
        global_lock = self.base_dir / ".dedup.lock"
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            with open(global_lock, "w") as glf:
                fcntl.flock(glf.fileno(), fcntl.LOCK_EX)
                try:
                    key = entry.envelope.idempotency_key
                    existing = self.find_by_idempotency_key(key)
                    # Skip when:
                    #   (a) existing is TERMINAL (any envelope_id) — the
                    #       classic idempotent-skip path;
                    #   (b) existing is IN-FLIGHT under a DIFFERENT
                    #       envelope_id — audit #3 TOCTOU: a concurrent emit
                    #       with the same key but a fresh envelope_id wrote a
                    #       second file → duplicate dispatch.
                    # Same envelope_id + non-terminal (watcher re-emit)
                    # still merges via write_entry.
                    if existing and (
                        existing.state.is_terminal
                        or existing.envelope.envelope_id != entry.envelope.envelope_id
                    ):
                        return False, existing
                    ok = self.write_entry(entry)
                    return (True, None) if ok else (False, None)
                finally:
                    fcntl.flock(glf.fileno(), fcntl.LOCK_UN)
        except Exception:
            return False, None

    def read_entry(self, envelope_id: str) -> Optional[OutboxEntry]:
        """Read an outbox entry by envelope ID."""
        path = self._entry_path(envelope_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            if data.get("schema_version") != self.SCHEMA_VERSION:
                return None
            return OutboxEntry.from_dict(data)
        except (json.JSONDecodeError, KeyError):
            return None

    def update_state(
        self,
        envelope_id: str,
        new_state: DeliveryState,
        error: Optional[str] = None,
        platform_result: Optional[dict] = None,
    ) -> bool:
        """Update delivery state atomically with cross-call locking."""
        lock = self._lock_path(envelope_id)
        path = self._entry_path(envelope_id)
        try:
            with open(lock, "w") as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    # Read under lock
                    if not path.exists():
                        return False
                    try:
                        data = json.loads(path.read_text())
                        if data.get("schema_version") != self.SCHEMA_VERSION:
                            return False
                        entry = OutboxEntry.from_dict(data)
                    except (json.JSONDecodeError, KeyError):
                        return False

                    # Modify under lock
                    entry.state = new_state
                    entry.last_error = error
                    if platform_result:
                        entry.platform_result = platform_result
                    entry.attempts += 1 if new_state in (DeliveryState.DISPATCHING, DeliveryState.RETRY_WAIT) else 0
                    entry.last_attempt_at = datetime.now(timezone.utc).isoformat() if new_state != DeliveryState.PENDING else entry.last_attempt_at
                    entry.updated_at = datetime.now(timezone.utc).isoformat()

                    # Write under lock (inline — no re-acquire)
                    tmp_path = path.with_suffix(".tmp")
                    tmp_path.write_text(json.dumps(entry.to_dict(), indent=2, ensure_ascii=False))
                    tmp_path.rename(path)
                    return True
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        except Exception:
            return False

    def list_pending(self) -> list[OutboxEntry]:
        """List all entries that need delivery (PENDING or RETRY_WAIT)."""
        results = []
        for path in sorted(self.base_dir.glob("*.json")):
            if path.name.endswith(".lock"):
                continue
            entry = self.read_entry(path.stem)
            if entry and entry.state in (DeliveryState.PENDING, DeliveryState.RETRY_WAIT, DeliveryState.FAILED):
                results.append(entry)
        return results

    def find_by_idempotency_key(self, key: str) -> Optional[OutboxEntry]:
        """Find an entry by its envelope's idempotency key.
        
        Scans ALL entries (active + in-flight + terminal) to prevent 
        duplicate delivery across crash/restart boundaries.
        CLAIMED/DISPATCHING entries indicate a prior attempt that may
        have completed on the provider side despite our crash.
        """
        # Check ALL entries — not just terminal ones
        for path in sorted(self.base_dir.glob("*.json")):
            if path.name.endswith(".lock"):
                continue
            entry = self.read_entry(path.stem)
            if entry and entry.envelope.idempotency_key == key:
                return entry
        return None

    def cleanup_delivered(self, older_than_hours: int = 24) -> int:
        """Remove terminal entries older than N hours."""
        cutoff = time.time() - (older_than_hours * 3600)
        removed = 0
        for path in self.base_dir.glob("*.json"):
            if path.name.endswith(".lock"):
                continue
            try:
                mtime = path.stat().st_mtime
                if mtime < cutoff:
                    data = json.loads(path.read_text())
                    state = data.get("state", "")
                    if state in (DeliveryState.USER_VISIBLE_ACKED.value, DeliveryState.DEAD_LETTER.value):
                        path.unlink()
                        removed += 1
            except Exception:
                pass
        return removed
