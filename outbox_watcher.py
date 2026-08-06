#!/usr/bin/env python3
"""
Hermes Runtime Outbox Watcher — polls egress outbox while Hermes is live.

CONTRACT: OUTBOX_WATCHER_CONTRACT_V1

Solves the TRUE_BLOCKER: recover_pending() only runs on startup.
This watcher runs as a background task inside the Hermes event loop,
polling the outbox directory for new COMMITTED entries and delivering
them via EgressCoordinator.emit().

Usage (one-time bootstrap):
    Add to Hermes startup:
    from outbox_watcher import install_outbox_watcher
    install_outbox_watcher(poll_interval=30)

The watcher survives Hermes auto-update because it lives in Core repo.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable, Any

logger = logging.getLogger("hermes.outbox_watcher")

CONTRACT_VERSION = "OUTBOX_WATCHER_CONTRACT_V1"
DEFAULT_POLL_INTERVAL = 30  # seconds

# Standby retry cadence: when the single-instance lock is held at boot (e.g.
# by a stale serve orphan), the watcher enters standby and retries every
# LOCK_RETRY_SECONDS instead of skipping forever — so it takes over as soon
# as the holder dies (watchdog kill). 5s is fast enough for canary E2E while
# remaining cheap (2 log lines / retry, skipped work is nil).
LOCK_RETRY_SECONDS = 5

# ── Paths ────────────────────────────────────────────────────────────────────
OUTBOX_DIR = Path.home() / ".hermes" / "egress" / "outbox"
WATCHER_STATE_FILE = Path.home() / ".hermes" / "supervisor" / "watcher_state.json"
# Single-instance lock: only ONE OutboxWatcher may poll the outbox at a time.
# Hermes desktop restart spawns a NEW serve process without killing old ones,
# so stale serve processes accumulate — each running its own watcher. Without
# this lock, N watchers poll the same COMMITTED envelope in the same window
# and N-deliver it (observed: CANARY-A delivered 10x by 11 concurrent
# watchers). fcntl.flock is per-open-file-description, so a second process
# opening the lock path fails LOCK_NB and exits cleanly.
WATCHER_LOCK_FILE = Path.home() / ".hermes" / "egress" / ".watcher.lock"

# ── Delivery states ─────────────────────────────────────────────────────────
DELIVERY_COMMITTED = "COMMITTED"
DELIVERY_CLAIMED = "CLAIMED"
DELIVERY_DISPATCHING = "DISPATCHING"
DELIVERY_PROVIDER_ACCEPTED = "PROVIDER_ACCEPTED"
DELIVERY_USER_VISIBLE_ACKED = "USER_VISIBLE_ACKED"
DELIVERY_FAILED = "FAILED"

# States that need delivery
PENDING_STATES = {DELIVERY_COMMITTED, "PENDING", "RETRY_WAIT", DELIVERY_FAILED}

# Audit FINDING 4: a coordinator crash between `await adapter.send()` and the
# PROVIDER_ACCEPTED write leaves the envelope stuck in CLAIMED/DISPATCHING —
# not in PENDING_STATES, so the watcher never recovers it (message lost).
# Reclaim those only after a lease age: if the entry is still mid-flight
# (fresh timestamp), another writer owns it and we must NOT touch it.
STUCK_STATES = {DELIVERY_CLAIMED, DELIVERY_DISPATCHING}
STUCK_LEASE_SECONDS = 300  # 5 min — a normal emit completes in <1s

# Audit FINDING 4: FAILED entries retried every 15s forever (no backoff, no
# attempt cap, and write_entry resets attempts on merge). Cap attempts and
# dead-letter the entry instead of hammering a permanently failing envelope.
MAX_ATTEMPTS = 10
DELIVERY_DEAD_LETTER = "DEAD_LETTER"

# ── Data model ──────────────────────────────────────────────────────────────

class DesktopCanaryAdapter:
    """Canonical desktop delivery via SQLite message persistence.
    
    This is NOT a mock — it uses the real Hermes messages table (SQLite)
    that the Desktop renderer reads via /api/sessions/{id}/messages.
    Messages delivered through this adapter appear as normal assistant
    messages in the Desktop chat timeline after page refresh/navigation.
    """
    
    async def send(self, chat_id: str, content: str, metadata=None, **kwargs) -> dict:
        """Deliver message via canonical desktop persistence."""
        try:
            from desktop_canary import deliver_as_canary_message
            session_id = chat_id if chat_id and chat_id != "local" else None
            if session_id is None:
                from desktop_canary import find_active_session
                session_id = find_active_session()
            if session_id is None:
                return {"ok": False, "error": "No active session found"}
            
            result = deliver_as_canary_message(
                session_id=session_id,
                content=content,
                task_id=(metadata or {}).get("task_id", "outbox_watcher"),
                # OPUS #5: pass the envelope idempotency key so crash-recovery
                # re-delivery is deduped at the SQLite layer (side table),
                # never duplicating a visible message.
                dedup_key=(metadata or {}).get("egress_dedup"),
            )
            if result.get("persisted"):
                return {"ok": True, "message_id": str(result.get("message_id"))}
            return {"ok": False, "error": result.get("error", "persistence failed")}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    
    def __getattr__(self, name):
        """Delegate unknown attributes — just return self for chaining."""
        if name.startswith("_"):
            raise AttributeError(name)
        return lambda *a, **kw: {"ok": False, "error": f"Desktop adapter: {name} not implemented"}


@dataclass
class WatcherState:
    """Persistent watcher state."""
    active: bool = True
    poll_interval: int = DEFAULT_POLL_INTERVAL
    total_polled: int = 0
    total_delivered: int = 0
    total_failed: int = 0
    total_duplicates_skipped: int = 0
    last_poll_at: str = ""
    last_delivery_at: str = ""
    started_at: str = ""
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self):
        if not self.started_at:
            self.started_at = datetime.now(timezone.utc).isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════════
# OUTBOX WATCHER
# ═══════════════════════════════════════════════════════════════════════════════

class OutboxWatcher:
    """Background poll loop for the egress outbox.

    Installed into the Hermes asyncio event loop. Polls OUTBOX_DIR every
    poll_interval seconds for new COMMITTED entries and delivers them.
    """

    def __init__(
        self,
        *,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        adapter_factory: Optional[Callable[[str], Any]] = None,
    ):
        self.poll_interval = poll_interval
        self.adapter_factory = adapter_factory
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._state = self._load_state()
        self._delivered_ids: set[str] = set()  # in-memory dedup

    def _load_state(self) -> WatcherState:
        if WATCHER_STATE_FILE.exists():
            try:
                data = json.loads(WATCHER_STATE_FILE.read_text())
                # The constructor interval is authoritative — a stale state
                # file (e.g. an earlier run with a different poll_interval)
                # must not silently change the live polling cadence. Without
                # this, a persisted 999s interval defeats install-time tuning
                # and starves the outbox of polls.
                data["poll_interval"] = self.poll_interval
                return WatcherState(**data)
            except Exception:
                pass
        return WatcherState(poll_interval=self.poll_interval)

    def _save_state(self) -> None:
        WATCHER_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = WATCHER_STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self._state), indent=2, ensure_ascii=False, default=str))
        tmp.rename(WATCHER_STATE_FILE)

    async def start(self) -> None:
        """Start the background poll loop — active-standby single-instance.

        Enforces single-instance via an exclusive non-blocking flock on
        WATCHER_LOCK_FILE. If another OutboxWatcher (from a stale serve
        process) already holds the lock at boot, this instance DOES NOT skip
        permanently — it enters standby and retries every
        LOCK_RETRY_SECONDS until it can acquire (audit FINDING 8: a boot-time
        skip left ZERO watchers polling when the holder was killed right
        after: lock freed but nobody retried, so canaries stayed COMMITTED
        forever). The first watcher to acquire polls; the rest wait in
        standby and take over when the holder dies — exactly one active
        watcher at any time, never zero.
        """
        if self._running:
            return
        while not self._acquire_lock():
            logger.warning(
                "OutboxWatcher: .watcher.lock held by another process — "
                f"standby, retry trong {LOCK_RETRY_SECONDS}s"
            )
            self._close_lock_handle()  # fd leak guard (FINDING 8)
            try:
                await asyncio.sleep(LOCK_RETRY_SECONDS)
            except asyncio.CancelledError:
                raise
        self._running = True
        self._state.active = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(f"OutboxWatcher started: poll_interval={self.poll_interval}s (lock held)")

    def _acquire_lock(self) -> bool:
        """Try to take the exclusive watcher lock (non-blocking)."""
        try:
            import fcntl
            WATCHER_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
            self._lock_handle = open(WATCHER_LOCK_FILE, "w")
            fcntl.flock(self._lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False
        except Exception as e:  # noqa: BLE001 — lock failure must never crash serve
            logger.warning(f"OutboxWatcher: lock acquire failed ({e}); starting anyway")
            return True

    def _close_lock_handle(self) -> None:
        """Close the lock fd if open — prevents fd leak on failed acquire
        (FINDING 8: an unclosed fd made lsof report the serve as 'holding'
        the lock while it was actually NOT flocking, confusing diagnosis)."""
        handle = getattr(self, "_lock_handle", None)
        if handle is not None:
            try:
                handle.close()
            except Exception:  # noqa: BLE001
                pass
            self._lock_handle = None

    async def stop(self) -> None:
        """Stop the background poll loop.

        Only a RUNNING watcher (i.e. one that actually holds the lock and
        polls) may persist state — audit #4: a lock-loser or self-test that
        called stop() on a non-running watcher clobbered watcher_state.json
        to active=false while the real holder was still polling.
        """
        if not self._running:
            self._release_lock()
            return
        self._running = False
        self._state.active = False
        self._save_state()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._release_lock()
        logger.info("OutboxWatcher stopped")

    def _release_lock(self) -> None:
        """Release the single-instance lock (if held)."""
        handle = getattr(self, "_lock_handle", None)
        if handle is not None:
            try:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_UN)
            except Exception:  # noqa: BLE001
                pass
            try:
                handle.close()
            except Exception:  # noqa: BLE001
                pass
            self._lock_handle = None

    async def _poll_loop(self) -> None:
        """Main poll loop — runs forever while _running is True."""
        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error(f"OutboxWatcher poll error: {e}")
            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self) -> None:
        """One poll iteration — check for and deliver pending outbox entries."""
        if not OUTBOX_DIR.exists():
            return

        self._state.total_polled += 1
        self._state.last_poll_at = utc_now()

        # OPUS #1 (redefine): disk-scan + flock are blocking sync I/O — run
        # them off the event loop via to_thread so a busy outbox (or a lock
        # held by another process) never stalls the serve's WebSocket/HTTP.
        pending = await asyncio.to_thread(self._find_pending_entries)
        if not pending:
            return

        logger.info(f"OutboxWatcher: found {len(pending)} pending entries")

        for entry_data in pending:
            envelope_id = entry_data.get("envelope", {}).get("envelope_id", "unknown")
            if envelope_id in self._delivered_ids:
                self._state.total_duplicates_skipped += 1
                continue

            try:
                delivered = await self._deliver_entry(entry_data)
                if delivered:
                    self._delivered_ids.add(envelope_id)
                    self._state.total_delivered += 1
                    self._state.last_delivery_at = utc_now()
                    # Persist ACK to the envelope file so idempotency survives
                    # a watcher restart — _delivered_ids is in-memory only and
                    # resets on every serve restart, which re-delivered entries
                    # that stayed COMMITTED (observed with stale TEST-20 spam).
                    await asyncio.to_thread(self._ack_envelope, envelope_id)
                else:
                    self._state.total_failed += 1
            except Exception as e:
                logger.error(f"Failed to deliver {envelope_id}: {e}")
                self._state.total_failed += 1

        # OPUS #1: bounded outbox growth — prune terminal entries older than
        # 24h every ~120 polls (30 min at poll_interval=15s) so the directory
        # never balloons and glob-scans stay cheap.
        self._poll_count = getattr(self, "_poll_count", 0) + 1
        if self._poll_count % 120 == 0:
            try:
                from egress.outbox import OutboxStore
                await asyncio.to_thread(self._cleanup_delivered)
            except Exception as e:
                logger.warning(f"OutboxWatcher cleanup skipped: {e}")

    def _cleanup_delivered(self) -> None:
        """Prune terminal outbox entries older than CLEANUP_AGE_HOURS."""
        try:
            from egress.outbox import OutboxStore
            store = OutboxStore(base_dir=OUTBOX_DIR)
            removed = store.cleanup_delivered(older_than_hours=24)
            if removed:
                logger.info(f"OutboxWatcher: cleaned {removed} delivered entries")
        except Exception as e:
            logger.warning(f"OutboxWatcher cleanup failed: {e}")

        self._save_state()

    def _find_pending_entries(self) -> list[dict]:
        """Find outbox entries that need delivery.

        Handles three delivery-relevant states:
        - PENDING_STATES (COMMITTED/PENDING/RETRY_WAIT/FAILED): deliver now.
        - STUCK_STATES (CLAIMED/DISPATCHING): reclaim only after the lease
          expires — a fresh timestamp means another writer owns the emit.
        - FAILED with attempts >= MAX_ATTEMPTS: dead-letter (audit FINDING 4:
          previously retried every 15s forever with attempts reset by merge).
        """
        import time as _time
        now = _time.time()
        results = []
        for f in sorted(OUTBOX_DIR.glob("*.json")):
            if f.name.endswith(".lock") or f.name.endswith(".tmp"):
                continue
            try:
                data = json.loads(f.read_text())
                state = data.get("state", "")
                if state in PENDING_STATES:
                    attempts = int(data.get("attempts", 0) or 0)
                    if state == DELIVERY_FAILED and attempts >= MAX_ATTEMPTS:
                        # Dead-letter: permanently failing envelope — stop
                        # hammering it every poll.
                        self._dead_letter(f, data)
                        continue
                    results.append(data)
                elif state in STUCK_STATES:
                    # Lease reclaim: only touch a stuck entry whose timestamp
                    # is older than the lease window (crashed mid-emit).
                    updated = data.get("updated_at", "") or data.get("created_at", "")
                    if updated:
                        try:
                            from datetime import datetime as _dt
                            from datetime import timezone as _tz
                            ts = _dt.fromisoformat(updated)
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=_tz.utc)
                            age = now - ts.timestamp()
                            if age > STUCK_LEASE_SECONDS:
                                logger.warning(f"OutboxWatcher: reclaiming stuck {state} envelope {f.stem} (age={age:.0f}s)")
                                results.append(data)
                        except Exception:
                            pass
            except Exception:
                pass
        return results

    def _dead_letter(self, path, data) -> None:
        """Mark a permanently failing envelope DEAD_LETTER (no more retries)."""
        try:
            data["state"] = DELIVERY_DEAD_LETTER
            data["dead_lettered_at"] = utc_now()
            data["dead_letter_reason"] = f"attempts={data.get('attempts', 0)} >= MAX_ATTEMPTS"
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str))
            tmp.rename(path)
            logger.warning(f"OutboxWatcher: dead-lettered {path.stem} after {data.get('attempts', 0)} attempts")
        except Exception as e:
            logger.error(f"OutboxWatcher _dead_letter failed for {path.stem}: {e}")

    def _fail_envelope(self, envelope_id: str, reason: str) -> None:
        """Flip a pending envelope to FAILED and increment attempts — under
        the per-envelope flock, mirroring _ack_envelope. OPUS #6: delivery
        failures (e.g. no adapter for platform) must move the envelope out of
        COMMITTED so attempts accumulate and dead-letter can eventually catch
        it; otherwise it stays COMMITTED and retries forever."""
        try:
            import fcntl
            f = OUTBOX_DIR / f"{envelope_id}.json"
            if not f.exists():
                return
            lock_path = OUTBOX_DIR / f"{envelope_id}.lock"
            with open(lock_path, "w") as lock_fh:
                fcntl.flock(lock_fh, fcntl.LOCK_EX)
                try:
                    data = json.loads(f.read_text())
                except Exception:
                    data = None
                if data and data.get("state") in PENDING_STATES:
                    data["state"] = DELIVERY_FAILED
                    data["attempts"] = int(data.get("attempts", 0) or 0) + 1
                    data["last_error"] = reason
                    data["updated_at"] = utc_now()
                    tmp = f.with_suffix(".json.tmp")
                    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str))
                    tmp.rename(f)
                    logger.info(f"OutboxWatcher failed {envelope_id}: {reason} (attempt {data['attempts']})")
        except Exception as e:
            logger.error(f"OutboxWatcher _fail_envelope failed for {envelope_id}: {e}")

    async def _deliver_entry(self, entry_data: dict) -> bool:
        """Deliver one outbox entry via EgressCoordinator with a real adapter."""
        envelope_id = entry_data.get("envelope", {}).get("envelope_id", "unknown")
        task_id = entry_data.get("envelope", {}).get("task_id", "unknown")
        payload = entry_data.get("envelope", {}).get("payload", "")
        platform = entry_data.get("envelope", {}).get("metadata", {}).get("platform", "cli")
        chat_id = entry_data.get("envelope", {}).get("metadata", {}).get("chat_id", "local")

        try:
            from egress.coordinator import get_coordinator
            from egress.envelope import EmissionEnvelope, EnvelopeKind
            from egress.coordinator import DeliveryTarget as EgressTarget

            coordinator = get_coordinator()

            # Audit #9: preserve the ORIGINAL envelope kind instead of
            # hardcoding background_notify — re-emitted entries keep their
            # classification/display kind and match the original
            # idempotency key.
            raw_kind = entry_data.get("envelope", {}).get("kind") or "background_notify"
            try:
                kind = EnvelopeKind(raw_kind)
            except ValueError:
                kind = EnvelopeKind("background_notify")

            envelope = EmissionEnvelope(
                kind=kind,
                task_id=task_id,
                session_id=entry_data.get("envelope", {}).get("session_id"),
                turn_id=entry_data.get("envelope", {}).get("turn_id"),
                payload=payload,
                metadata=entry_data.get("envelope", {}).get("metadata", {}),
                envelope_id=envelope_id,
                message_sha256=entry_data.get("envelope", {}).get("message_sha256", ""),
            )

            target = EgressTarget(
                platform=platform,
                chat_id=chat_id,
                thread_id=entry_data.get("envelope", {}).get("metadata", {}).get("thread_id"),
            )

            # Get a REAL adapter via factory — never pass None
            adapter = self._resolve_adapter(platform)
            if adapter is None:
                logger.warning(f"OutboxWatcher: no adapter for platform={platform}, failing {envelope_id}")
                # OPUS #6: no-adapter must FAIL + increment attempts so the
                # dead-letter path can catch it — previously it returned False
                # without touching state, so the envelope stayed COMMITTED and
                # was retried every 15s forever.
                self._fail_envelope(envelope_id, f"NO_ADAPTER_FOR_PLATFORM:{platform}")
                return False

            result = await coordinator.emit(envelope, target, adapter, idempotent=True)

            if result.success:
                logger.info(f"OutboxWatcher delivered: {envelope_id} → {platform}:{chat_id}")
                return True
            else:
                logger.warning(f"OutboxWatcher delivery failed: {envelope_id} error={result.error}")
                return False

        except ImportError:
            logger.debug(f"OutboxWatcher: not inside Hermes, skipping {envelope_id}")
            return False
        except Exception as e:
            logger.error(f"OutboxWatcher delivery error for {envelope_id}: {e}")
            return False

    def _ack_envelope(self, envelope_id: str) -> bool:
        """Persist a successful delivery by flipping the envelope to
        USER_VISIBLE_ACKED on disk. In-memory _delivered_ids resets on serve
        restart; without this the envelope stays COMMITTED and gets
        re-delivered (observed with stale TEST-20 background_notify spam).

        Uses the SAME per-envelope flock as OutboxStore (outbox.py) so the
        read-modify-write can't lose the platform_result/message_id that the
        coordinator wrote (audit FINDING 2: lost update when the watcher
        overwrote a PROVIDER_ACCEPTED entry from a stale snapshot)."""
        try:
            import fcntl
            for f in OUTBOX_DIR.glob("*.json"):
                if f.name.endswith(".lock") or f.name.endswith(".tmp"):
                    continue
                try:
                    data = json.loads(f.read_text())
                except Exception:
                    continue
                env = data.get("envelope", {})
                if env.get("envelope_id") != envelope_id or data.get("state") not in PENDING_STATES:
                    continue
                # Per-envelope lock: serialize with OutboxStore writers.
                lock_path = OUTBOX_DIR / f"{envelope_id}.lock"
                try:
                    with open(lock_path, "w") as lock_fh:
                        fcntl.flock(lock_fh, fcntl.LOCK_EX)
                        try:
                            # Re-read under lock — state may have advanced
                            # (coordinator wrote PROVIDER_ACCEPTED + message_id).
                            data = json.loads(f.read_text())
                        except Exception:
                            data = None
                        if data and data.get("state") in PENDING_STATES:
                            data["state"] = "USER_VISIBLE_ACKED"
                            data["acked_at"] = utc_now()
                            tmp = f.with_suffix(".json.tmp")
                            tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str))
                            tmp.rename(f)
                            logger.info(f"OutboxWatcher ACKed envelope on disk: {envelope_id}")
                            return True
                        # state already terminal (another watcher won the race) —
                        # treat as acked; the envelope is no longer pending.
                        logger.info(f"OutboxWatcher: {envelope_id} already terminal, ack skipped")
                        return True
                except Exception as e:
                    logger.error(f"OutboxWatcher _ack_envelope lock failed for {envelope_id}: {e}")
                    return False
        except Exception as e:
            logger.error(f"OutboxWatcher _ack_envelope failed for {envelope_id}: {e}")
        return False

    def _resolve_adapter(self, platform: str) -> Any:
        """Resolve a real platform adapter or desktop delivery path.
        
        For 'desktop'/'local' platforms, returns a DesktopCanaryAdapter
        that writes to canonical SQLite persistence via desktop_canary.py.
        For other platforms, tries the adapter_factory and known adapters.
        """
        # Desktop/local: use canonical SQLite persistence
        if platform in ("desktop", "local", "cli"):
            return DesktopCanaryAdapter()
        
        # Try injected factory first
        if self.adapter_factory:
            adapter = self.adapter_factory(platform)
            if adapter is not None:
                return adapter

        # Fall back to known platform adapters
        adapter_map = {
            "telegram": "plugins.platforms.telegram.adapter",
            "discord": "plugins.platforms.discord.adapter",
            "slack": "plugins.platforms.slack.adapter",
        }
        mod_path = adapter_map.get(platform)
        if mod_path:
            try:
                import importlib
                mod = importlib.import_module(mod_path)
                for attr in dir(mod):
                    obj = getattr(mod, attr)
                    if isinstance(obj, type) and hasattr(obj, "send"):
                        return obj()
            except (ImportError, Exception) as e:
                logger.debug(f"Cannot resolve adapter for {platform}: {e}")
        return None

    @property
    def stats(self) -> dict:
        return asdict(self._state)

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()


# ═══════════════════════════════════════════════════════════════════════════════
# INSTALLATION HOOKS
# ═══════════════════════════════════════════════════════════════════════════════

_watcher: Optional[OutboxWatcher] = None


def install_outbox_watcher(poll_interval: int = DEFAULT_POLL_INTERVAL) -> OutboxWatcher:
    """Install the outbox watcher into the current asyncio event loop.

    Call once at Hermes startup. Idempotent — returns existing watcher if
    already installed.

    Returns the watcher instance.
    """
    global _watcher
    if _watcher is not None:
        # Already installed — return existing watcher
        # If not running, try to start it in the current loop
        if not _watcher.is_running:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_watcher.start())
            except RuntimeError:
                pass
        return _watcher

    _watcher = OutboxWatcher(poll_interval=poll_interval)

    # Schedule start in the event loop. Audit #12: when no loop is running
    # yet, "will start when loop runs" was a LIE — nothing re-armed the
    # start. Re-arm via call_soon on the NEXT available loop so the watcher
    # reliably comes up even if installed during early startup.
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_watcher.start())
        logger.info("OutboxWatcher: installed in running event loop")
    except RuntimeError:
        # OPUS #10: the previous "re-arm via new_event_loop" branch was a lie —
        # a fresh loop was created but never run_forever(), so the task never
        # executed and the loop leaked. Be honest: without a running loop we
        # cannot start; web_server always calls this inside a running loop, so
        # this branch is effectively unreachable in production.
        logger.warning(
            "OutboxWatcher: no running event loop — cannot start now; "
            "install again from within a running loop"
        )

    return _watcher


def get_watcher() -> Optional[OutboxWatcher]:
    """Get the installed watcher, or None."""
    return _watcher


async def uninstall_outbox_watcher() -> None:
    """Stop and uninstall the watcher."""
    global _watcher
    if _watcher:
        await _watcher.stop()
        _watcher = None


def healthcheck() -> dict:
    """Check watcher health."""
    w = _watcher
    if w is None:
        return {"status": "NOT_INSTALLED", "contract": CONTRACT_VERSION}
    return {
        "status": "RUNNING" if w.is_running else "STOPPED",
        "contract": CONTRACT_VERSION,
        "stats": w.stats,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

async def _self_test():
    """Verify the watcher infrastructure."""
    # Test: watcher can be created and started
    watcher = OutboxWatcher(poll_interval=5)
    assert watcher.poll_interval == 5
    assert not watcher.is_running

    # Test: install into event loop
    w = install_outbox_watcher(poll_interval=999)
    assert w is not None
    assert w.poll_interval == 999

    # Test: second install returns same watcher
    w2 = install_outbox_watcher(poll_interval=30)
    assert w2 is w

    # Test: stats
    s = w.stats
    assert s["contract_version"] == CONTRACT_VERSION

    # Cleanup
    await uninstall_outbox_watcher()
    assert get_watcher() is None

    return True


if __name__ == "__main__":
    asyncio.run(_self_test())
    print(f"SELF-TEST: PASS ({CONTRACT_VERSION})")
