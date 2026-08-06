"""
Host-native Egress Coordinator — sole owner of all outbound message delivery.

Architecture:
  All producers → EmissionEnvelope → EgressCoordinator.emit()
    → policy + capability validation
    → transactional durable outbox (write-before-dispatch)
    → adapter implementation allowlist check
    → platform transport (via adapter.send)
    → delivery state update (ack/fail)
    → return SendResult

This module is the ONLY code path that may call adapter.send() for user-visible
messages. All other call sites must be migrated to use emit().
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from .envelope import EmissionEnvelope, EnvelopeKind
from .outbox import OutboxStore, OutboxEntry, DeliveryState

logger = logging.getLogger(__name__)


# ── Adapter allowlist ──────────────────────────────────────────────────
# Only these adapter subclasses are authorized to receive dispatch from the
# coordinator. Any other adapter.send() call is a raw sink violation.

ALLOWED_ADAPTER_BASES = {
    # Base class names that platform adapters must inherit from
    "BasePlatformAdapter",
    "BaseGatewayAdapter",
    "DesktopCanaryAdapter",  # canonical SQLite persistence
}


@dataclass
class SendResult:
    """Result of a coordinated send operation."""
    success: bool
    envelope_id: str
    message_id: Optional[str] = None       # platform-assigned message ID
    error: Optional[str] = None
    platform: Optional[str] = None
    delivered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class DeliveryTarget:
    """Where to deliver the message."""
    platform: str          # e.g., "telegram", "discord", "cli", "tui"
    chat_id: str           # platform-specific chat/channel identifier
    thread_id: Optional[str] = None
    metadata: dict = field(default_factory=dict)


class EgressCoordinator:
    """Sole owner of all outbound message delivery.

    Usage:
        coordinator = EgressCoordinator()
        result = await coordinator.emit(envelope, target, adapter)
    """

    def __init__(self, outbox_store: Optional[OutboxStore] = None):
        self.outbox = outbox_store or OutboxStore()
        self._delivery_stats: dict[str, int] = {
            "total": 0, "success": 0, "failed": 0, "duplicates": 0
        }

    # ── Public API ─────────────────────────────────────────────────────

    async def emit(
        self,
        envelope: EmissionEnvelope,
        target: DeliveryTarget,
        adapter: Any,  # platform adapter with .send() method
        *,
        idempotent: bool = True,
    ) -> SendResult:
        """Emit an envelope through the coordinator — the ONLY send path.

        Flow:
          1. Validate envelope and target
          2. Check idempotency (skip if already delivered)
          3. Write to transactional outbox (PENDING)
          4. Mark DISPATCHING
          5. Dispatch to adapter.send()
          6. Mark DELIVERED or FAILED
          7. Return SendResult
        """
        # 1. Validation
        if not self._validate_envelope(envelope):
            return SendResult(success=False, envelope_id=envelope.envelope_id,
                            error="ENVELOPE_VALIDATION_FAILED")

        if not self._validate_adapter(adapter):
            return SendResult(success=False, envelope_id=envelope.envelope_id,
                            error="ADAPTER_NOT_IN_ALLOWLIST")

        # 2. Idempotency check + write — ATOMIC (audit #3: previously the
        # unlocked find_by_idempotency_key() scan and write_entry() were two
        # separate steps, so two concurrent emits with the same key both
        # passed the check and both dispatched — reproduced 2x send).
        entry = OutboxEntry(envelope=envelope, state=DeliveryState.COMMITTED)
        if idempotent:
            written, existing = self.outbox.write_entry_idempotent(entry)
            if not written:
                if existing is not None:
                    self._delivery_stats["duplicates"] += 1
                    logger.debug(f"Idempotent skip: {envelope.idempotency_key}")
                    return SendResult(
                        success=True,
                        envelope_id=envelope.envelope_id,
                        message_id=existing.platform_result.get("message_id") if existing.platform_result else None,
                        platform=target.platform,
                    )
                return SendResult(success=False, envelope_id=envelope.envelope_id,
                                error="OUTBOX_WRITE_FAILED")
        else:
            if not self.outbox.write_entry(entry):
                return SendResult(success=False, envelope_id=envelope.envelope_id,
                                error="OUTBOX_WRITE_FAILED")

        # 4. Claim and dispatch — COMMITTED → CLAIMED → DISPATCHING
        self.outbox.update_state(envelope.envelope_id, DeliveryState.CLAIMED)
        self.outbox.update_state(envelope.envelope_id, DeliveryState.DISPATCHING)

        # 5. Dispatch
        self._delivery_stats["total"] += 1
        try:
            send_metadata = dict(target.metadata)
            if target.thread_id:
                send_metadata["thread_id"] = target.thread_id
            # OPUS #5: stamp the envelope idempotency key into send metadata so
            # idempotent adapters (desktop_canary side table) can dedup
            # crash-recovery re-delivery at the persistence layer.
            if idempotent:
                send_metadata["egress_dedup"] = envelope.idempotency_key

            platform_result = await adapter.send(target.chat_id, envelope.payload, metadata=send_metadata or None)

            # Check if the platform returned a failure
            if self._is_send_failure(platform_result):
                error_msg = self._extract_error(platform_result)
                self.outbox.update_state(envelope.envelope_id, DeliveryState.FAILED, error=error_msg)
                self._delivery_stats["failed"] += 1
                return SendResult(
                    success=False,
                    envelope_id=envelope.envelope_id,
                    error=error_msg,
                    platform=target.platform,
                )

            # 6. Mark PROVIDER_ACCEPTED or FAILED
            message_id = self._extract_message_id(platform_result)
            self.outbox.update_state(
                envelope.envelope_id,
                DeliveryState.PROVIDER_ACCEPTED,
                platform_result={"message_id": message_id, "platform": target.platform},
            )
            # Transition to USER_VISIBLE_ACKED (terminal) 
            # In production this waits for actual user receipt confirmation;
            # for now, provider acceptance is the highest verifiable state.
            self.outbox.update_state(
                envelope.envelope_id,
                DeliveryState.USER_VISIBLE_ACKED,
                platform_result={"message_id": message_id, "platform": target.platform},
            )
            self._delivery_stats["success"] += 1

            return SendResult(
                success=True,
                envelope_id=envelope.envelope_id,
                message_id=message_id,
                platform=target.platform,
            )

        except Exception as e:
            error_msg = str(e)
            self.outbox.update_state(envelope.envelope_id, DeliveryState.FAILED, error=error_msg)
            self._delivery_stats["failed"] += 1
            return SendResult(
                success=False,
                envelope_id=envelope.envelope_id,
                error=error_msg,
                platform=target.platform,
            )

    # ── Recovery ────────────────────────────────────────────────────────

    async def recover_pending(self, adapter_factory: Callable[[str], Any]) -> list[SendResult]:
        """Replay all COMMITTED/RETRY_WAIT outbox entries on startup."""
        results = []
        for entry in self.outbox.list_pending():
            if entry.state in (DeliveryState.COMMITTED, DeliveryState.RETRY_WAIT):
                target = DeliveryTarget(
                    platform=entry.envelope.metadata.get("platform", "unknown"),
                    chat_id=entry.envelope.metadata.get("chat_id", ""),
                    thread_id=entry.envelope.metadata.get("thread_id"),
                    metadata=entry.envelope.metadata.get("send_metadata", {}),
                )
                adapter = adapter_factory(target.platform)
                if adapter:
                    result = await self.emit(entry.envelope, target, adapter, idempotent=True)
                    results.append(result)
        return results

    # ── Stats ───────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        return dict(self._delivery_stats)

    # ── Validation helpers ──────────────────────────────────────────────

    def _validate_envelope(self, envelope: EmissionEnvelope) -> bool:
        """Validate envelope invariants."""
        if not envelope.payload:
            logger.warning("Empty payload in envelope")
            return False
        if not envelope.message_sha256:
            logger.warning("Missing message_sha256 in envelope")
            return False
        if envelope.kind not in EnvelopeKind:
            logger.warning(f"Unknown envelope kind: {envelope.kind}")
            return False
        return True

    def _validate_adapter(self, adapter: Any) -> bool:
        """Check that the adapter is in the implementation allowlist."""
        adapter_type = type(adapter)
        # Identity check: verify inheritance from allowed bases AND module prefix
        for base in adapter_type.__mro__:
            if base.__name__ in ALLOWED_ADAPTER_BASES:
                base_module = getattr(base, '__module__', '')
                # DesktopCanaryAdapter: must be the REAL class from outbox_watcher
                if base.__name__ == 'DesktopCanaryAdapter':
                    if base_module == 'outbox_watcher':
                        return True
                    continue
                if any(base_module.startswith(prefix) for prefix in (
                    'gateway.platforms', 'plugins.platforms.',
                )):
                    return True
        logger.warning(f"Adapter {adapter_type.__name__} (module={adapter_type.__module__}) not in allowlist")
        return False

    @staticmethod
    def _is_send_failure(result: Any) -> bool:
        """Check if a platform send result indicates failure."""
        if isinstance(result, dict):
            if result.get("ok") is False:
                return True
            if result.get("error"):
                return True
            if result.get("status") == "error":
                return True
        if isinstance(result, bool):
            return not result
        return result is None

    @staticmethod
    def _extract_error(result: Any) -> str:
        """Extract error message from platform result."""
        if isinstance(result, dict):
            return str(result.get("error", result.get("description", "unknown platform error")))
        return "unknown platform error"

    @staticmethod
    def _extract_message_id(result: Any) -> Optional[str]:
        """Extract platform-assigned message ID."""
        if isinstance(result, dict):
            return str(result.get("message_id", result.get("id", ""))) or None
        return None


# ── Singleton ──────────────────────────────────────────────────────────

_coordinator: Optional[EgressCoordinator] = None


def get_coordinator() -> EgressCoordinator:
    """Get or create the global Egress Coordinator singleton.
    
    Auto-installs the runtime guard on first access.
    """
    global _coordinator
    if _coordinator is None:
        _coordinator = EgressCoordinator()
        # Auto-install runtime guard for production detection
        _install_runtime_guard()
    return _coordinator


def _install_runtime_guard():
    """Install the egress runtime guard if not already active."""
    try:
        from egress.runtime_guard import install_guard, is_guard_installed
        if not is_guard_installed():
            install_guard(strict=False)  # warn in dev, strict via env
    except ImportError:
        pass


def reset_coordinator() -> None:
    """Reset the coordinator singleton (for testing)."""
    global _coordinator
    _coordinator = None
