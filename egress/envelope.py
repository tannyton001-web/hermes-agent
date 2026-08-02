"""
Emission Envelope — typed container for all outbound messages.

Every message leaving the Hermes system MUST be wrapped in an EmissionEnvelope.
The envelope carries identity (task, session, turn), content hash for
idempotency, and a capability tag that the Egress Coordinator validates
before allowing delivery.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class EnvelopeKind(str, Enum):
    """Classification of outbound message types."""
    TURN_RESPONSE = "turn_response"
    PROACTIVE_NOTIFICATION = "proactive_notification"
    STREAM_CHUNK = "stream_chunk"
    TOOL_RESULT = "tool_result"
    MEDIA = "media"
    WARNING = "warning"
    BACKGROUND_NOTIFY = "background_notify"
    CRON_DELIVERY = "cron_delivery"
    API_SSE = "api_sse"
    CLI_OUTPUT = "cli_output"
    TUI_OUTPUT = "tui_output"


@dataclass
class EmissionEnvelope:
    """Typed emission envelope for all outbound Hermes messages."""

    kind: EnvelopeKind
    task_id: Optional[str]
    session_id: Optional[str]
    turn_id: Optional[str]
    payload: str  # the actual message content (markdown or plain text)
    metadata: dict = field(default_factory=dict)

    # Auto-computed fields
    envelope_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    message_sha256: str = ""

    def __post_init__(self):
        if not self.message_sha256:
            self.message_sha256 = self._compute_sha256()

    def _compute_sha256(self) -> str:
        """Deterministic content hash for idempotency key."""
        return hashlib.sha256(
            json.dumps({
                "kind": self.kind.value,
                "task_id": self.task_id,
                "session_id": self.session_id,
                "turn_id": self.turn_id,
                "payload": self.payload,
            }, sort_keys=True).encode("utf-8")
        ).hexdigest()

    @property
    def idempotency_key(self) -> str:
        """Composite key for delivery deduplication.
        
        Fully deterministic — uses only content-derived values.
        Two envelopes with same (kind, task_id, session_id, turn_id, payload)
        will produce identical idempotency keys.
        """
        parts = [
            self.kind.value,
            self.task_id or "no-task",
            self.session_id or "no-session",
            self.turn_id or "no-turn",
            self.message_sha256,  # full 256-bit hash of content
        ]
        return ":".join(parts)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EmissionEnvelope":
        kind = EnvelopeKind(data["kind"]) if isinstance(data["kind"], str) else data["kind"]
        return cls(
            kind=kind,
            task_id=data.get("task_id"),
            session_id=data.get("session_id"),
            turn_id=data.get("turn_id"),
            payload=data["payload"],
            metadata=data.get("metadata", {}),
            envelope_id=data.get("envelope_id", str(uuid.uuid4())),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            message_sha256=data.get("message_sha256", ""),
        )
