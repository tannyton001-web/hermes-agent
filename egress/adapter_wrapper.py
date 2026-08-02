"""
Adapter Wrapper — intercepts adapter.send() and routes through EgressCoordinator.

This module provides a drop-in replacement for direct adapter.send() calls.
When wired into gateway/delivery.py, it intercepts the send and:
1. Wraps the payload in an EmissionEnvelope
2. Routes through EgressCoordinator.emit()
3. Returns the platform result in the same format the caller expects

This allows progressive migration: existing code continues to call
adapter.send() but the call is transparently routed through the coordinator.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from .envelope import EmissionEnvelope, EnvelopeKind
from .coordinator import EgressCoordinator, DeliveryTarget, SendResult, get_coordinator

logger = logging.getLogger(__name__)


class CoordinatedAdapter:
    """Wraps a platform adapter, routing all .send() calls through the coordinator.
    
    Usage:
        original_adapter = get_platform_adapter("telegram")
        wrapped = CoordinatedAdapter(original_adapter, platform="telegram")
        result = await wrapped.send(chat_id, content, metadata={...})
    """

    def __init__(
        self,
        adapter: Any,
        platform: str,
        *,
        coordinator: Optional[EgressCoordinator] = None,
        default_kind: EnvelopeKind = EnvelopeKind.TURN_RESPONSE,
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        turn_id: Optional[str] = None,
    ):
        self._adapter = adapter
        self._platform = platform
        self._coordinator = coordinator or get_coordinator()
        self._default_kind = default_kind
        self._task_id = task_id
        self._session_id = session_id
        self._turn_id = turn_id

    # ── Delegate all non-send attributes to the original adapter ─────────

    def __getattr__(self, name: str):
        """Delegate attribute access to the original adapter."""
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._adapter, name)

    # ── Coordinated send ─────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        metadata: Optional[dict] = None,
        kind: Optional[EnvelopeKind] = None,
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        turn_id: Optional[str] = None,
    ) -> Any:
        """Send a message through the coordinator.
        
        Auto-detects envelope kind from metadata context:
        - cron job → CRON_DELIVERY
        - notify_on_complete → BACKGROUND_NOTIFY
        - stream → STREAM_CHUNK
        - default → TURN_RESPONSE
        
        Falls back to direct adapter.send() if outbox write fails
        (delivery must not be blocked by persistence errors).
        """
        meta = metadata or {}
        detected_kind = kind or self._detect_kind(meta)
        
        envelope = EmissionEnvelope(
            kind=detected_kind,
            task_id=task_id or self._task_id or meta.get("task_id"),
            session_id=session_id or self._session_id or meta.get("session_id"),
            turn_id=turn_id or self._turn_id or meta.get("turn_id"),
            payload=content,
            metadata=meta,
        )

        target = DeliveryTarget(
            platform=self._platform,
            chat_id=str(chat_id),
            thread_id=meta.get("thread_id"),
            metadata=meta,
        )

        result = await self._coordinator.emit(envelope, target, self._adapter)

        # If coordinator failed, return the error — do NOT fall back to raw send.
        # Raw sends bypass the outbox, envelope, allowlist, and all governance.
        if result.success:
            return {"ok": True, "message_id": result.message_id}
        else:
            import logging
            logging.getLogger(__name__).error(
                "CoordinatedAdapter.send() failed: envelope=%s error=%s",
                envelope.envelope_id, result.error,
            )
            return {"ok": False, "error": result.error}

    @staticmethod
    def _detect_kind(metadata: dict) -> EnvelopeKind:
        """Detect envelope kind from delivery context metadata."""
        if metadata.get("cron_job_id") or metadata.get("is_cron"):
            return EnvelopeKind.CRON_DELIVERY
        if metadata.get("notify_on_complete") or metadata.get("is_background"):
            return EnvelopeKind.BACKGROUND_NOTIFY
        if metadata.get("is_stream") or metadata.get("stream_chunk"):
            return EnvelopeKind.STREAM_CHUNK
        if metadata.get("is_warning") or metadata.get("is_tool"):
            return EnvelopeKind.WARNING
        return EnvelopeKind.TURN_RESPONSE

    async def send_image(self, chat_id: str, image_path: str, *, metadata=None, **kwargs) -> Any:
        """Send an image through the coordinator (envelope-wrapped)."""
        # Create envelope with image path as payload metadata
        envelope = EmissionEnvelope(
            kind=EnvelopeKind.MEDIA,
            task_id=self._task_id,
            session_id=self._session_id,
            turn_id=self._turn_id,
            payload=f"[MEDIA: {image_path}]",
            metadata={"media_type": "image", "file_path": image_path, **(metadata or {})},
        )
        target = DeliveryTarget(platform=self._platform, chat_id=str(chat_id))
        result = await self._coordinator.emit(envelope, target, self._adapter)
        if result.success:
            return await self._adapter.send_image(chat_id, image_path, metadata=metadata, **kwargs)
        return {"ok": False, "error": result.error}

    async def send_image_file(self, chat_id: str, file_path: str, *, metadata=None, **kwargs) -> Any:
        envelope = EmissionEnvelope(
            kind=EnvelopeKind.MEDIA, task_id=self._task_id,
            session_id=self._session_id, turn_id=self._turn_id,
            payload=f"[MEDIA_FILE: {file_path}]",
            metadata={"media_type": "image_file", "file_path": file_path, **(metadata or {})},
        )
        target = DeliveryTarget(platform=self._platform, chat_id=str(chat_id))
        result = await self._coordinator.emit(envelope, target, self._adapter)
        if result.success:
            return await self._adapter.send_image_file(chat_id, file_path, metadata=metadata, **kwargs)
        return {"ok": False, "error": result.error}

    async def send_document(self, chat_id: str, file_path: str, *, metadata=None, **kwargs) -> Any:
        envelope = EmissionEnvelope(
            kind=EnvelopeKind.MEDIA, task_id=self._task_id,
            session_id=self._session_id, turn_id=self._turn_id,
            payload=f"[DOCUMENT: {file_path}]",
            metadata={"media_type": "document", "file_path": file_path, **(metadata or {})},
        )
        target = DeliveryTarget(platform=self._platform, chat_id=str(chat_id))
        result = await self._coordinator.emit(envelope, target, self._adapter)
        if result.success:
            return await self._adapter.send_document(chat_id, file_path, metadata=metadata, **kwargs)
        return {"ok": False, "error": result.error}

    async def send_video(self, chat_id: str, file_path: str, *, metadata=None, **kwargs) -> Any:
        envelope = EmissionEnvelope(
            kind=EnvelopeKind.MEDIA, task_id=self._task_id,
            session_id=self._session_id, turn_id=self._turn_id,
            payload=f"[VIDEO: {file_path}]",
            metadata={"media_type": "video", "file_path": file_path, **(metadata or {})},
        )
        target = DeliveryTarget(platform=self._platform, chat_id=str(chat_id))
        result = await self._coordinator.emit(envelope, target, self._adapter)
        if result.success:
            return await self._adapter.send_video(chat_id, file_path, metadata=metadata, **kwargs)
        return {"ok": False, "error": result.error}

    async def send_voice(self, chat_id: str, file_path: str, *, metadata=None, **kwargs) -> Any:
        envelope = EmissionEnvelope(
            kind=EnvelopeKind.MEDIA, task_id=self._task_id,
            session_id=self._session_id, turn_id=self._turn_id,
            payload=f"[VOICE: {file_path}]",
            metadata={"media_type": "voice", "file_path": file_path, **(metadata or {})},
        )
        target = DeliveryTarget(platform=self._platform, chat_id=str(chat_id))
        result = await self._coordinator.emit(envelope, target, self._adapter)
        if result.success:
            return await self._adapter.send_voice(chat_id, file_path, metadata=metadata, **kwargs)
        return {"ok": False, "error": result.error}

    async def send_typing(self, chat_id: str, *, metadata=None, **kwargs) -> Any:
        return await self._adapter.send_typing(chat_id, metadata=metadata, **kwargs)

    # ── Context setters ──────────────────────────────────────────────────

    def set_context(
        self,
        task_id: Optional[str] = None,
        session_id: Optional[str] = None,
        turn_id: Optional[str] = None,
        kind: Optional[EnvelopeKind] = None,
    ):
        """Update the delivery context for subsequent sends."""
        if task_id is not None:
            self._task_id = task_id
        if session_id is not None:
            self._session_id = session_id
        if turn_id is not None:
            self._turn_id = turn_id
        if kind is not None:
            self._default_kind = kind
