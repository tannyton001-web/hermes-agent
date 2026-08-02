"""
Tests for the Egress module — envelope, coordinator, outbox, gate.

Run with:
    python3 -m pytest egress/tests/ -v
or:
    python3 egress/tests/test_egress.py
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import sys
from pathlib import Path

# Ensure the host dir is on the path
_host_dir = Path(__file__).resolve().parents[2]
if str(_host_dir) not in sys.path:
    sys.path.insert(0, str(_host_dir))

from egress.envelope import EmissionEnvelope, EnvelopeKind
from egress.outbox import OutboxStore, OutboxEntry, DeliveryState
from egress.coordinator import EgressCoordinator, DeliveryTarget, SendResult, get_coordinator, reset_coordinator


class TestEmissionEnvelope(unittest.TestCase):
    """Test envelope creation, hashing, and serialization."""

    def test_envelope_creates_with_sha256(self):
        env = EmissionEnvelope(
            kind=EnvelopeKind.TURN_RESPONSE,
            task_id="TASK-1",
            session_id="sess-1",
            turn_id="turn-1",
            payload="Hello world",
        )
        self.assertEqual(env.kind, EnvelopeKind.TURN_RESPONSE)
        self.assertEqual(env.task_id, "TASK-1")
        self.assertEqual(env.payload, "Hello world")
        self.assertTrue(env.message_sha256)
        self.assertEqual(len(env.message_sha256), 64)

    def test_envelope_sha256_is_deterministic(self):
        env1 = EmissionEnvelope(
            kind=EnvelopeKind.TURN_RESPONSE,
            task_id="TASK-1",
            session_id="sess-1",
            turn_id="turn-1",
            payload="Hello",
        )
        env2 = EmissionEnvelope(
            kind=EnvelopeKind.TURN_RESPONSE,
            task_id="TASK-1",
            session_id="sess-1",
            turn_id="turn-1",
            payload="Hello",
        )
        self.assertEqual(env1.message_sha256, env2.message_sha256)

    def test_envelope_sha256_differs_on_content(self):
        env1 = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1", turn_id="t-1", payload="A")
        env2 = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1", turn_id="t-1", payload="B")
        self.assertNotEqual(env1.message_sha256, env2.message_sha256)

    def test_idempotency_key(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1", turn_id="t-1", payload="X")
        key = env.idempotency_key
        self.assertIn("T-1", key)
        self.assertIn("s-1", key)
        self.assertIn("t-1", key)
        self.assertIn(env.message_sha256[:16], key)

    def test_envelope_roundtrip(self):
        env = EmissionEnvelope(kind=EnvelopeKind.PROACTIVE_NOTIFICATION, task_id="T-2", session_id="s-2",
                               turn_id="t-2", payload="Notify!", metadata={"priority": "high"})
        d = env.to_dict()
        env2 = EmissionEnvelope.from_dict(d)
        self.assertEqual(env.kind, env2.kind)
        self.assertEqual(env.task_id, env2.task_id)
        self.assertEqual(env.payload, env2.payload)
        self.assertEqual(env.message_sha256, env2.message_sha256)

    def test_envelope_kinds(self):
        kinds = list(EnvelopeKind)
        self.assertIn(EnvelopeKind.TURN_RESPONSE, kinds)
        self.assertIn(EnvelopeKind.STREAM_CHUNK, kinds)
        self.assertIn(EnvelopeKind.CRON_DELIVERY, kinds)
        self.assertGreaterEqual(len(kinds), 10)


class TestOutboxStore(unittest.TestCase):
    """Test transactional outbox operations."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = OutboxStore(base_dir=Path(self.tmp))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_write_and_read_entry(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Test")
        entry = OutboxEntry(envelope=env, state=DeliveryState.PENDING)
        self.assertTrue(self.store.write_entry(entry))

        read = self.store.read_entry(env.envelope_id)
        self.assertIsNotNone(read)
        self.assertEqual(read.state, DeliveryState.PENDING)
        self.assertEqual(read.envelope.payload, "Test")

    def test_update_state(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Test")
        entry = OutboxEntry(envelope=env, state=DeliveryState.PENDING)
        self.store.write_entry(entry)

        self.assertTrue(self.store.update_state(env.envelope_id, DeliveryState.DELIVERED))
        read = self.store.read_entry(env.envelope_id)
        self.assertEqual(read.state, DeliveryState.DELIVERED)

    def test_list_pending(self):
        for i in range(3):
            env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id=f"T-{i}", session_id="s",
                                   turn_id=f"t-{i}", payload=f"Msg {i}")
            entry = OutboxEntry(envelope=env, state=DeliveryState.PENDING)
            self.store.write_entry(entry)

        pending = self.store.list_pending()
        self.assertEqual(len(pending), 3)

    def test_idempotency_find(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Unique")
        entry = OutboxEntry(envelope=env, state=DeliveryState.DELIVERED)
        self.store.write_entry(entry)

        found = self.store.find_by_idempotency_key(env.idempotency_key)
        self.assertIsNotNone(found)
        self.assertEqual(found.state, DeliveryState.DELIVERED)

    def test_idempotency_not_found(self):
        found = self.store.find_by_idempotency_key("nonexistent:key:here:abc123")
        self.assertIsNone(found)

    def test_cleanup_delivered(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Old")
        entry = OutboxEntry(envelope=env, state=DeliveryState.DELIVERED)
        self.store.write_entry(entry)

        removed = self.store.cleanup_delivered(older_than_hours=0)  # clean all
        self.assertGreaterEqual(removed, 1)

        found = self.store.read_entry(env.envelope_id)
        self.assertIsNone(found)

    def test_read_nonexistent(self):
        self.assertIsNone(self.store.read_entry("nonexistent-id"))


# ── Test helper classes for allowlist MRO ──────────────────────────────
class BasePlatformAdapter:
    """Fake BasePlatformAdapter for test MRO — must match ALLOWED_ADAPTER_BASES."""
    __module__ = "gateway.platforms"


class TestEgressCoordinator(unittest.TestCase):
    """Test the Egress Coordinator flow."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = OutboxStore(base_dir=Path(self.tmp))
        self.coord = EgressCoordinator(outbox_store=self.store)
        reset_coordinator()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)
        reset_coordinator()

    def _make_mock_adapter(self, should_succeed=True, message_id="msg-123"):
        # Dynamically create a class with the correct module for allowlist
        async def _mock_send(s, chat_id, content, metadata=None):
            return {"ok": should_succeed, "message_id": message_id} if should_succeed else {"ok": False, "error": "Test error"}

        MockAdapter = type("MockAdapter", (BasePlatformAdapter,), {
            "__module__": "plugins.platforms.telegram.adapter",
            "send": _mock_send,
        })
        return MockAdapter()

    async def _async_test_emit_success(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Hello")
        target = DeliveryTarget(platform="telegram", chat_id="12345")
        adapter = self._make_mock_adapter(should_succeed=True)

        result = await self.coord.emit(env, target, adapter)
        return result

    def test_emit_success(self):
        import asyncio
        result = asyncio.run(self._async_test_emit_success())
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "msg-123")
        self.assertEqual(result.platform, "telegram")

    async def _async_test_emit_failure(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Hello")
        target = DeliveryTarget(platform="telegram", chat_id="12345")
        adapter = self._make_mock_adapter(should_succeed=False)

        result = await self.coord.emit(env, target, adapter)
        return result

    def test_emit_failure(self):
        import asyncio
        result = asyncio.run(self._async_test_emit_failure())
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)

    async def _async_test_idempotent_delivery(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="Hello")
        target = DeliveryTarget(platform="telegram", chat_id="12345")
        adapter = self._make_mock_adapter(should_succeed=True)

        # First delivery
        result1 = await self.coord.emit(env, target, adapter)
        # Second delivery (idempotent skip)
        result2 = await self.coord.emit(env, target, adapter, idempotent=True)
        return result1, result2

    def test_idempotent_delivery(self):
        import asyncio
        r1, r2 = asyncio.run(self._async_test_idempotent_delivery())
        self.assertTrue(r1.success)
        self.assertTrue(r2.success)
        # Second call should be a duplicate
        self.assertEqual(self.coord.stats["duplicates"], 1)

    def test_validate_envelope_empty_payload(self):
        env = EmissionEnvelope(kind=EnvelopeKind.TURN_RESPONSE, task_id="T-1", session_id="s-1",
                               turn_id="t-1", payload="")
        # Override the sha256 since empty payload still hashes
        env.message_sha256 = ""
        self.assertFalse(self.coord._validate_envelope(env))

    def test_get_coordinator_singleton(self):
        c1 = get_coordinator()
        c2 = get_coordinator()
        self.assertIs(c1, c2)
        reset_coordinator()
        c3 = get_coordinator()
        self.assertIsNot(c1, c3)

    def test_stats_tracking(self):
        self.assertEqual(self.coord.stats["total"], 0)
        self.assertEqual(self.coord.stats["success"], 0)
        self.assertEqual(self.coord.stats["failed"], 0)


class TestDeliveryTarget(unittest.TestCase):
    def test_target_creation(self):
        t = DeliveryTarget(platform="telegram", chat_id="12345", thread_id="67890")
        self.assertEqual(t.platform, "telegram")
        self.assertEqual(t.chat_id, "12345")
        self.assertEqual(t.thread_id, "67890")


if __name__ == "__main__":
    unittest.main(verbosity=2)
