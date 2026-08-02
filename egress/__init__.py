"""
Hermes Egress Module — Host-native delivery integrity.

This module provides:
- EmissionEnvelope: typed container for all outbound messages
- EgressCoordinator: sole send owner with policy validation
- OutboxStore: transactional durable outbox (write-before-dispatch)
- DeliveryState: FSM for message lifecycle tracking

All user-visible message delivery MUST go through EgressCoordinator.emit().
Direct adapter.send() calls outside this module are prohibited (enforced by
AST gate and runtime assertions).
"""
from .envelope import EmissionEnvelope, EnvelopeKind
from .coordinator import EgressCoordinator, DeliveryTarget, SendResult, get_coordinator, reset_coordinator
from .outbox import OutboxStore, OutboxEntry, DeliveryState

__all__ = [
    "EmissionEnvelope",
    "EnvelopeKind",
    "EgressCoordinator",
    "DeliveryTarget",
    "SendResult",
    "OutboxStore",
    "OutboxEntry",
    "DeliveryState",
    "get_coordinator",
    "reset_coordinator",
]
