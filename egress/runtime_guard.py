"""
Runtime assertions for egress integrity.

Monkey-patches adapter.send() at import time to log warnings when called
outside the Egress Coordinator. In strict mode, raises RuntimeError.

Usage:
    # In gateway startup:
    from egress.runtime_guard import install_guard
    install_guard(strict=False)  # warn only
    install_guard(strict=True)   # raise on violation
"""
from __future__ import annotations

import functools
import inspect
import logging
import os
import threading

logger = logging.getLogger(__name__)

_guard_installed = False
_guard_lock = threading.Lock()
_strict_mode = False

# Track which adapter classes have been wrapped
_wrapped_classes: set[int] = set()


def _is_coordinator_frame() -> bool:
    """Check if the caller is within the egress coordinator module."""
    for frame_info in inspect.stack():
        filename = frame_info.filename
        if "egress/coordinator.py" in filename:
            return True
        # Stop at the adapter layer - if we haven't found coordinator by now,
        # it's a raw call
        if "plugins/platforms/" in filename or "gateway/platforms/" in filename:
            if "adapter.py" in filename:
                return False  # This is the adapter's own send() being called
    return False


def _warn_raw_send(adapter_class_name: str, caller_file: str, caller_line: int):
    """Log a warning when adapter.send() is called outside the coordinator."""
    msg = (
        f"EGRESS_GUARD: Raw adapter.send() call detected — "
        f"adapter={adapter_class_name}, caller={caller_file}:{caller_line}"
    )
    if _strict_mode:
        raise RuntimeError(
            f"{msg}\n"
            f"All outbound messages must go through EgressCoordinator.emit(). "
            f"Set EGRESS_GUARD_STRICT=0 to log warnings instead."
        )
    else:
        logger.warning(msg)


def _make_guarded_send(original_send, adapter_class_name: str):
    """Wrap adapter.send() with a guard that checks for coordinator bypass."""

    @functools.wraps(original_send)
    async def guarded_send(self, chat_id, content, metadata=None, **kwargs):
        # Check if caller is outside the coordinator
        if not _is_coordinator_frame():
            # Find the actual caller
            frame = inspect.currentframe()
            caller_frame = frame.f_back.f_back if frame and frame.f_back else None
            caller_file = caller_frame.f_code.co_filename if caller_frame else "unknown"
            caller_line = caller_frame.f_lineno if caller_frame else 0
            _warn_raw_send(adapter_class_name, caller_file, caller_line)

        # Call the original send
        return await original_send(self, chat_id, content, metadata=metadata, **kwargs)

    return guarded_send


def install_guard(strict: bool = False):
    """Install the egress runtime guard globally.

    Monkey-patches platform adapter .send() methods to detect and warn
    about calls made outside the EgressCoordinator.

    Args:
        strict: If True, raise RuntimeError on violation instead of logging.
    """
    global _guard_installed, _strict_mode

    with _guard_lock:
        if _guard_installed:
            return

        _strict_mode = strict or os.environ.get("EGRESS_GUARD_STRICT", "0") == "1"
        _guard_installed = True

        # Import and patch adapters lazily
        try:
            _patch_known_adapters()
        except Exception as e:
            logger.warning(f"Failed to patch adapters: {e}")

        logger.info(
            f"Egress runtime guard installed (strict={_strict_mode})"
        )


def _patch_known_adapters():
    """Attempt to patch known platform adapter classes."""
    adapter_modules = [
        "plugins.platforms.telegram.adapter",
        "plugins.platforms.discord.adapter",
        "plugins.platforms.slack.adapter",
        "plugins.platforms.teams.adapter",
        "plugins.platforms.matrix.adapter",
        "plugins.platforms.mattermost.adapter",
        "plugins.platforms.photon.adapter",
        "plugins.platforms.wecom.adapter",
        "plugins.platforms.dingtalk.adapter",
        "plugins.platforms.feishu.adapter",
        "plugins.platforms.simplex.adapter",
        "plugins.platforms.email.adapter",
        "gateway.platforms.base",
    ]
    for mod_name in adapter_modules:
        try:
            import importlib
            mod = importlib.import_module(mod_name)
            for name in dir(mod):
                obj = getattr(mod, name)
                if isinstance(obj, type) and hasattr(obj, "send"):
                    original = getattr(obj, "send", None)
                    if original and not getattr(original, "_egress_guarded", False):
                        _patch_adapter_send(obj, original)
        except (ImportError, AttributeError):
            pass


def _patch_adapter_send(adapter_cls, original_send):
    """Patch a single adapter class's send() — factory avoids closure bug."""
    cls_name = adapter_cls.__name__

    import functools
    @functools.wraps(original_send)
    async def guarded_send(self, chat_id, content, metadata=None, **kwargs):
        if not _is_coordinator_frame():
            frame = __import__('inspect').currentframe()
            caller = frame.f_back.f_back if frame and frame.f_back else None
            cf = caller.f_code.co_filename if caller else "unknown"
            cl = caller.f_lineno if caller else 0
            _warn_raw_send(cls_name, cf, cl)
        return await original_send(self, chat_id, content, metadata=metadata, **kwargs)

    guarded_send._egress_guarded = True
    setattr(adapter_cls, "send", guarded_send)


def is_guard_installed() -> bool:
    return _guard_installed


def uninstall_guard():
    """Remove the egress runtime guard (for testing)."""
    global _guard_installed, _strict_mode
    with _guard_lock:
        _guard_installed = False
        _strict_mode = False
        _wrapped_classes.clear()
