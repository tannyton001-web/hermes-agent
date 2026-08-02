#!/usr/bin/env python3
"""
Desktop Canary Delivery — canonical SQLite message persistence.

Writes directly to the Hermes messages table (~/.hermes/state.db) using
the exact same SQLite schema as the web_server session API. The Desktop
renderer reads from this table via /api/sessions/{id}/messages.
"""
import json
import time
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("hermes.desktop_canary")

# ── Schema from hermes_state.py ───────────────────────────────────────
# messages table: id, session_id, role, content, timestamp, active, display_kind, display_metadata

def deliver_as_canary_message(
    session_id: str,
    content: str,
    task_id: str = "CANARY",
    *,
    db_path: Optional[Path] = None,
) -> dict:
    """Insert a canary message into the canonical Hermes messages table.
    
    Uses HermesState._insert_message_rows() for canonical persistence.
    The desktop renderer fetches messages via /api/sessions/{id}/messages
    and will display this as a normal assistant message.
    
    Returns: {message_id, session_id, timestamp, persisted}
    """
    if db_path is None:
        db_path = Path.home() / ".hermes" / "state.db"
    
    ts = time.time()
    msg = {
        "role": "assistant",
        "content": content,
        "timestamp": ts,
        "display_kind": "canary",
        "display_metadata": json.dumps({
            "source": "outbox_watcher",
            "task_id": task_id,
            "delivery_method": "canonical_persistence",
            "delivered_at": ts,
        }),
    }
    
    # Use HermesState's canonical insert path
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        
        # Insert into messages table
        cursor = conn.execute(
            """INSERT INTO messages 
               (session_id, role, content, timestamp, active, display_kind, display_metadata)
               VALUES (?, ?, ?, ?, 1, ?, ?)""",
            (session_id, msg["role"], msg["content"], ts,
             msg.get("display_kind"), msg.get("display_metadata")),
        )
        conn.commit()
        message_id = cursor.lastrowid
        
        # Update FTS index
        try:
            conn.execute(
                "INSERT INTO messages_fts(rowid, content) VALUES (?, ?)",
                (message_id, content),
            )
            conn.commit()
        except Exception:
            pass  # FTS may not exist or may be virtual table
        
        conn.close()
        
        return {
            "message_id": message_id,
            "session_id": session_id,
            "timestamp": ts,
            "persisted": True,
            "content_preview": content[:80],
        }
    except sqlite3.Error as e:
        logger.error("SQLite error delivering canary: %s", e, exc_info=True)
        return {
            "message_id": None,
            "session_id": session_id,
            "timestamp": ts,
            "persisted": False,
            "error": f"SQLite error: {e}",
        }
    except Exception as e:
        logger.error("Failed to deliver canary: %s", e, exc_info=True)
        return {
            "message_id": None,
            "session_id": session_id,
            "timestamp": ts,
            "persisted": False,
            "error": str(e),
        }


def find_active_session() -> Optional[str]:
    """Find the currently active desktop session ID."""
    db_path = Path.home() / ".hermes" / "state.db"
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.execute(
            "SELECT id FROM sessions WHERE archived=0 ORDER BY rowid DESC LIMIT 1"
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.error(f"Failed to find active session: {e}")
        return None
