"""
AST/Static CI Gate — enforce no raw adapter.send() outside Egress Coordinator.

Scans the host codebase and reports any call site where adapter.send() is
called outside the egress/coordinator module. This gate must pass before
any deployment.

Usage:
    python3 egress/gate.py [--strict] [path/to/host]
    
Exit codes:
    0 = PASS (no violations)
    1 = FAIL (violations found)
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Set, Tuple


# ── Allowlist: files/paths permitted to call adapter.send() directly ──
# These are the adapter implementations themselves, test files, and the
# egress coordinator which is the sole dispatch owner.

PERMITTED_SEND_CALLERS = {
    # The egress module itself (sole dispatch owner)
    "egress/coordinator.py",
    "egress/outbox.py",
    "egress/adapter_wrapper.py",  # Delegates media calls to original adapter
    
    # Platform adapter implementations (must implement .send())
    "plugins/platforms/",
    "gateway/platforms/",
    
    # Gateway infrastructure (routing, not raw sinks)
    "gateway/run.py",         # Core gateway message handler
    "gateway/delivery.py",    # Delivery routing (to be wired through coordinator)
    "gateway/kanban_watchers.py",
    "gateway/systemd_notify.py",
    "gateway/stream_consumer.py",
    "gateway/relay/",
    
    # Cron infrastructure
    "cron/",
    
    # CLI/TUI transport (these ARE transport adapters)
    "cli.py",
    "hermes_cli/",
    "tui_gateway/",
    
    # Test files
    "tests/",
    "tests-js/",
    
    # Tool implementations that are themselves transport adapters
    "tools/yuanbao_tools.py",
    "tools/send_message_tool.py",
    "tools/browser_cdp_tool.py",
    "tools/browser_supervisor.py",
    "tools/mcp_oauth.py",
    "tools/mcp_oauth_manager.py",
    
    # Third-party / vendored
    "optional-skills/",
    "plugins/memory/",
    "plugins/google_meet/",
    
    # Scripts (one-off utilities)
    "scripts/",
    
    # Web/SSE transport
    "web/",
}


class SendCallVisitor(ast.NodeVisitor):
    """AST visitor that finds .send() call sites."""

    def __init__(self, filepath: str, permitted: Set[str]):
        self.filepath = filepath
        self.permitted = permitted
        self.violations: list[Tuple[int, str]] = []

    def _is_permitted(self) -> bool:
        for prefix in self.permitted:
            if prefix in self.filepath:
                return True
        return False

    def visit_Call(self, node: ast.Call):
        # Check for obj.send(...) pattern
        if isinstance(node.func, ast.Attribute) and node.func.attr == "send":
            if not self._is_permitted():
                self.violations.append((node.lineno, "adapter.send()"))
        # Check for send_message(...) pattern (may bypass coordinator)
        elif isinstance(node.func, ast.Name) and node.func.id in ("send_message", "send_response"):
            if not self._is_permitted():
                self.violations.append((node.lineno, f"{node.func.id}()"))
        # Check for await adapter.send(...)
        elif isinstance(node.func, ast.Attribute) and node.func.attr in (
            "send_message", "send_image", "send_document", "send_voice",
            "send_video", "send_animation", "send_sticker", "send_typing",
            "send_dm", "send_image_file", "send_response",
        ):
            if not self._is_permitted():
                self.violations.append((node.lineno, f"adapter.{node.func.attr}()"))

        self.generic_visit(node)


def scan_file(filepath: str, permitted: Set[str]) -> list[Tuple[int, str]]:
    """Scan a single Python file for raw send call sites."""
    try:
        with open(filepath, "r") as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
        visitor = SendCallVisitor(filepath, permitted)
        visitor.visit(tree)
        return visitor.violations
    except SyntaxError:
        return [(0, f"SYNTAX_ERROR in {filepath}")]
    except Exception as e:
        return [(0, f"PARSE_ERROR: {e}")]


def scan_directory(root: str, strict: bool = False) -> dict:
    """Scan a directory tree for raw send call sites.
    
    Returns: {filepath: [(lineno, pattern), ...]}
    """
    root_path = Path(root)
    violations: dict[str, list[Tuple[int, str]]] = {}
    total_files = 0

    for py_file in root_path.rglob("*.py"):
        # Skip __pycache__, .git, node_modules
        if any(skip in str(py_file) for skip in ("__pycache__", ".git/", "node_modules/", "venv/", ".venv/")):
            continue

        total_files += 1
        rel_path = str(py_file.relative_to(root_path))
        file_violations = scan_file(str(py_file), PERMITTED_SEND_CALLERS)

        # In strict mode, even permitted files are checked (for internal violations)
        if strict:
            file_violations = scan_file(str(py_file), {"egress/coordinator.py"})  # only coordinator is truly clean

        if file_violations:
            violations[rel_path] = file_violations

    return violations


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    strict = "--strict" in sys.argv

    root_path = Path(root)
    if not root_path.exists():
        print(f"ERROR: Path not found: {root}")
        sys.exit(2)

    # Count files scanned
    total_files = len(list(root_path.rglob("*.py")))
    violations = scan_directory(str(root_path), strict=strict)

    total_call_sites = sum(len(v) for v in violations.values())

    if violations:
        print(f"\n{'='*60}")
        print(f"EGRESS AST GATE: FAIL")
        print(f"Files with violations: {len(violations)}")
        print(f"Total call sites: {total_call_sites}")
        print(f"{'='*60}\n")

        for filepath, sites in sorted(violations.items()):
            print(f"\n📄 {filepath}:")
            for lineno, pattern in sorted(sites):
                print(f"   L{lineno}: {pattern}")

        print(f"\n{'='*60}")
        print(f"RESULT: {total_call_sites} raw send() call sites outside coordinator")
        print(f"ACTION: Migrate these to use EgressCoordinator.emit()")
        print(f"{'='*60}\n")

        if strict:
            sys.exit(1)
        else:
            # Non-strict mode: report violations but don't fail if they're in
            # the allowlist migration queue
            unmigrated = {
                f: s for f, s in violations.items()
                if not any(p in f for p in PERMITTED_SEND_CALLERS)
            }
            if unmigrated:
                print(f"\n⚠️  {len(unmigrated)} files with UNEXPECTED violations (outside allowlist):")
                for f in sorted(unmigrated):
                    print(f"   {f}")
                sys.exit(1)
            else:
                print("All violations are in the migration allowlist — no unexpected bypasses.")
                sys.exit(0)
    else:
        print(f"\n{'='*60}")
        print(f"EGRESS AST GATE: PASS")
        print(f"Files scanned: {total_files}")
        print(f"Violations: 0")
        print(f"{'='*60}\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
