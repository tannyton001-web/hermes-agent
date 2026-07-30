"""Integration test: real parent-child handoff & duplicate updater detection.

Simulates:
  1. Tauri parent writing marker → Python child with HANDOFF_PID_ENV (new binary)
  2. Tauri parent writing marker → Python child without HANDOFF (old binary, getppid fallback)
  3. Foreign updater writing marker → our process refuses (unit test covers this)
  4. No marker at all → normal acquire
  5. Stale/dead PID marker → reclaimed
"""
import os, sys, time, tempfile, subprocess, json
from pathlib import Path

HANDOFF_PID_ENV = "HERMES_UPDATE_HANDOFF_PID"
REPO_ROOT = Path(__file__).resolve().parent

def _build_child_script(marker_path):
    """Return a string of Python code that tries to acquire the lock."""
    return (
        "import os, sys, json\n"
        "from pathlib import Path\n"
        f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
        "from hermes_cli.update_lock import UpdateLock\n"
        f"marker_path = Path({str(marker_path)!r})\n"
        "lock = UpdateLock(path=marker_path)\n"
        "ok = lock.acquire()\n"
        "result = dict(\n"
        "    pid=os.getpid(),\n"
        "    ppid=os.getppid(),\n"
        "    handoff_pid_env=os.environ.get('HERMES_UPDATE_HANDOFF_PID', None),\n"
        "    acquired=ok,\n"
        "    acquired_flag=lock.acquired,\n"
        "    holder_pid=lock.holder.pid if lock.holder else None,\n"
        ")\n"
        "if ok:\n"
        "    lock.release()\n"
        'print("RESULT:" + json.dumps(result))\n'
    )

def run_child(marker_path, set_handoff_env=False, env_handoff_pid=None):
    """Spawn a child that tries to acquire the lock on marker_path."""
    env = os.environ.copy()
    env.pop('PYTHONPATH', None)
    if set_handoff_env and env_handoff_pid is not None:
        env[HANDOFF_PID_ENV] = str(env_handoff_pid)

    script = _build_child_script(marker_path)
    r = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True, text=True, timeout=15, env=env,
    )
    if r.returncode != 0:
        return {'error': f'exit={r.returncode} stderr={r.stderr[:500]}'}
    # Parse "RESULT:{json}" line
    for line in r.stdout.strip().split('\n'):
        if line.startswith('RESULT:'):
            try:
                return json.loads(line[len('RESULT:'):])
            except json.JSONDecodeError:
                pass
    return {'error': f'no RESULT line: stdout={r.stdout[:500]}'}

def test(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {name}")
    if detail:
        print(f"         {detail}")
    return status == "PASS"

def main():
    passed = 0
    failed = 0

    print("=" * 70)
    print("INTEGRATION TEST: update_lock parent-child handoff")
    print(f"  PID: {os.getpid()}  PPID: {os.getppid()}")
    print("=" * 70)

    with tempfile.TemporaryDirectory(prefix="hermes-inttest-") as tmp:
        marker = Path(tmp) / ".hermes-update-in-progress"
        now = int(time.time())
        our_pid = os.getpid()

        # ── Test 1: Old Tauri binary (no HANDOFF) → getppid() fallback ──
        print("\n── Test 1: Old Tauri binary (no HANDOFF env) → getppid() fallback ──")
        marker.write_text(f"{our_pid}\n{now}\n", encoding="utf-8")

        child = run_child(marker, set_handoff_env=False)
        t1 = test("Child allowed under parent's lock (getppid fallback)",
                  child.get('acquired') is True and child.get('acquired_flag') is False,
                  f"child ppid={child.get('ppid')}, holder_pid={child.get('holder_pid')}")
        t2 = test("Marker preserved after child release",
                  marker.exists(),
                  f"marker exists={marker.exists()}")
        passed += t1 + t2
        failed += (not t1) + (not t2)

        # ── Test 2: New Tauri binary (with HANDOFF_PID env) ──
        print("\n── Test 2: New Tauri binary (HANDOFF_PID_ENV set) ──")
        marker.write_text(f"{our_pid}\n{now}\n", encoding="utf-8")

        child2 = run_child(marker, set_handoff_env=True, env_handoff_pid=our_pid)
        t3 = test("Child allowed under parent's lock (HANDOFF_PID env)",
                  child2.get('acquired') is True and child2.get('acquired_flag') is False,
                  f"child={ {k:v for k,v in child2.items() if k!='error'} }")
        passed += t3
        failed += not t3

        # ── Test 3: No marker → normal acquire ──
        print("\n── Test 3: No marker (clean state) → normal acquire ──")
        if marker.exists():
            marker.unlink()

        child3 = run_child(marker, set_handoff_env=False)
        t4 = test("Child acquires lock when no marker exists",
                  child3.get('acquired') is True and child3.get('acquired_flag') is True,
                  f"acquired_flag={child3.get('acquired_flag')}")
        t5 = test("Marker cleaned up after release",
                  not marker.exists(),
                  f"marker exists after release={marker.exists()}")
        passed += t4 + t5
        failed += (not t4) + (not t5)

        # ── Test 4: Stale/dead PID marker → reclaimed ──
        print("\n── Test 4: Stale/dead PID marker → reclaimed ──")
        marker.write_text(f"99999999\n{now}\n", encoding="utf-8")

        child4 = run_child(marker, set_handoff_env=False)
        t6 = test("Stale marker reclaimed (acquire succeeds)",
                  child4.get('acquired') is True,
                  f"child={ {k:v for k,v in child4.items() if k!='error'} }")
        passed += t6
        failed += not t6

        # ── Test 5: Duplicate updater (both children allowed under parent's marker) ──
        print("\n── Test 5: Duplicate children under same parent marker ──")
        marker.write_text(f"{our_pid}\n{now}\n", encoding="utf-8")

        child5a = run_child(marker, set_handoff_env=False)
        child5b = run_child(marker, set_handoff_env=False)
        t7 = test("First child allowed",
                  child5a.get('acquired') is True,
                  f"child5a={ {k:v for k,v in child5a.items() if k!='error'} }")
        t8 = test("Second child also allowed (same parent)",
                  child5b.get('acquired') is True,
                  f"child5b={ {k:v for k,v in child5b.items() if k!='error'} }")
        t9 = test("Marker still intact after both children finish",
                  marker.exists() and int(marker.read_text().splitlines()[0]) == our_pid,
                  "parent still owns the marker")
        passed += t7 + t8 + t9
        failed += (not t7) + (not t8) + (not t9)

    # ── Summary ──
    print("\n" + "=" * 70)
    print(f"  TOTAL: {passed} passed, {failed} failed")
    print("=" * 70)
    if failed > 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
