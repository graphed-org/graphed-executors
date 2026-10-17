"""Scratch (#30): py-spy dumps of the driver and every worker once a test runs past HANG_S."""

import os
import subprocess
import threading

import psutil
import pytest

HANG_S = float(os.environ.get("HANG_S", "60"))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    done = threading.Event()

    def watch():
        if done.wait(HANG_S):
            return
        me = psutil.Process()
        for p in [me, *me.children(recursive=True)]:
            try:
                head = f"{p.pid} {p.name()} {' '.join(p.cmdline())[:200]}"
            except psutil.Error as exc:
                head = f"{p.pid} ({exc})"
            out = subprocess.run(
                ["py-spy", "dump", "--pid", str(p.pid)], capture_output=True, text=True, timeout=120
            )
            with open("hang-dumps.txt", "a") as dumps:
                dumps.write(f"=== HANG {item.nodeid} py-spy {head}\n{out.stdout}{out.stderr}\n")

    threading.Thread(target=watch, daemon=True).start()
    try:
        yield
    finally:
        done.set()
