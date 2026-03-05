from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from collections import deque

logger = logging.getLogger("multi_agen.mcp_manager")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass
class ManagedProcess:
    server_id: str
    cmd: List[str]
    base_url: str
    popen: subprocess.Popen[str]
    output_tail: deque[str]  # last N lines captured


def _parse_host_port(base_url: str) -> Tuple[str, int]:
    u = urlparse(base_url)
    host = u.hostname or "127.0.0.1"
    port = u.port or (443 if u.scheme == "https" else 80)
    return host, int(port)


def _can_connect(host: str, port: int, timeout: float = 0.25) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _normalize_cmd(cmd: List[str]) -> List[str]:
    """
    If cmd starts with python/python.exe, replace with the running interpreter.
    """
    if not cmd:
        return cmd
    first = cmd[0].lower()
    if first in ("python", "python.exe"):
        return [sys.executable] + cmd[1:]
    return cmd


def _drain_some_output(mp: ManagedProcess, max_lines: int = 200) -> None:
    """
    Drain whatever is already available without risking a long block.
    NOTE: On Windows pipes, readline() can block if there's no newline.
    We only call this AFTER the process exited or we are timing out, so it's OK.
    """
    if not mp.popen.stdout:
        return

    # Try reading a limited number of lines; if the process is dead, this won't hang long.
    for _ in range(max_lines):
        line = mp.popen.stdout.readline()
        if not line:
            break
        mp.output_tail.append(line.rstrip("\n"))


def _wait_for_port_or_exit(mp: ManagedProcess, timeout_seconds: float) -> None:
    host, port = _parse_host_port(mp.base_url)
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        rc = mp.popen.poll()
        if rc is not None:
            # process exited early — capture output tail and fail
            _drain_some_output(mp, max_lines=200)
            tail = "\n".join(mp.output_tail) or "(no output captured)"
            raise RuntimeError(
                f"MCP process '{mp.server_id}' exited early (code={rc}) while waiting for {mp.base_url}.\n"
                f"Command: {mp.cmd}\n"
                f"Last output:\n{tail}"
            )

        if _can_connect(host, port):
            return

        time.sleep(0.1)

    # timeout — capture output tail and fail
    _drain_some_output(mp, max_lines=200)
    tail = "\n".join(mp.output_tail) or "(no output captured)"
    raise RuntimeError(
        f"MCP server '{mp.server_id}' not reachable at {mp.base_url} after {timeout_seconds:.1f}s.\n"
        f"Command: {mp.cmd}\n"
        f"Last output:\n{tail}"
    )


class MCPManager:
    """
    Starts MCP servers sequentially (one-by-one) for clean logs.

    - Starts in the order defined by servers_cfg (YAML order preserved in Py3.7+ dicts)
    - Waits for each to become reachable before moving to the next
    - Logs a clear SUCCESS/FAIL block per server
    - allow_partial_start:
        * False -> raise and fail orchestrator startup if any MCP fails
        * True  -> keep going; orchestrator can start with missing MCP servers
    """

    def __init__(
        self,
        servers_cfg: Dict[str, dict],
        startup_timeout_seconds: float = 20.0,
        allow_partial_start: bool = False,
    ):
        self.servers_cfg = servers_cfg
        self.startup_timeout_seconds = float(startup_timeout_seconds)
        self.allow_partial_start = bool(allow_partial_start)
        self.procs: Dict[str, ManagedProcess] = {}
        self.failures: List[str] = []

    def start_all(self) -> None:
        logger.info("MCPManager using interpreter: %s", sys.executable)

        # Sequential startup for clean logs
        for sid, info in (self.servers_cfg or {}).items():
            if not isinstance(info, dict):
                continue

            base_url = info.get("base_url")
            cmd = info.get("cmd")

            logger.info("========== MCP START: %s ==========", sid)

            if not base_url:
                msg = f"{sid}: missing base_url"
                logger.error(msg)
                self.failures.append(msg)
                logger.info("========== MCP FAIL: %s ==========", sid)
                if not self.allow_partial_start:
                    raise RuntimeError(msg)
                continue

            # If server is unmanaged (no cmd), just check reachability and move on.
            if not cmd:
                if _can_connect(*_parse_host_port(base_url)):
                    logger.info("%s unmanaged but reachable at %s", sid, base_url)
                    logger.info("========== MCP OK: %s ==========", sid)
                else:
                    msg = f"{sid}: unmanaged and NOT reachable at {base_url}"
                    logger.error(msg)
                    self.failures.append(msg)
                    logger.info("========== MCP FAIL: %s ==========", sid)
                    if not self.allow_partial_start:
                        raise RuntimeError(msg)
                continue

            cmd = _normalize_cmd(list(cmd))

            # If already running externally, do not spawn.
            if _can_connect(*_parse_host_port(base_url)):
                logger.info("%s already reachable at %s (not spawning)", sid, base_url)
                logger.info("========== MCP OK: %s ==========", sid)
                continue

            # Spawn
            creationflags = 0
            if os.name == "nt":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

            logger.info("Starting %s cmd=%s", sid, cmd)

            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=creationflags,
            )

            mp = ManagedProcess(
                server_id=sid,
                cmd=cmd,
                base_url=base_url,
                popen=p,
                output_tail=deque(maxlen=200),
            )
            self.procs[sid] = mp

            # Wait for this server BEFORE moving to next
            try:
                logger.info("Waiting for %s at %s", sid, base_url)
                _wait_for_port_or_exit(mp, timeout_seconds=self.startup_timeout_seconds)
                logger.info("%s reachable at %s", sid, base_url)
                logger.info("========== MCP OK: %s ==========", sid)
            except Exception as e:
                err = str(e)
                logger.error("========== MCP FAIL: %s ==========", sid)
                logger.error("%s", err)
                self.failures.append(err)

                # stop this failed process (it may still be alive in a bad state)
                self._stop_one(sid)

                if not self.allow_partial_start:
                    # stop anything we already started
                    self.stop_all()
                    raise RuntimeError(
                        "One or more MCP servers failed to start:\n- " + "\n- ".join(self.failures)
                    )

        if self.failures:
            logger.warning(
                "MCP startup completed with failures (partial mode=%s):\n- %s",
                self.allow_partial_start,
                "\n- ".join(self.failures),
            )
        else:
            logger.info("MCP startup completed successfully (all servers reachable).")

    def _stop_one(self, sid: str) -> None:
        mp = self.procs.get(sid)
        if not mp:
            return
        p = mp.popen
        if p.poll() is not None:
            return
        try:
            p.terminate()
        except Exception:
            logger.exception("Failed to terminate MCP %s", sid)
        # brief wait then kill
        deadline = time.time() + 2.0
        while time.time() < deadline and p.poll() is None:
            time.sleep(0.05)
        if p.poll() is None:
            try:
                p.kill()
            except Exception:
                logger.exception("Failed to kill MCP %s", sid)

    def stop_all(self) -> None:
        for sid in list(self.procs.keys()):
            self._stop_one(sid)
        self.procs.clear()