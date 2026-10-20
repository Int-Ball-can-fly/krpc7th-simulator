#!/usr/bin/env python3
"""Local HTTP manager for isolated Kibo-RPC simulator runs."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import sqlite3
import tarfile
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

from aiohttp import ClientSession, WSMsgType, web


logger = logging.getLogger("simulator_manager")
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("SIM_MANAGER_DATA_DIR", "/mnt/data/simulator-manager")).resolve()
JOBS_DIR = DATA_DIR / "jobs"
DB_PATH = DATA_DIR / "state.sqlite3"
COMPOSE_FILE = ROOT / "docker-compose.yaml"
MAX_UPLOAD_BYTES = max(1, int(os.environ.get("SIM_UPLOAD_MAX_MB", "512"))) * 1024 * 1024
MAX_ARCHIVE_BYTES = max(1, int(os.environ.get("SIM_ARCHIVE_MAX_MB", "2048"))) * 1024 * 1024
DEFAULT_CONCURRENCY = max(1, int(os.environ.get("SIM_MAX_CONCURRENT", "2")))
RETENTION_DAYS = max(0, int(os.environ.get("SIM_RETENTION_DAYS", "30")))
CLEANUP_INTERVAL_HOURS = max(0, int(os.environ.get("SIM_CLEANUP_INTERVAL_HOURS", "24")))
LOG_THRESHOLD_GB = max(0, float(os.environ["SIM_LOG_THRESHOLD_GB"])) if "SIM_LOG_THRESHOLD_GB" in os.environ else None
LOG_THRESHOLD_BYTES = int(LOG_THRESHOLD_GB * 1024 ** 3) if LOG_THRESHOLD_GB else 0
MAX_SIMULATION_RECORDS = 200
MISSION_TIME_LIMIT_SECONDS = 300
ACTIVE_STATES = ("starting", "running", "stopping")
TERMINAL_STATES = ("completed", "failed", "stopped")
HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    with db_connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS simulations (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                error TEXT,
                web_port INTEGER
            )"""
        )


def fetch_job(job_id: str) -> dict[str, Any] | None:
    with db_connect() as conn:
        row = conn.execute("SELECT * FROM simulations WHERE id = ?", (job_id,)).fetchone()
    return dict(row) if row else None


def update_job(job_id: str, **changes: Any) -> None:
    if not changes:
        return
    columns = ", ".join(f"{column} = ?" for column in changes)
    values = [*changes.values(), job_id]
    with db_connect() as conn:
        conn.execute(f"UPDATE simulations SET {columns} WHERE id = ?", values)


def public_job(job: dict[str, Any]) -> dict[str, Any]:
    job_id = job["id"]
    return {
        "id": job_id,
        "filename": job["filename"],
        "status": job["status"],
        "created_at": job["created_at"],
        "started_at": job["started_at"],
        "finished_at": job["finished_at"],
        "error": job["error"],
        "view_url": f"/simulations/{job_id}/view/static/vnc.html?autoconnect=1&resize=scale&view_only=1&path=simulations/{job_id}/view/websockify",
        "logs_url": f"/api/simulations/{job_id}/logs",
    }


def validate_and_extract(archive_path: Path, users_dir: Path) -> None:
    """Extract only regular files beneath catkin_ws/src, rejecting tar path tricks."""
    expanded = 0
    found_source_file = False
    users_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = archive.getmembers()
            if not members:
                raise ValueError("The archive is empty.")
            for member in members:
                name = member.name
                if name.startswith("/"):
                    raise ValueError("The archive contains an absolute path.")
                parts = list(PurePosixPath(name).parts)
                while parts and parts[0] in (".", ""):
                    parts.pop(0)
                if ".." in parts:
                    raise ValueError("The archive contains a path outside the submission directory.")

                # Official competition packages may retain the outer `users/`
                # directory. Strip that packaging prefix before validating and
                # extracting into this job's users workspace.
                has_users_prefix = bool(parts and parts[0] == "users")
                if has_users_prefix:
                    parts = parts[1:]

                # Some official archives package the complete `users/` tree,
                # including bootstrap scripts and generated catkin build
                # output. These files are not part of a submission: keep only
                # catkin_ws/src and leave the simulator's trusted bootstrap
                # files in place.
                if has_users_prefix and parts[:2] != ["catkin_ws", "src"]:
                    if member.isfile():
                        expanded += member.size
                        if expanded > MAX_ARCHIVE_BYTES:
                            raise ValueError("The expanded archive exceeds the configured size limit.")
                    elif not (member.isdir() or member.issym()):
                        raise ValueError("Links and special files are not allowed in a submission archive.")
                    continue

                wrapper_dir = member.isdir() and (
                    parts == ["catkin_ws"] or (has_users_prefix and not parts)
                )
                if parts[:2] != ["catkin_ws", "src"] and not wrapper_dir:
                    raise ValueError(
                        "The archive must contain files only under catkin_ws/src. "
                        f"Unexpected archive path: {name!r}."
                    )
                if member.issym():
                    # The official source tree includes this catkin workspace
                    # link. Accept only its exact path and target; arbitrary
                    # archive links can escape the extracted workspace.
                    if parts != ["catkin_ws", "src", "CMakeLists.txt"] or member.linkname != "/opt/ros/melodic/share/catkin/cmake/toplevel.cmake":
                        raise ValueError(
                            "The archive contains an unsupported symbolic link: "
                            f"{name!r} -> {member.linkname!r}."
                        )
                    destination = users_dir.joinpath(*parts)
                    if destination.exists() or destination.is_symlink():
                        raise ValueError("The archive contains duplicate paths.")
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    os.symlink(member.linkname, destination)
                    continue
                if not (member.isdir() or member.isfile()):
                    raise ValueError("Links and special files are not allowed in a submission archive.")
                if member.isfile():
                    expanded += member.size
                    if expanded > MAX_ARCHIVE_BYTES:
                        raise ValueError("The expanded archive exceeds the configured size limit.")
                    if len(parts) > 2:
                        found_source_file = True

                destination = users_dir.joinpath(*parts)
                if not destination.resolve().is_relative_to(users_dir.resolve()):
                    raise ValueError("The archive contains a path outside the submission directory.")
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise ValueError("A file in the archive could not be read.")
                with extracted, destination.open("wb") as target:
                    shutil.copyfileobj(extracted, target, length=1024 * 1024)
                os.chmod(destination, member.mode & 0o777 & ~0o6000)
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(f"Could not read the submission archive: {exc}") from exc

    if not found_source_file:
        raise ValueError("The archive must contain at least one file under catkin_ws/src.")


def prepare_job_workspace(job_id: str, archive_path: Path) -> None:
    job_dir = JOBS_DIR / job_id
    users_dir = job_dir / "users"
    users_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ROOT / "users" / "bootstrap", users_dir / "bootstrap")
    (users_dir / "catkin_ws" / "bootstrap").mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / "users" / "catkin_ws" / "bootstrap" / "build-user.sh",
                 users_dir / "catkin_ws" / "bootstrap" / "build-user.sh")
    shutil.copy2(ROOT / "users" / "catkin_ws" / ".catkin_workspace",
                 users_dir / "catkin_ws" / ".catkin_workspace")
    (job_dir / "share").mkdir(exist_ok=True)
    shm_dir = job_dir / "shm"
    shm_dir.mkdir(exist_ok=True)
    os.chmod(shm_dir, 0o1777)
    validate_and_extract(archive_path, users_dir)


class SimulationManager:
    def __init__(self, app: web.Application):
        self.app = app
        self.limit = DEFAULT_CONCURRENCY
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.scheduler_task: asyncio.Task[None] | None = None
        self.cleanup_task: asyncio.Task[None] | None = None
        self.session: ClientSession | None = None
        self.lock = asyncio.Lock()
        self.log_usage_bytes = 0
        self.log_threshold_bytes = LOG_THRESHOLD_BYTES

    def refresh_log_usage(self) -> int:
        total = 0
        if JOBS_DIR.exists():
            for job_dir in JOBS_DIR.iterdir():
                if not job_dir.is_dir() or job_dir.is_symlink():
                    continue
                roots = [job_dir / "share" / "logs", job_dir / "manager.log"]
                for root in roots:
                    if root.is_file() and not root.is_symlink():
                        try:
                            total += root.stat().st_size
                        except OSError:
                            pass
                    elif root.is_dir() and not root.is_symlink():
                        for current, dirs, files in os.walk(root, followlinks=False):
                            dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()]
                            for name in files:
                                path = Path(current) / name
                                try:
                                    if not path.is_symlink():
                                        total += path.stat().st_size
                                except OSError:
                                    pass
        self.log_usage_bytes = total
        # By default, budget the logs against the actual filesystem that holds
        # the manager data. Existing log bytes are included in that budget.
        self.log_threshold_bytes = (
            LOG_THRESHOLD_BYTES if LOG_THRESHOLD_GB is not None
            else total + shutil.disk_usage(DATA_DIR).free
        )
        return total

    def compose_command(self, job_id: str, *args: str) -> tuple[list[str], dict[str, str]]:
        job_dir = JOBS_DIR / job_id
        project = f"krpc-{job_id}"
        command = [
            "docker", "compose", "--project-name", project,
            "--project-directory", str(ROOT), "--file", str(COMPOSE_FILE), *args,
        ]
        env = os.environ.copy()
        env.update({
            "SIM_WEB_MAPPING": "127.0.0.1::80",
            "SIM_SHARE_DIR": str(job_dir / "share"),
            "SIM_USERS_DIR": str(job_dir / "users"),
            "SIM_SHM_DIR": str(job_dir / "shm"),
        })
        return command, env

    async def command(self, job_id: str, *args: str, record_output: bool = False) -> tuple[int, str]:
        command, env = self.compose_command(job_id, *args)
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=ROOT,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output_bytes, _ = await process.communicate()
        output = output_bytes.decode("utf-8", errors="replace")
        if record_output and output:
            with (JOBS_DIR / job_id / "manager.log").open("a", encoding="utf-8") as log:
                log.write(output)
        return process.returncode or 0, output

    async def stop_stack(self, job_id: str) -> None:
        _, output = await self.command(job_id, "logs", "--no-color", "--tail", "500")
        if output:
            with (JOBS_DIR / job_id / "manager.log").open("a", encoding="utf-8") as log:
                log.write("\n--- Final container logs ---\n")
                log.write(output)
        await self.command(job_id, "down", "--remove-orphans", record_output=True)

    async def service_states(self, job_id: str) -> dict[str, tuple[str, int]]:
        code, output = await self.command(job_id, "ps", "--all", "--quiet")
        ids = [line.strip() for line in output.splitlines() if line.strip()]
        if code or not ids:
            return {}
        inspect_format = '{{index .Config.Labels "com.docker.compose.service"}}|{{.State.Status}}|{{.State.ExitCode}}'
        code, output = await self.command(job_id, "ps", "--all", "--format", "json")
        if code == 0 and output.strip():
            try:
                entries = json.loads(output) if output.lstrip().startswith("[") else [json.loads(line) for line in output.splitlines() if line.strip()]
                states = {}
                for entry in entries:
                    service = entry.get("Service") or entry.get("service")
                    state = entry.get("State") or entry.get("state") or "unknown"
                    exit_code = entry.get("ExitCode", entry.get("exitCode", 0))
                    if service:
                        states[service] = (str(state).lower(), int(exit_code or 0))
                if states:
                    return states
            except (ValueError, TypeError, json.JSONDecodeError):
                pass
        inspect = await asyncio.create_subprocess_exec(
            "docker", "inspect", "--format", inspect_format, *ids,
            cwd=ROOT, env=env, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        output_bytes, _ = await inspect.communicate()
        states = {}
        for line in output_bytes.decode("utf-8", errors="replace").splitlines():
            fields = line.split("|")
            if len(fields) == 3:
                try:
                    states[fields[0]] = (fields[1].lower(), int(fields[2]))
                except ValueError:
                    continue
        return states

    async def ensure_web_port(self, job_id: str) -> int | None:
        job = fetch_job(job_id)
        if job and job.get("web_port"):
            return int(job["web_port"])
        code, output = await self.command(job_id, "port", "krpc", "80")
        if code:
            return None
        match = re.search(r":(\d+)\s*$", output.strip())
        if not match:
            return None
        port = int(match.group(1))
        update_job(job_id, web_port=port)
        return port

    async def start(self) -> None:
        self.session = ClientSession(auto_decompress=True)
        if RETENTION_DAYS and CLEANUP_INTERVAL_HOURS:
            await asyncio.to_thread(self.cleanup_expired_jobs)
        await asyncio.to_thread(self.make_room_for_simulation)
        await asyncio.to_thread(self.refresh_log_usage)
        with db_connect() as conn:
            recovered = conn.execute(
                "SELECT id FROM simulations WHERE status IN ('starting', 'running', 'stopping')"
            ).fetchall()
        for row in recovered:
            job_id = row["id"]
            self.tasks[job_id] = asyncio.create_task(self.monitor_job(job_id, start_compose=False))
        self.scheduler_task = asyncio.create_task(self.scheduler())
        if RETENTION_DAYS and CLEANUP_INTERVAL_HOURS:
            self.cleanup_task = asyncio.create_task(self.cleanup_loop())

    async def close(self) -> None:
        if self.scheduler_task:
            self.scheduler_task.cancel()
        for task in self.tasks.values():
            task.cancel()
        pending = [task for task in [self.scheduler_task, self.cleanup_task, *self.tasks.values()] if task]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if self.session:
            await self.session.close()

    async def cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL_HOURS * 3600)
            try:
                removed = await asyncio.to_thread(self.cleanup_expired_jobs)
                if removed:
                    logger.info("Removed %d expired simulation jobs", removed)
            except Exception:
                logger.exception("Scheduled simulation cleanup failed")

    def cleanup_expired_jobs(self) -> int:
        if not RETENTION_DAYS:
            return 0
        cutoff = datetime.now(timezone.utc).timestamp() - RETENTION_DAYS * 86400
        removed = 0
        with db_connect() as conn:
            rows = conn.execute(
                "SELECT id, finished_at, created_at FROM simulations WHERE status IN ('completed', 'failed', 'stopped')"
            ).fetchall()
        for row in rows:
            job_id = row["id"]
            if job_id in self.tasks:
                continue
            timestamp = row["finished_at"] or row["created_at"]
            try:
                age = datetime.fromisoformat(timestamp).timestamp()
            except (TypeError, ValueError):
                continue
            if age >= cutoff:
                continue
            job_dir = (JOBS_DIR / job_id).resolve()
            if not job_dir.is_relative_to(JOBS_DIR.resolve()):
                logger.warning("Skipping cleanup of unexpected job path: %s", job_id)
                continue
            if job_dir.exists():
                shutil.rmtree(job_dir)
            with db_connect() as conn:
                conn.execute("DELETE FROM simulations WHERE id = ?", (job_id,))
            removed += 1
        return removed

    def make_room_for_simulation(self) -> bool:
        """Keep the task history within its fixed limit, preserving queued and active jobs."""
        with db_connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM simulations").fetchone()[0]
            if count < MAX_SIMULATION_RECORDS:
                return True
            rows = conn.execute(
                """SELECT id FROM simulations
                   WHERE status IN ('completed', 'failed', 'stopped')
                   ORDER BY COALESCE(finished_at, created_at), id"""
            ).fetchall()
        for row in rows:
            job_id = row["id"]
            if job_id in self.tasks:
                continue
            job_dir = (JOBS_DIR / job_id).resolve()
            if not job_dir.is_relative_to(JOBS_DIR.resolve()):
                logger.warning("Skipping removal of unexpected job path: %s", job_id)
                continue
            if job_dir.exists():
                shutil.rmtree(job_dir)
            with db_connect() as conn:
                conn.execute("DELETE FROM simulations WHERE id = ?", (job_id,))
            count -= 1
            logger.info("Removed oldest finished simulation %s to enforce task history limit", job_id)
            if count < MAX_SIMULATION_RECORDS:
                return True
        return False

    async def scheduler(self) -> None:
        next_usage_refresh = 0.0
        while True:
            async with self.lock:
                loop = asyncio.get_running_loop()
                if loop.time() >= next_usage_refresh:
                    await asyncio.to_thread(self.refresh_log_usage)
                    next_usage_refresh = loop.time() + 2
                with db_connect() as conn:
                    active = conn.execute(
                        "SELECT COUNT(*) FROM simulations WHERE status IN ('starting', 'running', 'stopping')"
                    ).fetchone()[0]
                    capacity = max(0, self.limit - active)
                    queued = [] if self.log_threshold_bytes and self.log_usage_bytes >= self.log_threshold_bytes else conn.execute(
                        "SELECT id FROM simulations WHERE status = 'queued' ORDER BY created_at, id LIMIT ?",
                        (capacity,),
                    ).fetchall()
                for row in queued:
                    job_id = row["id"]
                    update_job(job_id, status="starting", started_at=now(), error=None)
                    logger.info("Simulation %s is starting", job_id)
                    self.tasks[job_id] = asyncio.create_task(self.monitor_job(job_id, start_compose=True))
            await asyncio.sleep(1)

    async def monitor_job(self, job_id: str, start_compose: bool) -> None:
        try:
            if start_compose:
                code, output = await self.command(job_id, "up", "--detach", "--remove-orphans", record_output=True)
                if code:
                    raise RuntimeError(output.strip()[-2000:] or f"docker compose up failed ({code})")
            else:
                # Jobs are left running when the Web Manager restarts. Ensure the VNC port is recorded.
                await self.ensure_web_port(job_id)

            missing_since: float | None = None
            ever_seen = False
            while True:
                job = fetch_job(job_id)
                if not job:
                    return
                if job["status"] == "stopping":
                    logger.info("Simulation %s is stopping", job_id)
                    await self.stop_stack(job_id)
                    update_job(job_id, status="stopped", finished_at=now(), error=None)
                    logger.info("Simulation %s stopped", job_id)
                    return

                states = await self.service_states(job_id)
                if states:
                    ever_seen = True
                    user_state = states.get("krpc-user")
                    sim_state = states.get("krpc")
                    if sim_state and sim_state[0] == "running":
                        if job["status"] != "running":
                            update_job(job_id, status="running")
                            logger.info("Simulation %s is running", job_id)
                        await self.ensure_web_port(job_id)

                    if user_state and user_state[0] in ("exited", "dead") and user_state[1] != 0:
                        detail = f"Participant container exited with code {user_state[1]}."
                        await self.stop_stack(job_id)
                        update_job(job_id, status="failed", finished_at=now(), error=detail)
                        logger.error("Simulation %s failed: %s", job_id, detail)
                        return
                    if sim_state and sim_state[0] in ("exited", "dead"):
                        await self.stop_stack(job_id)
                        if sim_state[1] == 0:
                            update_job(job_id, status="completed", finished_at=now(), error=None)
                            logger.info("Simulation %s completed", job_id)
                        else:
                            detail = f"Simulator container exited with code {sim_state[1]}."
                            update_job(job_id, status="failed", finished_at=now(), error=detail)
                            logger.error("Simulation %s failed: %s", job_id, detail)
                        return
                    if states and all(state in ("exited", "dead", "removing") for state, _ in states.values()):
                        failed = [(service, code) for service, (state, code) in states.items() if code != 0]
                        await self.stop_stack(job_id)
                        if failed:
                            detail = ", ".join(f"{service} exited with code {code}" for service, code in failed)
                            update_job(job_id, status="failed", finished_at=now(), error=detail)
                            logger.error("Simulation %s failed: %s", job_id, detail)
                        else:
                            update_job(job_id, status="completed", finished_at=now(), error=None)
                            logger.info("Simulation %s completed", job_id)
                        return
                else:
                    if ever_seen:
                        detail = "Simulation containers disappeared unexpectedly."
                        update_job(job_id, status="failed", finished_at=now(), error=detail)
                        logger.error("Simulation %s failed: %s", job_id, detail)
                        return
                    if not start_compose:
                        detail = "Simulation containers were not found after the manager restarted."
                        update_job(job_id, status="failed", finished_at=now(), error=detail)
                        logger.error("Simulation %s failed: %s", job_id, detail)
                        return
                    if missing_since is None:
                        missing_since = asyncio.get_running_loop().time()
                    elif asyncio.get_running_loop().time() - missing_since > 120:
                        raise RuntimeError("Simulation containers did not start within 120 seconds.")
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Simulation %s encountered an error", job_id)
            try:
                await self.stop_stack(job_id)
            except Exception:
                logger.exception("Could not clean up simulation %s", job_id)
            update_job(job_id, status="failed", finished_at=now(), error=str(exc)[:2000])
        finally:
            current = asyncio.current_task()
            if self.tasks.get(job_id) is current:
                self.tasks.pop(job_id, None)

    async def stop(self, job_id: str) -> dict[str, Any]:
        async with self.lock:
            job = fetch_job(job_id)
            if not job:
                raise web.HTTPNotFound(text="Simulation not found.")
            if job["status"] in TERMINAL_STATES:
                return public_job(job)
            if job["status"] == "queued":
                update_job(job_id, status="stopped", finished_at=now())
                logger.info("Queued simulation %s stopped before starting", job_id)
                return public_job(fetch_job(job_id) or job)
            update_job(job_id, status="stopping")
            task = self.tasks.get(job_id)
            if task is None:
                task = asyncio.create_task(self.monitor_job(job_id, start_compose=False))
                self.tasks[job_id] = task
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=30)
        except asyncio.TimeoutError:
            await self.stop_stack(job_id)
            update_job(job_id, status="stopped", finished_at=now())
        return public_job(fetch_job(job_id) or {})


async def index(_: web.Request) -> web.Response:
    return web.Response(text=INDEX_HTML, content_type="text/html")


async def list_simulations(request: web.Request) -> web.Response:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM simulations ORDER BY created_at DESC LIMIT ?",
            (MAX_SIMULATION_RECORDS,),
        ).fetchall()
        task_count = conn.execute("SELECT COUNT(*) FROM simulations").fetchone()[0]
    manager: SimulationManager = request.app["manager"]
    return web.json_response({
        "simulations": [public_job(dict(row)) for row in rows],
        "task_count": task_count,
        "task_limit": MAX_SIMULATION_RECORDS,
        "max_concurrent": manager.limit,
        "log_usage_bytes": manager.log_usage_bytes,
        "log_threshold_bytes": manager.log_threshold_bytes,
        "log_blocked": bool(manager.log_threshold_bytes and manager.log_usage_bytes >= manager.log_threshold_bytes),
    })


async def get_simulation(request: web.Request) -> web.Response:
    job = fetch_job(request.match_info["job_id"])
    if not job:
        raise web.HTTPNotFound(text="Simulation not found.")
    return web.json_response(public_job(job))


async def upload_simulation(request: web.Request) -> web.Response:
    reader = await request.multipart()
    field = await reader.next()
    if field is None or field.name != "file" or not field.filename:
        raise web.HTTPBadRequest(text="Upload a .tar.gz file in the 'file' field.")
    filename = Path(field.filename).name
    if not filename.lower().endswith((".tar.gz", ".tgz")):
        raise web.HTTPBadRequest(text="Only .tar.gz or .tgz submission packages are supported.")

    job_id = uuid.uuid4().hex[:12]
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=False)
    archive_path = job_dir / "submission.tar.gz"
    size = 0
    try:
        with archive_path.open("wb") as destination:
            while True:
                chunk = await field.read_chunk(size=64 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise web.HTTPRequestEntityTooLarge(MAX_UPLOAD_BYTES, size)
                destination.write(chunk)
        await asyncio.to_thread(prepare_job_workspace, job_id, archive_path)
    except web.HTTPException:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    except ValueError as exc:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise web.HTTPBadRequest(text=str(exc)) from exc
    except Exception:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise

    manager: SimulationManager = request.app["manager"]
    async with manager.lock:
        has_room = await asyncio.to_thread(manager.make_room_for_simulation)
        if not has_room:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise web.HTTPConflict(text="The task history limit has been reached; finish or delete a task before uploading another.")
        created = now()
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO simulations (id, filename, status, created_at) VALUES (?, ?, 'queued', ?)",
                (job_id, filename, created),
            )
    logger.info("Simulation %s uploaded (%s) and queued", job_id, filename)
    return web.json_response(public_job(fetch_job(job_id) or {}), status=201)


async def stop_simulation(request: web.Request) -> web.Response:
    result = await request.app["manager"].stop(request.match_info["job_id"])
    return web.json_response(result)


async def simulation_logs(request: web.Request) -> web.Response:
    job_id = request.match_info["job_id"]
    if not fetch_job(job_id):
        raise web.HTTPNotFound(text="Simulation not found.")
    code, output = await request.app["manager"].command(
        job_id, "logs", "--no-color", "--tail", "300"
    )
    manager_log = JOBS_DIR / job_id / "manager.log"
    prefix = manager_log.read_text(encoding="utf-8", errors="replace") if manager_log.exists() else ""
    if code and not output:
        output = "Unable to read container logs.\n"
    content = (prefix + "\n" + output)[-200_000:]
    return web.Response(text=content, content_type="text/plain")


def simulation_artifact_files(job_id: str) -> tuple[Path, list[Path]]:
    job_root = (JOBS_DIR / job_id).resolve()
    logs_dir = job_root / "share" / "logs"
    if not logs_dir.is_dir() or not logs_dir.resolve().is_relative_to(job_root):
        return logs_dir, []
    files: list[Path] = []
    for current, dirs, names in os.walk(logs_dir, followlinks=False):
        dirs[:] = [name for name in dirs if name != "latest" and not (Path(current) / name).is_symlink()]
        for name in names:
            path = Path(current) / name
            if path.is_symlink() or not path.is_file():
                continue
            if path.suffix.lower() in {".log", ".txt", ".json", ".csv", ".bag"}:
                files.append(path)
    return logs_dir, files


def load_score_result(files: list[Path]) -> dict[str, Any] | None:
    results = [path for path in files if path.name == "result.json" and "latest" not in path.parts]
    for path in sorted(results, key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        except (OSError, json.JSONDecodeError):
            continue
    return None


async def simulation_score(request: web.Request) -> web.Response:
    job_id = request.match_info["job_id"]
    job = fetch_job(job_id)
    if not job:
        raise web.HTTPNotFound(text="Simulation not found.")
    _, files = simulation_artifact_files(job_id)
    result = load_score_result(files)
    mission = ((result or {}).get("JudgeReport", {}).get("mission", {}))
    duration = mission.get("duration_sec") if isinstance(mission, dict) else None
    try:
        mission_remain_seconds = max(0, MISSION_TIME_LIMIT_SECONDS - float(duration)) if duration is not None else None
    except (TypeError, ValueError):
        mission_remain_seconds = None
    return web.json_response({
        "score": result,
        "mission_remain_seconds": mission_remain_seconds,
        "status": job["status"],
    })


async def delete_simulation(request: web.Request) -> web.Response:
    job_id = request.match_info["job_id"]
    manager: SimulationManager = request.app["manager"]
    async with manager.lock:
        job = fetch_job(job_id)
        if not job:
            raise web.HTTPNotFound(text="Simulation not found.")
        if job["status"] not in TERMINAL_STATES:
            raise web.HTTPConflict(text="Only finished simulation tasks can be deleted.")
        if job_id in manager.tasks:
            raise web.HTTPConflict(text="The simulation task is still shutting down.")
        job_dir = (JOBS_DIR / job_id).resolve()
        if not job_dir.is_relative_to(JOBS_DIR.resolve()):
            raise web.HTTPBadRequest(text="Invalid simulation task path.")
        if job_dir.exists():
            await asyncio.to_thread(shutil.rmtree, job_dir)
        with db_connect() as conn:
            conn.execute("DELETE FROM simulations WHERE id = ?", (job_id,))
    await asyncio.to_thread(manager.refresh_log_usage)
    return web.json_response({"deleted": job_id})


async def simulation_export(request: web.Request) -> web.StreamResponse:
    job_id = request.match_info["job_id"]
    if not fetch_job(job_id):
        raise web.HTTPNotFound(text="Simulation not found.")
    logs_dir = (JOBS_DIR / job_id / "share" / "logs").resolve()
    if not logs_dir.is_relative_to((JOBS_DIR / job_id).resolve()):
        raise web.HTTPNotFound(text="Simulation artifacts not found.")
    temp_file = tempfile.NamedTemporaryFile(prefix=f"{job_id}-", suffix=".zip", dir=DATA_DIR, delete=False)
    temp_path = Path(temp_file.name)
    temp_file.close()
    files: list[tuple[Path, str]] = []
    logs_only = request.query.get("logs_only") == "1"
    if logs_dir.is_dir():
        for path in logs_dir.rglob("*"):
            if not path.is_file() or path.is_symlink() or "latest" in path.relative_to(logs_dir).parts:
                continue
            if path.suffix.lower() in {".log", ".txt", ".json", ".csv", ".bag"}:
                if logs_only and path.suffix.lower() not in {".log", ".txt"}:
                    continue
                files.append((path, path.relative_to(logs_dir).as_posix()))
    manager_log = JOBS_DIR / job_id / "manager.log"
    if manager_log.is_file() and not manager_log.is_symlink():
        files.append((manager_log, "manager.log"))
    if not files:
        temp_path.unlink(missing_ok=True)
        raise web.HTTPNotFound(text="No archived logs or ROS bags are available for this simulation.")
    def build_archive() -> None:
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
            for path, name in files:
                archive.write(path, name, compress_type=zipfile.ZIP_STORED if path.suffix.lower() == ".bag" else zipfile.ZIP_DEFLATED)

    try:
        await asyncio.to_thread(build_archive)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    response = web.StreamResponse(headers={
        "Content-Type": "application/zip",
        "Content-Disposition": f'attachment; filename="simulation-{job_id}-artifacts.zip"',
        "Content-Length": str(temp_path.stat().st_size),
    })
    try:
        await response.prepare(request)
        with temp_path.open("rb") as archive_file:
            while chunk := await asyncio.to_thread(archive_file.read, 1024 * 1024):
                await response.write(chunk)
        await response.write_eof()
    finally:
        temp_path.unlink(missing_ok=True)
    return response


def rewrite_html_paths(body: bytes, prefix: str) -> bytes:
    text = body.decode("utf-8", errors="replace")
    text = re.sub(
        r"(?i)(\b(?:src|href|action)=['\"])/(?!/)",
        lambda match: match.group(1) + prefix + "/",
        text,
    )
    text = re.sub(
        r"(?i)(url\(\s*['\"]?)/(?!/)",
        lambda match: match.group(1) + prefix + "/",
        text,
    )
    return text.encode("utf-8")


async def proxy_view(request: web.Request) -> web.StreamResponse:
    job_id = request.match_info["job_id"]
    job = fetch_job(job_id)
    if not job:
        raise web.HTTPNotFound(text="Simulation not found.")
    if job["status"] not in ACTIVE_STATES:
        raise web.HTTPServiceUnavailable(text="This simulation is not running.")
    port = await request.app["manager"].ensure_web_port(job_id)
    if not port:
        raise web.HTTPServiceUnavailable(text="The simulator web desktop is not ready yet.")

    tail = request.match_info.get("tail", "")
    path = "/" + tail.lstrip("/")
    prefix = f"/simulations/{job_id}/view"
    upstream_url = f"http://127.0.0.1:{port}{path}"
    if request.query_string:
        upstream_url += "?" + request.query_string

    session: ClientSession = request.app["manager"].session
    if request.headers.get("Upgrade", "").lower() == "websocket":
        # Keep the proxied VNC byte stream uncompressed. Browser WebSocket
        # extensions are optional, and disabling permessage-deflate avoids
        # negotiation mismatches across the reverse proxy chain.
        server_ws = web.WebSocketResponse(
            protocols=("binary",), autoping=False, heartbeat=30, compress=False
        )
        headers = {}
        for key in ("Origin", "Cookie"):
            if key in request.headers:
                headers[key] = request.headers[key]
        async with session.ws_connect(upstream_url.replace("http://", "ws://", 1), headers=headers,
                                      protocols=(), autoping=False, heartbeat=30) as upstream_ws:
            await server_ws.prepare(request)

            async def client_to_upstream() -> None:
                async for message in server_ws:
                    if message.type == WSMsgType.TEXT:
                        await upstream_ws.send_str(message.data)
                    elif message.type == WSMsgType.BINARY:
                        await upstream_ws.send_bytes(message.data)
                    elif message.type == WSMsgType.PING:
                        await upstream_ws.ping(message.data)
                    elif message.type == WSMsgType.PONG:
                        await upstream_ws.pong(message.data)
                    elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                        break

            async def upstream_to_client() -> None:
                async for message in upstream_ws:
                    if message.type == WSMsgType.TEXT:
                        await server_ws.send_str(message.data)
                    elif message.type == WSMsgType.BINARY:
                        await server_ws.send_bytes(message.data)
                    elif message.type == WSMsgType.PING:
                        await server_ws.ping(message.data)
                    elif message.type == WSMsgType.PONG:
                        await server_ws.pong(message.data)
                    elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                        break

            client_task = asyncio.create_task(client_to_upstream())
            upstream_task = asyncio.create_task(upstream_to_client())
            done, pending = await asyncio.wait(
                (client_task, upstream_task), return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await upstream_ws.close()
            await server_ws.close()
            return server_ws

    headers = {
        key: value for key, value in request.headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() not in ("content-length", "accept-encoding")
    }
    async with session.request(
        request.method, upstream_url, headers=headers,
        data=await request.read() if request.can_read_body else None,
        allow_redirects=False,
    ) as upstream:
        body = await upstream.read()
        response_headers = {
            key: value for key, value in upstream.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() not in ("content-length", "content-encoding")
        }
        content_type = upstream.headers.get("Content-Type", "")
        if "text/html" in content_type or "text/css" in content_type:
            body = rewrite_html_paths(body, prefix)
        location = response_headers.get("Location")
        if location:
            parsed = urlsplit(location)
            if parsed.netloc in (f"127.0.0.1:{port}", f"localhost:{port}"):
                response_headers["Location"] = prefix + (parsed.path or "/") + (("?" + parsed.query) if parsed.query else "")
        return web.Response(status=upstream.status, headers=response_headers, body=body)


def create_app() -> web.Application:
    init_db()
    app = web.Application(client_max_size=MAX_UPLOAD_BYTES + 1024 * 1024)
    manager = SimulationManager(app)
    app["manager"] = manager
    app.router.add_get("/", index)
    app.router.add_get("/api/simulations", list_simulations)
    app.router.add_post("/api/simulations", upload_simulation)
    app.router.add_get("/api/simulations/{job_id}", get_simulation)
    app.router.add_post("/api/simulations/{job_id}/stop", stop_simulation)
    app.router.add_get("/api/simulations/{job_id}/logs", simulation_logs)
    app.router.add_get("/api/simulations/{job_id}/score", simulation_score)
    app.router.add_route("DELETE", "/api/simulations/{job_id}", delete_simulation)
    app.router.add_get("/api/simulations/{job_id}/export", simulation_export)
    app.router.add_route("*", "/simulations/{job_id}/view/{tail:.*}", proxy_view)
    async def on_startup(_app: web.Application) -> None:
        await manager.start()
        logger.info("Web Manager started")

    app.on_startup.append(on_startup)
    app.on_cleanup.append(lambda _app: manager.close())
    return app


INDEX_HTML = r"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kibo-RPC Simulator Manager</title>
  <style>
    :root { color-scheme: dark; --bg:#101820; --panel:#18252e; --line:#31414b; --muted:#9aabb5; --accent:#71d6bc; --danger:#ff877d; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:#edf4f6; font:15px/1.5 system-ui,sans-serif; }
    header { padding:24px clamp(18px,4vw,56px); border-bottom:1px solid var(--line); display:flex; align-items:center; justify-content:space-between; gap:20px; }
    h1 { margin:0; font-size:21px; letter-spacing:.02em; } .subtitle,.muted { color:var(--muted); }
    main { padding:24px clamp(18px,4vw,56px) 48px; display:grid; grid-template-columns:minmax(270px,340px) minmax(0,1fr); gap:22px; max-width:1600px; margin:auto; }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px; min-width:0; }
    h2 { margin:0 0 14px; font-size:16px; } .upload { display:grid; gap:12px; } input[type=file] { width:100%; color:var(--muted); }
    button { border:0; border-radius:7px; background:var(--accent); color:#0d2421; font-weight:700; padding:9px 13px; cursor:pointer; }
    a.button { display:inline-block; text-decoration:none; border-radius:7px; font-weight:700; padding:5px 9px; font-size:13px; }
    a.button.secondary { background:#31434d; color:#edf4f6; }
    button:disabled { opacity:.5; cursor:wait; } button.secondary { background:#31434d; color:#edf4f6; } button.stop { background:#542e32; color:#ffd7d3; }
    .queue-list { display:grid; gap:10px; max-height:min(60vh,720px); overflow-y:auto; overscroll-behavior:contain; padding:2px 6px 2px 2px; scrollbar-gutter:stable; }
    .job { padding:12px; border:1px solid var(--line); border-radius:9px; display:grid; gap:7px; }
    .job[aria-pressed="true"] { border:2px solid var(--accent); background:#1d3537; box-shadow:0 0 0 2px #71d6bc33; }
    .job:focus-visible { outline:2px solid #c7dcff; outline-offset:2px; }
    .job-top { display:flex; justify-content:space-between; align-items:center; gap:10px; } .job-name { overflow-wrap:anywhere; font-weight:600; }
    .badge { font-size:12px; padding:2px 8px; border-radius:20px; background:#34444e; white-space:nowrap; }
    .badge.running { background:#174b40; color:#9af0d8; } .badge.failed { background:#542e32; color:#ffc3bd; }
    .badge.completed { background:#293f5b; color:#c7dcff; } .badge.queued { background:#514322; color:#ffe19a; }
    .job-actions { display:flex; gap:8px; flex-wrap:wrap; } .job-actions button { padding:5px 9px; font-size:13px; }
    .job-artifacts { display:grid; gap:8px; margin-top:12px; } .score-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px; margin:12px 0; }
    .checkpoint-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(112px,1fr)); gap:10px; margin-top:12px; }
    .checkpoint-card { padding:12px; border:1px solid var(--line); border-radius:8px; background:#0e151a; text-align:center; }
    .checkpoint-level { display:block; font-size:21px; font-weight:700; margin-top:4px; }
    .score-card { padding:12px; border:1px solid var(--line); border-radius:8px; } .score-value { display:block; font-size:20px; font-weight:700; }
    .viewer-head { display:flex; justify-content:space-between; align-items:center; gap:16px; margin-bottom:12px; } .viewer { position:relative; width:100%; aspect-ratio:16/9; max-height:calc(100vh - 300px); min-height:280px; background:#080d10; border:1px solid var(--line); border-radius:8px; overflow:hidden; }
    .viewer-download { display:inline-block; background:var(--accent); color:#0d2421; padding:10px 16px; border-radius:7px; font-weight:700; text-decoration:none; }
    iframe { display:block; width:100%; height:100%; border:0; } .empty-view { display:grid; place-items:center; height:100%; min-height:280px; color:var(--muted); }
    pre { max-height:320px; overflow:auto; white-space:pre-wrap; overflow-wrap:anywhere; background:#0e151a; border-radius:7px; padding:12px; color:#c4d3da; font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size:13px; line-height:1.55; }
    .notice { min-height:22px; color:var(--muted); } .notice.error { color:var(--danger); }
    @media(max-width:850px) { main { grid-template-columns:1fr; } header { align-items:flex-start; flex-direction:column; } }
  </style>
</head>
<body>
  <header><div><h1>Kibo-RPC Simulator Manager</h1><div class="subtitle">本機模擬工作佇列與即時畫面</div></div><div><div id="capacity" class="muted">讀取中…</div><div id="log-usage" class="muted"></div></div></header>
  <main>
    <section class="panel">
      <h2>上傳 submission package</h2>
      <form id="upload-form" class="upload">
        <input name="file" type="file" accept=".tar.gz,.tgz,application/gzip" required>
        <button id="upload-button" type="submit">加入模擬佇列</button>
        <div id="notice" class="notice" aria-live="polite"></div>
      </form>
      <h2 style="margin-top:26px">模擬工作 <span id="task-count" class="muted" style="font-size:14px;font-weight:400"></span></h2>
      <div id="jobs" class="queue-list"><div class="muted">讀取中…</div></div>
    </section>
    <section class="panel">
      <div class="viewer-head"><div><h2 style="margin:0">模擬畫面</h2><div id="selected-name" class="muted">選擇一個模擬工作</div></div><button id="refresh" class="secondary" type="button">重新整理</button></div>
      <div id="viewer" class="viewer"><div class="empty-view">目前沒有選取執行中的模擬</div></div>
      <details id="logs-panel" style="margin-top:16px"><summary>工作日誌</summary><pre id="logs">選擇工作後自動載入。</pre></details>
      <details id="score-panel" style="margin-top:16px"><summary>Score Results</summary><div id="score-results" class="job-artifacts">Select a task to load its score.</div></details>
    </section>
  </main>
<script>
  const labels = {queued:'等待中',starting:'準備中',running:'執行中',stopping:'停止中',completed:'完成',failed:'失敗',stopped:'已停止'};
  let selected = null;
  let selectedStatus = null;
  const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  const formatTaskTime = value => new Intl.DateTimeFormat(undefined, {year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}).format(new Date(value));
  function updateSelectedHighlight() {
    document.querySelectorAll('#jobs [data-select]').forEach(card => {
      const isSelected = card.dataset.select === selected;
      card.setAttribute('aria-pressed', String(isSelected));
      if (isSelected) card.setAttribute('aria-current', 'true');
      else card.removeAttribute('aria-current');
    });
  }
  async function refresh() {
    try {
      const response = await fetch('/api/simulations');
      const data = await response.json();
      const sims = data.simulations;
      document.getElementById('task-count').textContent = `目前 ${data.task_count} / ${data.task_limit} 筆`;
      const active = sims.filter(job => ['starting','running','stopping'].includes(job.status)).length;
      document.getElementById('capacity').textContent = `執行中 ${active} / ${data.max_concurrent}`;
      const usage = document.getElementById('log-usage');
      if (data.log_threshold_bytes) {
        const used = (data.log_usage_bytes / 1024 ** 3).toFixed(2);
        const limit = (data.log_threshold_bytes / 1024 ** 3).toFixed(2);
        usage.textContent = data.log_blocked ? `Log 空間 ${used} / ${limit} GiB，已達上限，暫停新模擬` : `Log 空間 ${used} / ${limit} GiB`;
        usage.className = data.log_blocked ? 'notice error' : 'muted';
      } else usage.textContent = `Log 空間 ${(data.log_usage_bytes / 1024 ** 3).toFixed(2)} GiB（未設上限）`;
      const list = document.getElementById('jobs');
      if (!sims.length) list.innerHTML = '<div class="muted">尚無模擬工作，上傳封包即可開始。</div>';
      else list.innerHTML = sims.map(job => `<article class="job" data-select="${esc(job.id)}" tabindex="0" role="button" aria-pressed="${selected === job.id}"${selected === job.id ? ' aria-current="true"' : ''}>
        <div class="job-top"><span class="job-name">${esc(job.filename)}</span><span class="badge ${esc(job.status)}">${labels[job.status] || esc(job.status)}</span></div>
        <div class="muted">時間：${esc(formatTaskTime(job.created_at))}${job.error ? ` · ${esc(job.error)}` : ''}</div>
        <div class="job-actions">
          ${job.status === 'running' || job.status === 'starting' || job.status === 'queued' ? `<button class="stop" data-stop="${esc(job.id)}">停止</button>` : ''}
          <a class="button secondary" href="/api/simulations/${encodeURIComponent(job.id)}/export">匯出日誌與 ROS bag</a>
          ${['completed','failed','stopped'].includes(job.status) ? `<button class="stop" data-delete-task="${esc(job.id)}">刪除任務</button>` : ''}
        </div></article>`).join('');
      list.querySelectorAll('[data-select]').forEach(card => {
        card.style.cursor = 'pointer';
        card.onclick = event => { if (!event.target.closest('button,a')) selectJob(sims.find(job => job.id === card.dataset.select)); };
        card.onkeydown = event => { if ((event.key === 'Enter' || event.key === ' ') && event.target === card) { event.preventDefault(); selectJob(sims.find(job => job.id === card.dataset.select)); } };
      });
      list.querySelectorAll('[data-stop]').forEach(button => button.onclick = async () => {
        button.disabled = true; await fetch(`/api/simulations/${button.dataset.stop}/stop`, {method:'POST'}); await refresh();
      });
      list.querySelectorAll('[data-delete-task]').forEach(button => button.onclick = async event => {
        event.stopPropagation();
        const job = sims.find(item => item.id === button.dataset.deleteTask);
        if (!window.confirm(`刪除模擬任務「${job?.filename || button.dataset.deleteTask}」及其所有工作檔案？`)) return;
        button.disabled = true;
        const response = await fetch(`/api/simulations/${encodeURIComponent(button.dataset.deleteTask)}`, {method:'DELETE'});
        if (!response.ok) { button.disabled = false; window.alert(await response.text()); return; }
        await refresh();
      });
      if (selected) {
        const current = sims.find(job => job.id === selected);
        if (!current) {
          selected = null;
          document.getElementById('selected-name').textContent = '選擇一個模擬工作';
          document.getElementById('viewer').innerHTML = '<div class="empty-view">目前沒有選取執行中的模擬</div>';
          document.getElementById('logs').textContent = '選擇工作後自動載入。';
          document.getElementById('score-results').textContent = 'Select a task to load its score.';
          selectedStatus = null;
        } else if (current.status !== selectedStatus) {
          selectedStatus = current.status;
          showView(current);
          document.getElementById('logs-panel').hidden = ['completed','failed','stopped'].includes(current.status);
          loadJobScore(current);
        } else if (current.status === 'running' && !document.querySelector('#viewer iframe')) {
          showView(current);
        }
      }
    } catch (error) {
      document.getElementById('notice').textContent = `無法連線到管理服務：${error}`;
      document.getElementById('notice').className = 'notice error';
    }
  }
  function showView(job) {
    if (job && ['completed','failed','stopped'].includes(job.status)) {
      document.getElementById('viewer').innerHTML = `<div class="empty-view"><a class="viewer-download" href="/api/simulations/${encodeURIComponent(job.id)}/export?logs_only=1">下載此模擬任務的 Log</a></div>`;
      return;
    }
    if (!job || job.status !== 'running') {
      document.getElementById('viewer').innerHTML = '<div class="empty-view">工作執行中時會顯示模擬畫面</div>';
      return;
    }
    document.getElementById('selected-name').textContent = `${job.filename} · ${formatTaskTime(job.created_at)}`;
    const viewUrl = new URL(job.view_url, window.location.href);
    viewUrl.searchParams.set('host', window.location.hostname);
    viewUrl.searchParams.set('port', window.location.port || (window.location.protocol === 'https:' ? '443' : '80'));
    viewUrl.searchParams.set('encrypt', window.location.protocol === 'https:' ? '1' : '0');
    document.getElementById('viewer').innerHTML = `<iframe title="${esc(job.filename)}" src="${esc(viewUrl.href)}"></iframe>`;
  }
  async function selectJob(job) {
    if (!job) return;
    const changed = selected !== job.id;
    selected = job.id;
    selectedStatus = job.status;
    updateSelectedHighlight();
    document.getElementById('selected-name').textContent = `${job.filename} · ${formatTaskTime(job.created_at)}`;
    if (changed || !document.querySelector('#viewer iframe')) showView(job);
    document.getElementById('logs-panel').open = true;
    document.getElementById('score-panel').open = true;
    const terminal = ['completed','failed','stopped'].includes(job.status);
    document.getElementById('logs-panel').hidden = terminal;
    if (!terminal) {
      const logs = document.getElementById('logs');
      logs.textContent = '載入日誌中…';
      try {
        const response = await fetch(`/api/simulations/${encodeURIComponent(job.id)}/logs`);
        logs.textContent = await response.text();
      } catch (error) { logs.textContent = `無法載入日誌：${error}`; }
    }
    await loadJobScore(job);
  }
  async function loadJobScore(job) {
    const scoreNode = document.getElementById('score-results');
    scoreNode.textContent = 'Loading score results…';
    try {
      const response = await fetch(`/api/simulations/${encodeURIComponent(job.id)}/score`);
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      if (selected !== job.id) return;
      renderScore(data.score, data.mission_remain_seconds);
    } catch (error) { scoreNode.textContent = `Unable to load score results: ${error}`; }
  }
  function renderScore(result, missionRemainSeconds) {
    const node = document.getElementById('score-results');
    if (!result) { node.innerHTML = '<div class="muted">No result.json score data yet.</div>'; return; }
    const report = result.JudgeReport || {};
    const mission = report.mission || {};
    const goal = report.goal || {};
    const checkpoints = Array.isArray(report.checkpoint_detail) ? report.checkpoint_detail : [];
    const remainText = missionRemainSeconds == null ? (mission.started && !mission.finished ? 'In progress' : '—') : `${Number(missionRemainSeconds).toFixed(2)} sec`;
    node.innerHTML = `<div class="score-grid">
      <div class="score-card"><span class="muted">Mission Remain Time</span><span class="score-value">${esc(remainText)}</span></div>
      <div class="score-card"><span class="muted">Mission Time</span><span class="score-value">${mission.duration_sec == null ? '—' : `${esc(mission.duration_sec)} sec`}</span></div>
      <div class="score-card"><span class="muted">Goal</span><span class="score-value">${goal.success ? 'Success' : mission.finished ? 'Not achieved' : 'Incomplete'}</span></div>
      <div class="score-card"><span class="muted">Illegal</span><span class="score-value">${result.Illegal ? 'Yes' : 'No'}</span></div>
      <div class="score-card"><span class="muted">Checkpoints Passed</span><span class="score-value">${checkpoints.filter(item => item.success).length} / ${checkpoints.length}</span></div>
    </div>${checkpoints.length ? `<h3>Final Checkpoint Levels</h3><div class="checkpoint-grid">${checkpoints.map(item => `<div class="checkpoint-card"><span>CP ${esc(item.checkpoint_id)}</span><span class="checkpoint-level">Lv ${esc(item.level ?? 0)}</span><span class="muted">${item.success ? 'Passed' : 'Not passed'}</span></div>`).join('')}</div>` : '<div class="muted">No checkpoint details.</div>'}`;
  }
  document.getElementById('upload-form').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const button = document.getElementById('upload-button');
    const notice = document.getElementById('notice');
    button.disabled = true; notice.className = 'notice'; notice.textContent = '上傳並檢查封包中…';
    try {
      const response = await fetch('/api/simulations', {method:'POST', body:new FormData(form)});
      if (!response.ok) throw new Error(await response.text());
      const job = await response.json();
      notice.textContent = `${job.filename} 已加入佇列。`;
      form.reset(); await refresh();
    } catch (error) { notice.className = 'notice error'; notice.textContent = `上傳失敗：${error.message || error}`; }
    finally { button.disabled = false; }
  });
  document.getElementById('refresh').onclick = refresh;
  refresh(); setInterval(refresh, 3000);
</script>
</body>
</html>"""


def main() -> None:
    host = os.environ.get("SIM_MANAGER_HOST", "127.0.0.1")
    port = int(os.environ.get("SIM_MANAGER_PORT", "8080"))
    logging.basicConfig(
        level=os.environ.get("SIM_MANAGER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("Starting Web Manager on %s:%d", host, port)
    # noVNC keeps WebSocket connections open for the lifetime of a simulation.
    # Do not let those clients hold a systemd restart for aiohttp's 60s default.
    web.run_app(create_app(), host=host, port=port, print=None, shutdown_timeout=5)


if __name__ == "__main__":
    main()
