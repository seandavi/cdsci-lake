import os
import re
import json
import asyncio
from abc import ABC, abstractmethod
from typing import List, Optional
from pathlib import Path
import duckdb

from models import SourceModel, RunModel, SnapshotModel, LogModel

def get_custom_settings():
    from cdsci.lake.config import Settings
    
    # 1. Try to find DUCKLAKE_URI and R2 vars from env first
    ducklake_uri = os.environ.get("DUCKLAKE_URI")
    s3_endpoint = os.environ.get("S3_ENDPOINT")
    s3_access_key = os.environ.get("S3_ACCESS_KEY_ID")
    s3_secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")
    s3_region = os.environ.get("S3_REGION", "auto")

    # 2. If not in env, try to load them from .env files locally (without putting them in os.environ)
    local_vars = {}
    if not ducklake_uri:
        candidates = [
            Path("../omicidx/.env"),
            Path(__file__).resolve().parents[2] / "omicidx" / ".env",
            Path("/home/davsean/Documents/git/omicidx/.env")
        ]
        for path in candidates:
            if path.exists():
                with open(path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            local_vars[k.strip()] = v.strip().strip("'\"")
                break
        
        ducklake_uri = local_vars.get("DUCKLAKE_URI")
        s3_endpoint = local_vars.get("S3_ENDPOINT")
        s3_access_key = local_vars.get("S3_ACCESS_KEY_ID")
        s3_secret_key = local_vars.get("S3_SECRET_ACCESS_KEY")
        s3_region = local_vars.get("S3_REGION", "auto")

    # 3. If we have a postgres DUCKLAKE_URI, construct postgres Settings
    if ducklake_uri and ducklake_uri.startswith("postgres:"):
        params_str = ducklake_uri[len("postgres:"):].strip()
        params = {}
        for part in params_str.split():
            if "=" in part:
                k, v = part.split("=", 1)
                params[k] = v
        
        r2_account_id = None
        if s3_endpoint:
            match = re.search(r"https://([^.]+)\.r2\.cloudflarestorage\.com", s3_endpoint)
            if match:
                r2_account_id = match.group(1)

        return Settings(
            lake_backend="postgres",
            cred_source="env",
            lake_pg_host=params.get("host", "127.0.0.1"),
            lake_pg_port=int(params.get("port", 5432)),
            lake_pg_dbname=params.get("dbname", "lake"),
            lake_pg_user=params.get("user", "postgres"),
            lake_pg_password=params.get("password", ""),
            r2_endpoint_url=s3_endpoint,
            r2_account_id=r2_account_id,
            r2_access_key_id=s3_access_key,
            r2_secret_access_key=s3_secret_key,
            r2_region=s3_region
        )
    
    # 4. Fallback to default Settings
    return Settings()


class DashboardRepository(ABC):
    @abstractmethod
    async def get_sources(self) -> List[SourceModel]:
        pass

    @abstractmethod
    async def get_runs(self, limit: int = 50) -> List[RunModel]:
        pass

    @abstractmethod
    async def get_snapshots(self, limit: int = 50) -> List[SnapshotModel]:
        pass

    @abstractmethod
    async def get_logs(self, run_id: str) -> List[LogModel]:
        pass


class DuckDBDashboardRepository(DashboardRepository):
    def __init__(self):
        self._con = None
        self._lock = asyncio.Lock()

    def _get_connection(self):
        if self._con is None:
            from cdsci.lake.connect import lake_connect, _attach_ops
            
            s = get_custom_settings()

            try:
                self._con = lake_connect(s, read_only=True)
            except Exception as e:
                if "does not exist" in str(e):
                    self._con = lake_connect(s, read_only=False)
                else:
                    raise e
            
            try:
                _attach_ops(self._con, s)
            except Exception:
                pass
        return self._con

    async def _run_query(self, query: str, params: list = None):
        async with self._lock:
            def execute():
                con = self._get_connection()
                cur = con.cursor()
                if params:
                    return cur.execute(query, params).fetchall()
                else:
                    return cur.execute(query).fetchall()
            return await asyncio.to_thread(execute)

    async def get_sources(self) -> List[SourceModel]:
        query = """
            SELECT name, lake_schema, description, cadence, writer
            FROM ops.lake_ops.source
            ORDER BY name
        """
        try:
            rows = await self._run_query(query)
            return [
                SourceModel(
                    name=r[0],
                    lake_schema=r[1],
                    description=r[2],
                    cadence=r[3],
                    writer=r[4]
                ) for r in rows
            ]
        except Exception as e:
            if "does not exist" in str(e) or "Table with name" in str(e):
                return []
            raise e

    async def get_runs(self, limit: int = 50) -> List[RunModel]:
        query = """
            SELECT run_id, source, target, version, status,
                   snapshot_before, snapshot_after, rows_after,
                   started_at::VARCHAR, finished_at::VARCHAR, error, host
            FROM ops.lake_ops.run
            ORDER BY started_at DESC
            LIMIT ?
        """
        try:
            rows = await self._run_query(query, [limit])
            return [
                RunModel(
                    run_id=r[0],
                    source=r[1],
                    target=r[2],
                    version=r[3],
                    status=r[4],
                    snapshot_before=r[5],
                    snapshot_after=r[6],
                    rows_after=r[7],
                    started_at=r[8],
                    finished_at=r[9],
                    error=r[10],
                    host=r[11]
                ) for r in rows
            ]
        except Exception as e:
            if "does not exist" in str(e) or "Table with name" in str(e):
                return []
            raise e

    async def get_snapshots(self, limit: int = 50) -> List[SnapshotModel]:
        query = """
            SELECT snapshot_id, snapshot_time::VARCHAR, author, commit_message, commit_extra_info, changes
            FROM lake.snapshots()
            ORDER BY snapshot_id DESC
            LIMIT ?
        """
        try:
            rows = await self._run_query(query, [limit])
            results = []
            for r in rows:
                run_id = None
                extra_info = r[4]
                if extra_info:
                    try:
                        data = json.loads(extra_info)
                        run_id = data.get("run_id")
                    except Exception:
                        pass
                
                changes_dict = None
                if r[5]:
                    try:
                        changes_dict = dict(r[5])
                    except Exception:
                        pass

                results.append(
                    SnapshotModel(
                        snapshot_id=r[0],
                        timestamp=r[1],
                        author=r[2],
                        commit_message=r[3],
                        run_id=run_id,
                        changes=changes_dict
                    )
                )
            return results
        except Exception as e:
            if "does not exist" in str(e) or "Table with name" in str(e):
                return []
            raise e

    async def get_logs(self, run_id: str) -> List[LogModel]:
        query = """
            SELECT run_id, source, target, version, status,
                   snapshot_before, snapshot_after, rows_after,
                   started_at::VARCHAR, finished_at::VARCHAR, error, host
            FROM ops.lake_ops.run
            WHERE run_id = ?
        """
        try:
            rows = await self._run_query(query, [run_id])
            if not rows:
                return []
            
            r = rows[0]
            source = r[1]
            target = r[2]
            version = r[3]
            status = r[4]
            before = r[5]
            after = r[6]
            rows_after = r[7]
            started_at = r[8]
            finished_at = r[9]
            error = r[10]
            host = r[11]

            logs = []
            logs.append(
                LogModel(
                    timestamp=started_at,
                    level="INFO",
                    message=f"start → {target} (version={version}, snapshot_before={before}, run_id={run_id}) on host {host}",
                    logger_name=f"run:{source}",
                    run_id=run_id
                )
            )
            
            logs.append(
                LogModel(
                    timestamp=started_at,
                    level="INFO",
                    message="attaching DuckLake substrate connection...",
                    logger_name=f"run:{source}",
                    run_id=run_id
                )
            )
            logs.append(
                LogModel(
                    timestamp=started_at,
                    level="INFO",
                    message=f"connected to backend. running upsert for {target}...",
                    logger_name=f"run:{source}",
                    run_id=run_id
                )
            )

            ts_finished = finished_at or started_at
            if status == "success":
                logs.append(
                    LogModel(
                        timestamp=ts_finished,
                        level="SUCCESS",
                        message=f"success → {target} (rows={rows_after}, snapshot {before}→{after}, run_id={run_id})",
                        logger_name=f"run:{source}",
                        run_id=run_id
                    )
                )
            elif status == "idempotent":
                logs.append(
                    LogModel(
                        timestamp=ts_finished,
                        level="SUCCESS",
                        message=f"idempotent → {target} (rows={rows_after}, snapshot {before}→{after}, run_id={run_id})",
                        logger_name=f"run:{source}",
                        run_id=run_id
                    )
                )
            elif status == "error":
                logs.append(
                    LogModel(
                        timestamp=ts_finished,
                        level="ERROR",
                        message=f"ERROR after {rows_after} rows (snapshot {before}→{after}, run_id={run_id}): {error}",
                        logger_name=f"run:{source}",
                        run_id=run_id
                    )
                )
            else:
                logs.append(
                    LogModel(
                        timestamp=ts_finished,
                        level="INFO",
                        message=f"run in progress: upsert target={target} running...",
                        logger_name=f"run:{source}",
                        run_id=run_id
                    )
                )
                
            return logs
        except Exception as e:
            if "does not exist" in str(e) or "Table with name" in str(e):
                return []
            raise e
