from pydantic import BaseModel
from typing import Optional, Dict, List

class SourceModel(BaseModel):
    name: str
    lake_schema: str
    description: Optional[str] = None
    cadence: Optional[str] = None
    writer: str

class RunModel(BaseModel):
    run_id: str
    source: str
    target: str
    version: Optional[str] = None
    status: str
    snapshot_before: Optional[int] = None
    snapshot_after: Optional[int] = None
    rows_after: Optional[int] = None
    started_at: str
    finished_at: Optional[str] = None
    error: Optional[str] = None
    host: Optional[str] = None

class SnapshotModel(BaseModel):
    snapshot_id: int
    timestamp: str
    author: Optional[str] = None
    commit_message: Optional[str] = None
    run_id: Optional[str] = None
    changes: Optional[Dict[str, List[str]]] = None

class LogModel(BaseModel):
    timestamp: str
    level: str
    message: str
    logger_name: Optional[str] = None
    run_id: Optional[str] = None
