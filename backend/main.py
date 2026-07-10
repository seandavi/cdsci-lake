from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List

from models import SourceModel, RunModel, SnapshotModel, LogModel
from repository import DuckDBDashboardRepository

app = FastAPI(
    title="DuckLake Operations Dashboard API",
    description="Backend API for monitoring DuckLake ingestions and commits",
    version="1.0.0"
)

# CORS middleware config for development integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instantiate concrete repository
repo = DuckDBDashboardRepository()

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "ducklake-dashboard-api"}

@app.get("/api/sources", response_model=List[SourceModel])
async def get_sources():
    try:
        return await repo.get_sources()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/runs", response_model=List[RunModel])
async def get_runs(limit: int = 50):
    try:
        return await repo.get_runs(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/snapshots", response_model=List[SnapshotModel])
async def get_snapshots(limit: int = 50):
    try:
        return await repo.get_snapshots(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/logs/{run_id}", response_model=List[LogModel])
async def get_logs(run_id: str):
    try:
        logs = await repo.get_logs(run_id)
        return logs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
