export interface Source {
  name: string;
  lake_schema: string;
  description?: string;
  cadence?: string;
  writer: string;
}

export interface Run {
  run_id: string;
  source: string;
  target: string;
  version?: string;
  status: 'running' | 'success' | 'idempotent' | 'error';
  snapshot_before?: number;
  snapshot_after?: number;
  rows_after?: number;
  started_at: string;
  finished_at?: string;
  error?: string;
  host?: string;
}

export interface Snapshot {
  snapshot_id: number;
  timestamp: string;
  author?: string;
  commit_message?: string;
  run_id?: string;
  changes?: Record<string, string[]>;
}

export interface LogEntry {
  timestamp: string;
  level: 'INFO' | 'WARNING' | 'ERROR' | 'SUCCESS';
  message: string;
  logger_name?: string;
  run_id?: string;
}
