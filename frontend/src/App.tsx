import { useState, useEffect, useCallback } from 'react';
import { 
  RefreshCw, CheckCircle2, AlertCircle, Activity, 
  Terminal, X, GitCommit, Database, 
  Cpu, Server, Play, Search, AlertOctagon, HelpCircle
} from 'lucide-react';
import './styles/dashboard.css';
import type { Source, Run, Snapshot, LogEntry } from './interfaces';

const API_BASE = 'http://localhost:8000';

function App() {
  const [sources, setSources] = useState<Source[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [apiOnline, setApiOnline] = useState<boolean | null>(null);
  
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [activeRunLogs, setActiveRunLogs] = useState<LogEntry[]>([]);
  const [loadingLogs, setLoadingLogs] = useState(false);
  
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('all');

  const fetchData = useCallback(async () => {
    setIsRefreshing(true);
    try {
      // Check health
      const healthRes = await fetch(`${API_BASE}/health`);
      if (healthRes.ok) {
        setApiOnline(true);
      } else {
        setApiOnline(false);
      }

      // Fetch metrics
      const [sourcesRes, runsRes, snapshotsRes] = await Promise.all([
        fetch(`${API_BASE}/api/sources`),
        fetch(`${API_BASE}/api/runs`),
        fetch(`${API_BASE}/api/snapshots`)
      ]);

      if (sourcesRes.ok) setSources(await sourcesRes.json());
      if (runsRes.ok) setRuns(await runsRes.json());
      if (snapshotsRes.ok) setSnapshots(await snapshotsRes.json());

    } catch (err) {
      console.error('Error fetching dashboard data:', err);
      setApiOnline(false);
    } finally {
      setIsRefreshing(false);
    }
  }, []);

  // Fetch data on load and configure polling
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 8000);
    return () => clearInterval(interval);
  }, [fetchData]);

  // Open Log Modal and fetch logs
  const viewLogs = async (runId: string) => {
    setActiveRunId(runId);
    setLoadingLogs(true);
    try {
      const res = await fetch(`${API_BASE}/api/logs/${runId}`);
      if (res.ok) {
        setActiveRunLogs(await res.json());
      } else {
        setActiveRunLogs([]);
      }
    } catch (err) {
      console.error('Error fetching logs:', err);
      setActiveRunLogs([]);
    } finally {
      setLoadingLogs(false);
    }
  };

  // Metrics calculators
  const totalRuns = runs.length;
  const activeRuns = runs.filter(r => r.status === 'running').length;
  const successfulRuns = runs.filter(r => r.status === 'success' || r.status === 'idempotent').length;
  const successRate = totalRuns > 0 ? Math.round((successfulRuns / totalRuns) * 100) : 100;
  const latestSnapshot = snapshots.length > 0 ? snapshots[0].snapshot_id : 'None';

  // Filtered runs
  const filteredRuns = runs.filter(run => {
    const matchesSearch = 
      run.source.toLowerCase().includes(searchQuery.toLowerCase()) ||
      run.target.toLowerCase().includes(searchQuery.toLowerCase()) ||
      run.run_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (run.version || '').toLowerCase().includes(searchQuery.toLowerCase());
      
    const matchesStatus = statusFilter === 'all' || run.status === statusFilter;
    
    return matchesSearch && matchesStatus;
  });

  return (
    <div className="dashboard-container">
      {/* Header */}
      <header className="dashboard-header">
        <div className="brand-section">
          <Activity className="logo-icon" size={28} />
          <div className="brand-title">
            <h1>DuckLake Operations</h1>
            <p>Unified Ingestion Ledger & Versioning Dashboard</p>
          </div>
        </div>
        <div className="refresh-section">
          <div className={`api-status ${apiOnline ? '' : 'offline'}`}>
            <span className={`status-dot ${apiOnline ? 'online' : 'offline'}`} />
            {apiOnline ? 'API Connected' : 'API Offline'}
          </div>
          <button 
            className="refresh-button" 
            onClick={fetchData} 
            disabled={isRefreshing}
          >
            <RefreshCw size={16} className={isRefreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </header>

      {/* Stats Cards */}
      <section className="stats-grid">
        <div className="stat-card total">
          <div className="stat-icon-container">
            <Database size={24} />
          </div>
          <div className="stat-info">
            <span className="stat-value">{totalRuns}</span>
            <span className="stat-label">Total Ingestion Runs</span>
          </div>
        </div>
        <div className="stat-card running">
          <div className="stat-icon-container">
            <Play size={24} />
          </div>
          <div className="stat-info">
            <span className="stat-value">{activeRuns}</span>
            <span className="stat-label">Active / Running</span>
          </div>
        </div>
        <div className="stat-card success">
          <div className="stat-icon-container">
            <CheckCircle2 size={24} />
          </div>
          <div className="stat-info">
            <span className="stat-value">{successRate}%</span>
            <span className="stat-label">Success Rate</span>
          </div>
        </div>
        <div className="stat-card error">
          <div className="stat-icon-container">
            <AlertCircle size={24} />
          </div>
          <div className="stat-info">
            <span className="stat-value">{latestSnapshot}</span>
            <span className="stat-label">Latest Snapshot ID</span>
          </div>
        </div>
      </section>

      {/* Main split grid */}
      <div className="main-grid">
        {/* Left side: Runs */}
        <section className="section-block">
          <div className="section-header">
            <h2>
              <Server size={18} />
              Recent Ingestion Logs ({filteredRuns.length} runs)
            </h2>
            <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
              {/* Status Filter */}
              <select 
                value={statusFilter} 
                onChange={(e) => setStatusFilter(e.target.value)}
                style={{
                  background: 'var(--bg-main)',
                  color: 'var(--text-primary)',
                  border: '1px solid var(--border-color)',
                  padding: '0.4rem 0.6rem',
                  borderRadius: '0.375rem',
                  fontSize: '0.8rem',
                  outline: 'none'
                }}
              >
                <option value="all">All Statuses</option>
                <option value="success">Success</option>
                <option value="idempotent">Idempotent</option>
                <option value="running">Running</option>
                <option value="error">Error</option>
              </select>
              {/* Search */}
              <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
                <Search size={14} style={{ position: 'absolute', left: '8px', color: 'var(--text-muted)' }} />
                <input 
                  type="text" 
                  placeholder="Filter runs..." 
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  style={{
                    background: 'var(--bg-main)',
                    color: 'var(--text-primary)',
                    border: '1px solid var(--border-color)',
                    padding: '0.4rem 0.6rem 0.4rem 1.75rem',
                    borderRadius: '0.375rem',
                    fontSize: '0.8rem',
                    width: '180px',
                    outline: 'none'
                  }}
                />
              </div>
            </div>
          </div>

          <div className="table-container">
            {filteredRuns.length === 0 ? (
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '240px', color: 'var(--text-muted)' }}>
                <HelpCircle size={32} style={{ marginBottom: '0.5rem' }} />
                <span>No ingestion runs match the criteria</span>
              </div>
            ) : (
              <table className="runs-table">
                <thead>
                  <tr>
                    <th>Source</th>
                    <th>Target Dataset</th>
                    <th>Version</th>
                    <th>Rows Written</th>
                    <th>Started</th>
                    <th>Status</th>
                    <th>Logs</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRuns.map((run) => (
                    <tr key={run.run_id}>
                      <td style={{ fontWeight: 600, color: 'white' }}>{run.source}</td>
                      <td className="font-mono">{run.target}</td>
                      <td>{run.version || <span style={{ color: 'var(--text-muted)' }}>-</span>}</td>
                      <td className="font-mono">
                        {run.rows_after !== null && run.rows_after !== undefined 
                          ? run.rows_after.toLocaleString() 
                          : <span style={{ color: 'var(--text-muted)' }}>-</span>}
                      </td>
                      <td className="timeline-time">
                        {run.started_at ? run.started_at.split('.')[0].replace('T', ' ') : '-'}
                      </td>
                      <td>
                        <span className={`badge ${run.status}`}>
                          {run.status === 'running' && <RefreshCw size={10} className="animate-spin" />}
                          {run.status}
                        </span>
                      </td>
                      <td>
                        <button 
                          className="btn-icon" 
                          title="View system logs"
                          onClick={() => viewLogs(run.run_id)}
                        >
                          <Terminal size={14} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Sources List footer */}
          {sources.length > 0 && (
            <div style={{ marginTop: '1rem', borderTop: '1px solid var(--border-color)', paddingTop: '1rem' }}>
              <h4 style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', marginBottom: '0.5rem' }}>Registered Sources ({sources.length})</h4>
              <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                {sources.map(s => (
                  <span key={s.name} className="timeline-author" style={{ fontSize: '0.75rem' }}>
                    {s.name} ({s.lake_schema})
                  </span>
                ))}
              </div>
            </div>
          )}
        </section>

        {/* Right side: Snapshots */}
        <section className="section-block">
          <div className="section-header">
            <h2>
              <GitCommit size={18} />
              Lake Commits
            </h2>
          </div>

          <div className="timeline">
            {snapshots.length === 0 ? (
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '240px', color: 'var(--text-muted)' }}>
                <GitCommit size={32} style={{ marginBottom: '0.5rem' }} />
                <span>No snapshots recorded in DuckLake catalog</span>
              </div>
            ) : (
              snapshots.map((snap) => (
                <div className="timeline-item" key={snap.snapshot_id}>
                  <div className="timeline-dot" />
                  <span className="timeline-time">
                    {snap.timestamp ? snap.timestamp.split('.')[0].replace('T', ' ') : ''}
                  </span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <span className="font-mono" style={{ color: 'var(--primary)', fontWeight: 600 }}>
                      v{snap.snapshot_id}
                    </span>
                    <span className="timeline-message">{snap.commit_message || 'No commit message'}</span>
                  </div>
                  <div className="timeline-meta">
                    {snap.author && (
                      <span className="timeline-author">
                        <Cpu size={10} style={{ marginRight: '0.2rem' }} />
                        {snap.author}
                      </span>
                    )}
                    {snap.changes && Object.keys(snap.changes).length > 0 && (
                      <span className="timeline-author" style={{ color: 'var(--info)' }}>
                        {Object.keys(snap.changes).length} changes
                      </span>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </section>
      </div>

      {/* Log Modal */}
      {activeRunId && (
        <div className="modal-overlay" onClick={() => setActiveRunId(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <div>
                <h3>System Log Inspector</h3>
                <p className="modal-header-meta font-mono">Run ID: {activeRunId}</p>
              </div>
              <button className="btn-close" onClick={() => setActiveRunId(null)}>
                <X size={20} />
              </button>
            </div>
            
            <div className="modal-body">
              {loadingLogs ? (
                <div className="empty-logs">
                  <RefreshCw size={24} className="animate-spin" style={{ marginRight: '0.5rem' }} />
                  <span>Loading ingestion logs...</span>
                </div>
              ) : activeRunLogs.length === 0 ? (
                <div className="empty-logs">
                  <AlertOctagon size={24} style={{ marginRight: '0.5rem' }} />
                  <span>No log entries available for this run</span>
                </div>
              ) : (
                activeRunLogs.map((log, index) => (
                  <div key={index} className={`log-line ${log.level.toLowerCase()}`}>
                    <span className="log-timestamp">{log.timestamp ? log.timestamp.split('.')[0].replace('T', ' ') : ''}</span>
                    <span className={`log-level ${log.level.toLowerCase()}`}>{log.level}</span>
                    {log.logger_name && <span className="log-ctx">[{log.logger_name}]</span>}
                    <span>{log.message}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
