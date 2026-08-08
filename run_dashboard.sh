#!/usr/bin/env bash

# Setup clean shutdown trap
trap cleanup INT TERM

cleanup() {
  echo ""
  echo "--------------------------------------------------"
  echo " Stopping DuckLake Dashboard services..."
  if [ -n "$BACKEND_PID" ]; then
    echo "Stopping backend (PID: $BACKEND_PID)..."
    kill "$BACKEND_PID" 2>/dev/null || true
  fi
  if [ -n "$FRONTEND_PID" ]; then
    echo "Stopping frontend (PID: $FRONTEND_PID)..."
    kill "$FRONTEND_PID" 2>/dev/null || true
  fi
  echo "Dashboard services stopped."
  echo "--------------------------------------------------"
  exit 0
}

echo "=================================================="
echo " Starting DuckLake Operations Dashboard"
echo "=================================================="

# 1. Start FastAPI backend
echo "Starting FastAPI backend on http://127.0.0.1:8000..."
PYTHONPATH=src:backend uv run uvicorn backend.main:app --port 8000 --host 0.0.0.0 > backend.log 2>&1 &
BACKEND_PID=$!

# Wait a brief moment to ensure backend started
sleep 2
if ! kill -0 $BACKEND_PID 2>/dev/null; then
  echo "Failed to start FastAPI backend. Check backend.log for details."
  if [ -f backend.log ]; then
    cat backend.log
  fi
  exit 1
fi
echo "Backend running (PID: $BACKEND_PID, logs at backend.log)."

# 2. Start Vite frontend
echo "Starting Vite dev server..."
cd frontend
npm run dev --host=0.0.0.0 &
FRONTEND_PID=$!
cd ..

echo "Vite dev server running (PID: $FRONTEND_PID)."
echo "Dashboard is ready!"
echo "Press CTRL+C to stop both servers."
echo "=================================================="

# Wait for both background jobs
wait
