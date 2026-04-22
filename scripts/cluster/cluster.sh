#!/usr/bin/env bash
# Minimal control script for a 3-node local nats cluster used for
# manually exercising PullConsumer failover behavior.
#
# Usage:
#   scripts/cluster/cluster.sh start           # start all 3
#   scripts/cluster/cluster.sh start n1        # start just n1
#   scripts/cluster/cluster.sh stop            # stop all 3 (SIGTERM)
#   scripts/cluster/cluster.sh stop n2         # stop just n2
#   scripts/cluster/cluster.sh kill n2         # SIGKILL n2 (hard fail)
#   scripts/cluster/cluster.sh status          # show what's running
#   scripts/cluster/cluster.sh clean           # stop all + wipe data dirs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PID_DIR="$SCRIPT_DIR/pids"
LOG_DIR="$SCRIPT_DIR/logs"
DATA_DIR="$SCRIPT_DIR/data"

mkdir -p "$PID_DIR" "$LOG_DIR" "$DATA_DIR"

nodes=(n1 n2 n3)

port_for() {
  case "$1" in
    n1) echo 4223 ;;
    n2) echo 4224 ;;
    n3) echo 4225 ;;
    *)  echo "unknown node: $1" >&2; return 1 ;;
  esac
}

is_running() {
  local node=$1
  local pidfile="$PID_DIR/$node.pid"
  [[ -f "$pidfile" ]] || return 1
  local pid
  pid=$(cat "$pidfile")
  kill -0 "$pid" 2>/dev/null
}

start_node() {
  local node=$1
  if is_running "$node"; then
    echo "$node already running (pid $(cat "$PID_DIR/$node.pid"))"
    return 0
  fi

  local conf="$SCRIPT_DIR/$node.conf"
  local log="$LOG_DIR/$node.log"
  local pidfile="$PID_DIR/$node.pid"

  # Run from repo root so the relative store_dir in the conf resolves.
  (
    cd "$REPO_ROOT"
    nohup nats-server -c "$conf" >"$log" 2>&1 &
    echo $! > "$pidfile"
  )
  sleep 0.3
  if is_running "$node"; then
    echo "$node started (pid $(cat "$pidfile"), port $(port_for "$node"), log $log)"
  else
    echo "$node failed to start — check $log" >&2
    return 1
  fi
}

stop_node() {
  local node=$1
  local signal=${2:-TERM}
  if ! is_running "$node"; then
    echo "$node not running"
    rm -f "$PID_DIR/$node.pid"
    return 0
  fi
  local pid
  pid=$(cat "$PID_DIR/$node.pid")
  kill "-$signal" "$pid"
  echo "$node sent SIG$signal (pid $pid)"
  rm -f "$PID_DIR/$node.pid"
}

status() {
  for node in "${nodes[@]}"; do
    if is_running "$node"; then
      echo "$node  RUNNING  pid=$(cat "$PID_DIR/$node.pid")  port=$(port_for "$node")"
    else
      echo "$node  stopped              port=$(port_for "$node")"
    fi
  done
}

cmd=${1:-}
shift || true

targets=()
if (( $# > 0 )); then
  targets=("$@")
else
  targets=("${nodes[@]}")
fi

case "$cmd" in
  start)
    for n in "${targets[@]}"; do start_node "$n"; done
    ;;
  stop)
    for n in "${targets[@]}"; do stop_node "$n" TERM; done
    ;;
  kill)
    for n in "${targets[@]}"; do stop_node "$n" KILL; done
    ;;
  status)
    status
    ;;
  clean)
    for n in "${nodes[@]}"; do stop_node "$n" KILL || true; done
    rm -rf "$DATA_DIR" "$LOG_DIR" "$PID_DIR"
    echo "cleaned data, logs, pids"
    ;;
  *)
    echo "usage: $0 {start|stop|kill|status|clean} [n1|n2|n3]..." >&2
    exit 1
    ;;
esac
