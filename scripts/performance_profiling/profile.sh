#!/usr/bin/env bash
# profile_capture.sh – launch Mithril and capture heap/CPU/goroutine pprof,
# full-memory smaps_rollup, and ps stats on a fixed interval.
#
# Usage:
#   ./profile_capture.sh 30 ./profile_runs mithril_run -- mithril verifier ...
# -----------------------------------------------------------------------------

# ----------- Configuration ---------------------------------------------------
INTERVAL=${1:-30}          # seconds between capture cycles
CPU_SECONDS=${CPU_SECONDS:-5}   # pprof CPU sample length (set env or edit)
BASE_DIR=${2:-"./profile_runs"}
LOG_PREFIX=${3:-"mithril_run"}
PPROF_PORT=6060            # mithril's net/http/pprof port
# -----------------------------------------------------------------------------

# ---------- arg parsing / setup ----------------------------------------------
if [ "$#" -lt 4 ]; then
  echo "Usage: $0 [interval] [profile-dir] [log-prefix] -- <mithril args>"
  exit 1
fi
shift 3; [[ $1 == "--" ]] || { echo "missing --"; exit 1; }; shift
MITHRIL_CMD=("$@"); [[ ${#MITHRIL_CMD[@]} -gt 0 ]] || { echo "no mithril cmd"; exit 1; }

RUN_TS=$(date +"%Y%m%d_%H%M%S")
RUN_DIR="${BASE_DIR}/${RUN_TS}_interval_${INTERVAL}s"
LOG_FILE="${RUN_DIR}/${LOG_PREFIX}.log"
MAP_CSV="${RUN_DIR}/profile_map.csv"
mkdir -p "$RUN_DIR" || { echo "mkdir $RUN_DIR failed"; exit 1; }

# ---------- cleanup handler ---------------------------------------------------
cleanup(){ [[ -n $MITHRIL_PID ]] && kill $MITHRIL_PID 2>/dev/null; }
trap cleanup EXIT INT TERM

echo "Profiles -> $RUN_DIR"
echo "Log      -> $LOG_FILE"

# CSV header
echo "Timestamp,Heap,CPU,Goroutine,Smaps,GoroutineCnt,CPU%,RSS_KB,VSZ_KB,LastLog" > "$MAP_CSV"

# ----------- start Mithril ----------------------------------------------------
if command -v unbuffer &>/dev/null; then
  unbuffer "${MITHRIL_CMD[@]}" 2>&1 | tee -a "$LOG_FILE" &
else
  "${MITHRIL_CMD[@]}" 2>&1 | tee -a "$LOG_FILE" &
fi
MITHRIL_PID=$!
sleep 5

# ------------- main loop ------------------------------------------------------
seq=0
while kill -0 $MITHRIL_PID 2>/dev/null; do
  ts=$(date +"%Y%m%d_%H%M%S")
  heap="heap_${ts}.pprof"; cpu="cpu_${ts}.pprof"; gor="goroutine_${ts}.pprof"; smaps="smaps_${ts}.txt"
  gcount="NA" sys_cpu="NA" sys_rss="NA" sys_vsz="NA"

  # --- smaps_rollup ---
    if sudo -n cat "/proc/$MITHRIL_PID/smaps_rollup" > "$RUN_DIR/$smaps" 2>/dev/null; then
        echo "[$ts] smaps saved ($smaps)" | tee -a "$LOG_FILE"
    else
        smaps="ERR"
        echo "[$ts] ERROR: sudo cat smaps_rollup failed for PID $MITHRIL_PID" | tee -a "$LOG_FILE"
    fi

  # ps stats
  read -r sys_cpu sys_rss sys_vsz <<<"$(ps -p $MITHRIL_PID -o %cpu=,rss=,vsz= --no-headers)"
  echo "[$ts] CPU=${sys_cpu}% RSS=${sys_rss}KB" | tee -a "$LOG_FILE"

  # pprof endpoints
  if curl -fs "http://localhost:$PPROF_PORT/debug/pprof/" &>/dev/null; then
    curl -fs "http://localhost:$PPROF_PORT/debug/pprof/goroutine?debug=1" -o "$RUN_DIR/$gor"
    gcount=$(grep -m1 '^goroutine [0-9]\+ \[' "$RUN_DIR/$gor" | grep -o '[0-9]\+')
    curl -fs "http://localhost:$PPROF_PORT/debug/pprof/heap"      -o "$RUN_DIR/$heap"
    curl -fs "http://localhost:$PPROF_PORT/debug/pprof/profile?seconds=$CPU_SECONDS" -o "$RUN_DIR/$cpu"
  else
    heap=cpu=gor="NO_PPROF"; echo "[$ts] pprof unreachable" | tee -a "$LOG_FILE"
  fi

  last_log=$(tail -n1 "$LOG_FILE" | tr -d '\n' | sed 's/\"/\"\"/g')
  echo "$ts,$heap,$cpu,$gor,$smaps,$gcount,$sys_cpu,$sys_rss,$sys_vsz,\"$last_log\"" >> "$MAP_CSV"

  echo "[$ts] cycle $seq done" | tee -a "$LOG_FILE"; seq=$((seq+1))
  sleep "$(( INTERVAL>CPU_SECONDS ? INTERVAL-CPU_SECONDS : 1 ))"
done

echo "Mithril exited; profiler stopping." | tee -a "$LOG_FILE"
