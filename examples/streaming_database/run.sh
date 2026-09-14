#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
if (($# > 1)); then
  printf 'Usage: %s [BUILD_DIR]\n' "$0" >&2
  exit 2
fi
build_dir=${1:-${EVENTFLOW_BUILD_DIR:-"$repo_root/build"}}

# Compile only this application's database, independent source server and SQL verifier.
# Existing caches preserve their configured build type and sanitizer policy.
if [[ ${EVENTFLOW_SKIP_BUILD:-0} != 1 && -f "$repo_root/CMakeLists.txt" ]]; then
  cmake_binary=${EVENTFLOW_CMAKE:-cmake}
  configure_args=(-S "$repo_root" -B "$build_dir" -G "Unix Makefiles")
  if [[ ! -f "$build_dir/CMakeCache.txt" ]]; then
    configure_args+=(-DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug)
  fi
  "$cmake_binary" "${configure_args[@]}"
  "$cmake_binary" --build "$build_dir" --target eventflow_streaming_database \
    eventflow_message_server eventflow_streaming_verify --parallel "${EVENTFLOW_BUILD_JOBS:-4}"
fi

binary_dir="$build_dir/bin"
if [[ ! -f "$repo_root/CMakeLists.txt" ]]; then
  binary_dir="$repo_root/bin"
fi
database_bin=${EVENTFLOW_DATABASE_BINARY:-"$binary_dir/eventflow_streaming_database"}
source_bin=${EVENTFLOW_MESSAGE_BINARY:-"$binary_dir/eventflow_message_server"}
verify_bin=${EVENTFLOW_DEMO_BINARY:-"$binary_dir/eventflow_streaming_verify"}
for executable in "$database_bin" "$source_bin" "$verify_bin"; do
  if [[ ! -x "$executable" ]]; then
    printf 'Missing executable: %s\n' "$executable" >&2
    printf 'Build the example targets or provide the runtime package binaries.\n' >&2
    exit 1
  fi
done

export ASAN_OPTIONS=${ASAN_OPTIONS-detect_leaks=1:halt_on_error=1}
export UBSAN_OPTIONS=${UBSAN_OPTIONS-halt_on_error=1:print_stacktrace=1}
export LSAN_OPTIONS=${LSAN_OPTIONS-exitcode=23}
export LD_LIBRARY_PATH="$repo_root/cpp3rdlib/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/eventflow-streaming-XXXXXX")
pids=()
labels=()

cleanup() {
  local result=$? failed=0 index pid attempt child_status
  trap - EXIT INT TERM
  # Stop dependents first, then shared KV and the independent message service.
  for ((index=${#pids[@]}-1; index>=0; --index)); do
    pid=${pids[index]}
    for ((attempt=0; attempt<600; ++attempt)); do
      if ! kill -0 "$pid" 2>/dev/null; then break; fi
      if ((attempt % 20 == 0)); then kill -TERM "$pid" 2>/dev/null || true; fi
      sleep 0.05
    done
    if kill -0 "$pid" 2>/dev/null; then
      printf '%s did not stop within 30 seconds; sending SIGKILL.\n' "${labels[index]}" >&2
      kill -KILL "$pid" 2>/dev/null || true
      failed=1
    fi
    if wait "$pid"; then
      printf 'Stopped %s pid=%s exit=0\n' "${labels[index]}" "$pid"
    else
      child_status=$?
      printf 'Stopped %s pid=%s exit=%s\n' "${labels[index]}" "$pid" "$child_status" >&2
      failed=1
    fi
  done
  printf 'Streaming example logs/configuration/data: %s\n' "$work_dir"
  if ((result == 0 && failed != 0)); then result=1; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ready_port=
launch() {
  local marker=$1 label=$2 pid attempt reported_marker port rest
  shift 2
  : >"$work_dir/$label.log"
  "$@" >"$work_dir/$label.log" 2>&1 &
  pid=$!
  pids+=("$pid")
  labels+=("$label")
  for ((attempt=0; attempt<600; ++attempt)); do
    while IFS=' ' read -r reported_marker port rest; do
      if [[ "$reported_marker" == "$marker" && "$port" =~ ^[0-9]+$ ]] &&
          ((port > 0 && port <= 65535)); then
        if ! kill -0 "$pid" 2>/dev/null; then cat "$work_dir/$label.log" >&2; return 1; fi
        ready_port=$port
        printf '%s pid=%s port=%s\n' "$marker" "$pid" "$port"
        return 0
      fi
    done <"$work_dir/$label.log"
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$work_dir/$label.log" >&2
      return 1
    fi
    sleep 0.05
  done
  printf 'Timed out waiting for %s\n' "$marker" >&2
  cat "$work_dir/$label.log" >&2
  return 1
}

printf 'Configuration and logs: %s\n' "$work_dir"
launch HTTP_MESSAGE_SOURCE_READY messages "$source_bin" --port 0
source_port=$ready_port
launch EVENTFLOW_KV_READY kv "$database_bin" kv --backend rocks --port 0 --shards 3 \
  --data-dir "$work_dir/kv-data"
kv_port=$ready_port
cat >"$work_dir/compute-a.json" <<JSON
{"node_id":"compute-a","kv_endpoint":"http://127.0.0.1:$kv_port","port":0,"shard_count":2,"shards":[0]}
JSON
cat >"$work_dir/compute-b.json" <<JSON
{"node_id":"compute-b","kv_endpoint":"http://127.0.0.1:$kv_port","port":0,"shard_count":2,"shards":[1]}
JSON
launch EVENTFLOW_COMPUTE_READY compute-a "$database_bin" compute --config "$work_dir/compute-a.json"
a_port=$ready_port
launch EVENTFLOW_COMPUTE_READY compute-b "$database_bin" compute --config "$work_dir/compute-b.json"
b_port=$ready_port
cat >"$work_dir/meta.json" <<JSON
{"cluster_id":"streaming-market","kv_endpoint":"http://127.0.0.1:$kv_port","port":0,"shard_count":2,"tick_interval_ms":20,"nodes":[{"node_id":"compute-a","endpoint":"http://127.0.0.1:$a_port","shards":[0]},{"node_id":"compute-b","endpoint":"http://127.0.0.1:$b_port","shards":[1]}],"source":{"type":"http","endpoint":"http://127.0.0.1:$source_port","source_id":"market_feed","relation":"trades","assignments":[{"topic":"trades","partition":0,"next_offset":"0"},{"topic":"trades","partition":1,"next_offset":"0"}],"poll_interval_ms":10,"max_message_bytes":262144}}
JSON
launch EVENTFLOW_COORDINATOR_READY meta "$database_bin" coordinator --config "$work_dir/meta.json"
meta_port=$ready_port
printf 'Message service: http://127.0.0.1:%s\nMeta: http://127.0.0.1:%s\nKV data: %s/kv-data\n' \
  "$source_port" "$meta_port" "$work_dir"
"$verify_bin" "http://127.0.0.1:$meta_port" "http://127.0.0.1:$source_port" |
  tee "$work_dir/verify.log"
