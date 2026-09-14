#!/usr/bin/env bash
set -euo pipefail

source_dir=${EVENTFLOW_SOURCE_DIR:-/opt/eventflow}
build_jobs=${EVENTFLOW_BUILD_JOBS:-4}
test_jobs=${EVENTFLOW_TEST_JOBS:-2}
release_dir="$source_dir/build-docker-release"
debug_dir="$source_dir/build-docker-debug"
demo_names=()

usage() {
  cat <<'EOF'
EventFlow reviewer image

Mount a complete source checkout at /opt/eventflow when using build-env.

Commands:
  demo [all|NAME]   Run Release demos after an explicit build (default selection: all).
  review           Rebuild Release from source, then run all demos.
  build [release|debug]
                   Build the source with Clang and Unix Makefiles.
  test             Build Debug and run all CTest targets with sanitizers.
  shell [ARGS...]  Open Bash in the source directory.
  help             Show this message (default command).

Demos:
  Names are discovered from examples/*/run.sh in the mounted source.
  Run build release before demo, or use review to build and run together.

Environment:
  EVENTFLOW_BUILD_JOBS   Parallel compiler jobs (default: 4).
  EVENTFLOW_TEST_JOBS    Parallel CTest targets (default: 2).
  TMPDIR                Parent directory for demo logs and temporary data.
EOF
}

fail_usage() {
  printf 'Error: %s\n' "$1" >&2
  usage >&2
  exit 2
}

build_project() {
  local mode=$1 clean=${2:-0} build_dir build_type testing
  case "$mode" in
    release) build_dir=$release_dir; build_type=Release; testing=OFF ;;
    debug) build_dir=$debug_dir; build_type=Debug; testing=ON ;;
    *) fail_usage "Unknown build mode: $mode" ;;
  esac
  cmake -S "$source_dir" -B "$build_dir" -G 'Unix Makefiles' \
    -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE="$build_type" \
    -DBUILD_TESTING="$testing" -DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld
  local build_args=(--build "$build_dir" --parallel "$build_jobs")
  if [[ "$clean" == 1 ]]; then build_args+=(--clean-first); fi
  cmake "${build_args[@]}"
}

run_demo() {
  local name=$1 allowed=0 candidate
  for candidate in "${demo_names[@]}"; do
    if [[ "$candidate" == "$name" ]]; then allowed=1; break; fi
  done
  if [[ "$allowed" != 1 ]]; then fail_usage "Unknown demo: $name"; fi
  printf '\nRunning %s\n' "$name"
  # Tini signals the process group once. Bash defers these traps until the
  # foreground demo finishes, allowing its service cleanup to complete.
  trap 'exit 130' INT
  trap 'exit 143' TERM
  EVENTFLOW_BUILD_DIR="$release_dir" EVENTFLOW_SKIP_BUILD=1 \
    bash "$source_dir/examples/${name}/run.sh"
  trap - INT TERM
  printf 'PASS: %s\n' "$name"
}

run_demos() {
  local name=$1
  if [[ "$name" == all ]]; then
    for name in "${demo_names[@]}"; do run_demo "$name"; done
    printf '\nPASS: all %s Release demos\n' "${#demo_names[@]}"
  else
    run_demo "$name"
  fi
}

command_name=${1:-help}
if (($# > 0)); then shift; fi
case "$command_name" in
  help|-h|--help) usage; exit 0 ;;
esac
[[ "$build_jobs" =~ ^[1-9][0-9]*$ ]] || fail_usage 'EVENTFLOW_BUILD_JOBS must be a positive integer.'
[[ "$test_jobs" =~ ^[1-9][0-9]*$ ]] || fail_usage 'EVENTFLOW_TEST_JOBS must be a positive integer.'
if [[ "$command_name" != shell && ! -f "$source_dir/CMakeLists.txt" ]]; then
  fail_usage "Mount a complete source checkout at $source_dir."
fi
cd "$source_dir"
if [[ "$command_name" == demo || "$command_name" == review ]]; then
  for demo_script in "$source_dir"/examples/*/run.sh; do
    if [[ -f "$demo_script" ]]; then
      demo_name=${demo_script%/run.sh}
      demo_names+=("${demo_name##*/}")
    fi
  done
  ((${#demo_names[@]} > 0)) || fail_usage 'No runnable demos found in the source checkout.'
fi

case "$command_name" in
  demo)
    (($# <= 1)) || fail_usage 'demo accepts at most one name.'
    run_demos "${1:-all}"
    ;;
  review)
    (($# == 0)) || fail_usage 'review accepts no arguments.'
    build_project release 1
    run_demos all
    ;;
  build)
    (($# <= 1)) || fail_usage 'build accepts at most one mode.'
    build_project "${1:-release}"
    ;;
  test)
    (($# == 0)) || fail_usage 'test accepts no arguments.'
    build_project debug
    exec ctest --test-dir "$debug_dir" --output-on-failure --parallel "$test_jobs"
    ;;
  shell)
    exec bash "$@"
    ;;
  *) fail_usage "Unknown command: $command_name" ;;
esac
