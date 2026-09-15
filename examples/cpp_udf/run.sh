#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
if (($# > 1)); then
  printf 'Usage: %s [BUILD_DIR]\n' "$0" >&2
  exit 2
fi
build_dir=${1:-${EVENTFLOW_BUILD_DIR:-"$repo_root/build"}}

if [[ -f "$repo_root/CMakeLists.txt" ]]; then
  if [[ ${EVENTFLOW_SKIP_BUILD:-0} != 1 ]]; then
    cmake_binary=${EVENTFLOW_CMAKE:-cmake}
    configure_args=(-S "$repo_root" -B "$build_dir" -G "Unix Makefiles")
    if [[ ! -f "$build_dir/CMakeCache.txt" ]]; then
      configure_args+=(-DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug)
    fi
    "$cmake_binary" "${configure_args[@]}"
    "$cmake_binary" --build "$build_dir" --target eventflow_cpp_udf_demo \
      --parallel "${EVENTFLOW_BUILD_JOBS:-4}"
  fi
  artifact_root=$(CDPATH= cd -- "$build_dir" && pwd)
else
  artifact_root=$repo_root
fi

host_binary=${EVENTFLOW_UDF_HOST:-"$artifact_root/bin/eventflow_cpp_udf_demo"}
library_path=${EVENTFLOW_UDF_LIBRARY:-"$artifact_root/lib/libeventflow_news_udf.so"}
if [[ ! -x "$host_binary" || ! -f "$library_path" ]]; then
  printf 'Missing UDF example executable or shared library.\n' >&2
  exit 1
fi

export ASAN_OPTIONS=${ASAN_OPTIONS-detect_leaks=1:halt_on_error=1}
export UBSAN_OPTIONS=${UBSAN_OPTIONS-halt_on_error=1:print_stacktrace=1}
export LSAN_OPTIONS=${LSAN_OPTIONS-exitcode=23}
exec "$host_binary" "$library_path"
