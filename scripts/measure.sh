#!/bin/sh
# Development-only harness; the submission CMD remains /app/run.sh.
set +e
timeout --signal=TERM --kill-after=10 "${RUN_TIMEOUT:-180}" /app/run.sh
status=$?
if [ -r /sys/fs/cgroup/memory.peak ]; then
  printf 'cgroup_memory_peak_bytes=' >&2
  cat /sys/fs/cgroup/memory.peak >&2
fi
if [ -r /sys/fs/cgroup/memory.stat ]; then
  while read -r key value; do
    case "$key" in
      pgfault|pgmajfault|anon|file|file_mapped) printf 'cgroup_%s=%s\n' "$key" "$value" >&2 ;;
    esac
  done < /sys/fs/cgroup/memory.stat
fi
exit "$status"
