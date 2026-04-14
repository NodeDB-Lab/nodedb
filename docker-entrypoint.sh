#!/bin/sh
# NodeDB container entrypoint.
#
# When invoked as root (the default for `docker run` with no --user), fix
# ownership of NODEDB_DATA_DIR and drop privileges to the unprivileged
# `nodedb` user before exec'ing the server. When invoked as any other UID
# (e.g. `--user 10001` or via Kubernetes runAsUser), exec directly and
# leave the data directory alone.
#
# This makes `-v <named-volume>:/var/lib/nodedb` work even when Docker
# initialises the named volume as root-owned (common on Linux hosts where
# the volume is created out-of-band before the container's first run).

set -e

DATA_DIR="${NODEDB_DATA_DIR:-/var/lib/nodedb}"

if [ "$(id -u)" = "0" ]; then
    # Running as root: ensure the data dir exists and is owned by nodedb,
    # then drop privileges. mkdir is a no-op for the declared VOLUME but
    # protects against custom NODEDB_DATA_DIR overrides.
    mkdir -p "$DATA_DIR"
    chown -R nodedb:nodedb "$DATA_DIR"
    exec gosu nodedb "$@"
fi

# Already non-root: ensure we can actually write to the data dir, otherwise
# fail fast with a clear message instead of the cryptic WAL "Permission
# denied (os error 13)" the user sees on a misconfigured volume mount.
if [ ! -w "$DATA_DIR" ]; then
    cat >&2 <<EOF
nodedb: data directory $DATA_DIR is not writable by uid=$(id -u) gid=$(id -g).

This usually means a host volume was mounted with root ownership while
NodeDB is configured to run as a non-root user. Fixes:

  1. Let the entrypoint fix it: drop the explicit --user flag so the
     container starts as root and chowns the volume on first boot.
  2. Pre-create the volume with the right ownership on the host, e.g.
     chown -R 10001:10001 /path/to/host/dir
  3. Run as root explicitly: docker run --user 0:0 ...

EOF
    exit 1
fi

exec "$@"
