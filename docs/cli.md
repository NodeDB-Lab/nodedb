# CLI

The server binary is `nodedb`. There is no separate client binary: query clients speak pgwire,
HTTP, the native protocol, or RESP — see [Protocols](protocols.md).

```
nodedb — NodeDB server + operator tooling

USAGE:
    nodedb [CONFIG_FILE]                     Run the server (default mode)
    nodedb --version                         Print version and exit
    nodedb regen-certs --data-dir D --node-id N
                                             Reissue this node's cert under the existing CA
    nodedb rotate-ca --stage --data-dir D    Generate a new CA, write to ca.d/, emit staged bundle
    nodedb rotate-ca --finalize --remove FP  Ask the running node to remove CA with fingerprint FP
    nodedb join-token --create --data-dir D --for-node N [--ttl 10m]
                                             Emit a one-time HMAC token for a joining node
    nodedb healthcheck [--port N]            Probe local HTTP /health (exit 0=healthy, 1=unhealthy)
    nodedb help                              Print this message
```

Running with no argument reads the default config path; `nodedb /etc/nodedb.toml` reads that
file instead. Every flag above is parsed by hand — the binary deliberately does not pull in a
CLI framework.

## Reserved verbs

These are parsed but not implemented; each exits with the usage message:

```
nodedb migrate    Schema/data migration
nodedb backup     Online backup
nodedb restore    Restore from backup
nodedb verify     Consistency check
nodedb repair     Repair corrupted data
nodedb dump       Logical export
nodedb fsck       Filesystem consistency check
```

Online backup and restore are driven from SQL instead today (`BACKUP TENANT`, `RESTORE TENANT`).

## Health probe

`nodedb healthcheck` exits `0` when the local HTTP endpoint answers healthy and `1` otherwise,
which is the intended Kubernetes liveness/readiness probe. The endpoint itself is the HTTP
`/healthz` family described in [Architecture](architecture.md).
