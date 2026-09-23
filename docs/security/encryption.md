# NodeDB Security Reference

## Encryption at Rest

NodeDB uses a tiered encryption model. Each storage type has a different
encryption mechanism and key management story. Operators must understand
which tiers require explicit configuration and which rely on
infrastructure-level controls.

### WAL (Write-Ahead Log)

**Mechanism:** Application-level AES-256-GCM, applied per WAL record
before writing to disk.

**Configuration:**

```toml
[encryption]
key_path = "/etc/nodedb/keys/wal.key"
```

The key file must contain exactly 32 bytes (256 bits). Generate with:

```bash
head -c 32 /dev/urandom > /etc/nodedb/keys/wal.key
chmod 600 /etc/nodedb/keys/wal.key
```

When `[encryption]` is absent, WAL records are written in plaintext.
This is acceptable only for local development or when the data directory
is on an encrypted volume (LUKS, dm-crypt, FileVault, BitLocker).

**Key rotation:** Stop the node, re-encrypt WAL segments with the new key,
update `key_path`, restart. A future release will support online key
rotation via a DEK wrapping scheme.

### Columnar and Timeseries Segments

**Mechanism:** Optional application-level AES-256-GCM via a per-collection
KEK (`packed_partition.rs`, SEGT envelope).

**Configuration:** Set `WITH (encryption=true)` on the collection at
creation time. The per-collection key is derived from the WAL KEK
(requires `[encryption]` to be configured).

When encryption is not enabled on the collection or `[encryption]` is
absent, columnar and timeseries segments are written in plaintext.

### General Segment Files

**Mechanism:** Optional application-level AES-256-GCM via
`write_encrypted_segment` (`storage/segment.rs`, SEGP preamble,
per-segment epoch key).

**Scope:** Document, KV, and spatial engine segment files that are
flushed from memtable to the L1 NVMe tier.

**Configuration:** Requires `[encryption]` to be set. Encryption is
applied at flush time. Existing unencrypted segments are not retroactively
encrypted on upgrade; a full compaction cycle is required to re-encrypt.

### redb Files (Catalog, KV Engine, CRDT, Graph CSR)

**Mechanism:** **Filesystem-level encryption required.** NodeDB writes
these files through the standard filesystem interface without
application-level encryption.

**Affected files:**
- `system.redb` — system catalog (users, roles, tenants, collection metadata)
- Per-collection KV engine redb files
- CRDT state redb files
- Graph CSR adjacency index redb files

**Required action:** Encrypt the NodeDB data directory at the filesystem
level before deploying to production. Recommended approaches:

| Platform | Mechanism                        |
|----------|----------------------------------|
| Linux    | LUKS / dm-crypt via `cryptsetup` |
| macOS    | FileVault on the data volume     |
| Windows  | BitLocker on the data volume     |
| Cloud VM | Instance-store volume encryption (EBS, persistent disk) |

Application-level encryption of redb files is planned for a future release.

### HNSW mmap Graphs

**Mechanism:** **Filesystem-level encryption required.** The HNSW index
is loaded via `mmap` directly from disk. No application-level cipher is
applied.

Same filesystem-level controls as redb files apply.

### Vamana / DiskANN Segments

**Mechanism:** **Filesystem-level encryption required.** Vamana DiskANN
segments are accessed via the io_uring read path. No application-level
cipher is applied.

Same filesystem-level controls as redb files apply.

### L2 Cold Tier (S3-Compatible Object Storage)

**Mechanism:** Server-side encryption (SSE) configured via
`[cold_storage].sse_mode`.

NodeDB can request SSE from the object store on every upload. The
available modes depend on the object store provider.

```toml
[cold_storage]
bucket  = "my-nodedb-cold"
region  = "us-east-1"
# No sse_mode set: NodeDB sends no SSE header.
# Rely on bucket-default encryption policy.
```

```toml
[cold_storage]
bucket   = "my-nodedb-cold"
region   = "us-east-1"
sse_mode = "aes256"   # S3-managed keys (SSE-S3)
```

```toml
[cold_storage]
bucket     = "my-nodedb-cold"
region     = "us-east-1"
sse_mode   = "kms"    # AWS KMS (SSE-KMS)
kms_key_id = "arn:aws:kms:us-east-1:123456789012:key/mrk-..."
```

**Important:** When `sse_mode` is not set, NodeDB does **not** add an SSE
header to uploads. Objects will be encrypted only if the S3 bucket has a
default encryption policy configured by the operator. Relying on bucket
defaults without setting `sse_mode` in NodeDB means encryption cannot be
verified at the application layer. Set `sse_mode` explicitly for any
production deployment where cold-tier data confidentiality is required.

#### SSE modes

| Value    | Description                                      |
|----------|--------------------------------------------------|
| (absent) | No SSE header sent; rely on bucket default       |
| `aes256` | SSE-S3: S3-managed AES-256 keys                  |
| `kms`    | SSE-KMS: AWS KMS-managed key; `kms_key_id` sets the CMK ARN. When `kms_key_id` is absent, the bucket's default KMS key is used. |

### Backup Encryption

Warm-tier backups (snapshots) can be encrypted with a per-backup DEK
wrapped by a backup KEK. The backup KEK must differ from the WAL KEK.

```toml
[backup_encryption]
key_path = "/etc/nodedb/keys/backup.key"
```

When `[backup_encryption]` is absent, a warning is emitted at the first
backup operation. Unencrypted backups should not leave the data center
or be stored in shared object storage without additional encryption.

## Encryption in Transit

See [protocols.md — TLS](../protocols.md#tls) for wire encryption
configuration. All five listeners support TLS. Plaintext is the default
and is appropriate only for local development.

## Key Management Summary

| Storage type              | Mechanism          | Key location             | Missing config result |
|---------------------------|--------------------|--------------------------|----------------------|
| WAL                       | AES-256-GCM (app)  | `[encryption].key_path`  | Plaintext WAL        |
| Columnar/timeseries segs  | AES-256-GCM (app)  | Derived from WAL KEK     | Plaintext segments   |
| Document/KV/spatial segs  | AES-256-GCM (app)  | Derived from WAL KEK     | Plaintext segments   |
| redb catalog & KV         | Filesystem         | OS volume encryption     | Plaintext on disk    |
| HNSW mmap graphs          | Filesystem         | OS volume encryption     | Plaintext on disk    |
| Vamana DiskANN segments   | Filesystem         | OS volume encryption     | Plaintext on disk    |
| L2 cold tier (S3)         | SSE (object store) | `[cold_storage].sse_mode`| No SSE header sent   |
| Backups                   | AES-256-GCM (app)  | `[backup_encryption].key_path` | Plaintext backup |
