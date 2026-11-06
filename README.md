# distributed-kv

A persistent, replicated key-value store, built as a learning project to
understand how storage engines and distributed systems actually work
underneath, rather than treating them as black boxes.

## What this is

A single logical key-value store that runs across multiple machines. Each
node holds a full copy of the data. One node is the primary and accepts all
writes; the rest are backups that stay in sync by replaying what the
primary does. If the primary goes down, a backup takes over automatically.

The interesting parts of the project are the storage engine underneath each
node, and the replication protocol that keeps nodes consistent with each
other.

## Storage engine

Each node stores its data in an LSM tree, the same family of structure used
by systems like LevelDB and RocksDB. Writes go to an in-memory sorted
structure (a skip list) and a write-ahead log at the same time. Once the
in-memory structure gets large enough, it is flushed to disk as an
immutable, sorted file (an SSTable). Reads check the in-memory structure
first, then fall back to progressively older files on disk, using bloom
filters to avoid reading files that cannot contain the key. A background
process periodically merges and rewrites these files (compaction) to keep
read performance from degrading as writes accumulate.

If a node crashes, the write-ahead log is replayed on restart to rebuild
whatever had not yet been flushed to disk, so no acknowledged write is
lost.

## Replication

Nodes replicate using a primary-backup scheme, not Raft or another
consensus protocol. This was a deliberate choice, not an accident of
scope: Raft was implemented and working earlier in this project's history,
but it is genuinely difficult to defend under close questioning without a
lot of time spent internalizing it, and the goal here was to build
something that could be fully understood, not just made to pass tests.

The scheme works like this:

- The primary appends every write to its own durable log and applies it
  locally, then sends it to every backup.
- There is no election. If the primary goes quiet for too long, a single
  fixed rule decides who takes over: the backup with the lowest node ID
  among the survivors. No voting, no majority requirement.
- Every time a new primary takes over, it increments a number called the
  epoch. Backups reject messages from anyone quoting an old epoch, which
  is what stops a primary that comes back online after being presumed
  dead from silently overwriting what happened while it was gone.
- A backup that falls behind, for instance after being restarted, asks the
  primary directly for whatever entries it is missing and replays them.

This design has a known, deliberate limitation: it does not handle a true
network partition safely. If the primary is still running but cut off from
every other node, it has no way to learn that a new primary has already
been elected on the other side of the partition, and both would keep
accepting writes independently. Solving that requires a quorum or majority
rule, which is what Raft and similar protocols exist to provide. That
tradeoff was made consciously here in exchange for a system simple enough
to reason about completely.

## The concurrency bug and its fix

Conditional writes, compare-and-swap and atomic increment, are not
naturally safe under concurrency. The original implementation read the
current value, decided what to write, and only then sent the write to be
replicated, all as three separate steps. Two clients issuing the same
operation at nearly the same time could both read the same starting value
and both compute their result from it, so one of the two updates would be
silently lost.

The fix is a single mutex on the primary that wraps the entire
read-decide-replicate sequence for every conditional write, so only one
such operation is ever in flight at a time. This is intentionally simpler
than what an equivalent fix under Raft would require, because Raft can
have leadership change mid-operation across terms, while this design only
ever has one primary deciding writes at any given moment.

The tradeoff is visible in the benchmarks: read throughput scales close to
linearly with concurrent clients, while write throughput plateaus almost
immediately, because every write serializes behind that one mutex. That is
not an oversight; it is the visible cost of the correctness fix, and the
benchmarking suite exists specifically to make tradeoffs like this
measurable instead of assumed.

## Repository layout

```
src/
  common/        status codes, binary encoding, CRC32C, logging, file utilities
  storage/       the LSM tree: memtable, WAL, SSTables, compaction
  concurrency/   thread pool, optimistic transactions for the standalone path
  net/           the wire protocol and a hand-rolled TCP transport
  replication/   the primary-backup engine described above
  server/        wires storage, replication, and networking into one process
  client/        client library and CLI
tests/           unit and integration tests, including real multi-process
                 crash and failover scenarios
bench/           a load generator and storage microbenchmarks
scripts/         manual end-to-end scripts for replication and crash recovery
```

## What is not implemented

- The replication log has no compaction or snapshotting yet, so it grows
  without bound. This is the most important gap to close before this
  could run for any real length of time.
- Partition tolerance, as described above.
- A gRPC transport was scaffolded early in the project's history but never
  built out; the actual transport is the hand-rolled one in `src/net/`.
