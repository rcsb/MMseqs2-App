# Results in redis

By default a server and a worker have to share the directory in `paths.results`:
the server writes a job's input there, the worker reads it, runs mmseqs, writes
results back, and the server reads those to answer the client. Scaling that out
means every pod mounting the same read-write-many volume, and past a few pods it
is that volume, not CPU, that limits the service — every job is a directory of
small files, and the metadata traffic of every pod lands on one filesystem.

This build shares redis instead, and nothing else.

## How a job travels

1. A server accepts a search and puts the whole request, query included, in
   `mmseqs:job:<ticket>`, then queues the ticket. **It writes no files**: it is
   not going to run this job and need not share a disk with whoever does.
2. Any worker pops the ticket, rebuilds the job directory on its own disk, and
   runs mmseqs, which needs real files.
3. When the job finishes the worker renders exactly what
   `/api/result/<ticket>/<entry>` would have served out of that directory,
   stores it gzipped in `mmseqs:result:<ticket>`, and **deletes the directory**.
4. Any server answers the fetch from redis, having never seen the files.

So a job directory is scratch space that exists only while a job runs. The
volume under `paths.results` can be an ordinary `emptyDir` sized for jobs in
flight, and nothing accumulates on it.

## Keys

| key | holds |
|---|---|
| `mmseqs:status:<id>` | the ticket status |
| `mmseqs:job:<id>` | the request, so any worker can stage the job |
| `mmseqs:result:<id>` | a hash, one field per query entry, gzipped JSON |
| `mmseqs:pending` | the queue, unchanged |

## Expiry

`results.ttl` (minutes) is the retention policy. A finished job's status, input
and results all expire together, so a ticket is never `COMPLETE` with nothing
behind it. Keep it just long enough for a client to poll and then fetch: this is
memory, and a repeat of the same query simply runs again.

`results.lease` (seconds) is how long a `RUNNING` job survives without its
worker refreshing it. The worker heartbeats while the job runs, so a worker that
is killed mid-job stops being `RUNNING` after one lease and the ticket becomes
resubmittable, instead of staying `RUNNING` for as long as redis does. This also
fixes that failure in the shared-volume setup, where such tickets were stuck for
good.

`results.queue` (minutes) is how long a queued job's ticket stays known. It is
also the clock a queue prune runs on: nothing outside redis records when a job
was submitted, so an entry still queued after its status expired is by
definition one nobody is waiting for.

## What this build does not serve

Only `/api/result/<ticket>/<entry>` is rendered into redis. The endpoints that
read other files from a job directory — `/api/result/download/<ticket>` and
`/api/result/queries/...` — work only while the directory exists, which is to
say only on the worker that ran the job and only until it finishes. The web UI
depends on those, so it is not usable against this build; that is deliberate,
and the UI is dropped from the deployment.

Msa jobs are the exception to the deletion: they produce files rather than
alignments, nothing renders them, so their directories are kept rather than
silently thrown away.

## Sizing

Memory is the constraint, since redis holds all of this in RAM. Measured over
796 real queries:

| | raw JSON |
|---|---|
| median | 191,874 B (~187 KB) |
| max | 1,740,081 B (~1.7 MB) |

The distribution is right skewed — a query against a well represented protein
hits the `--max-seqs` cap and carries two aligned sequence strings per hit — so
size the memory from the **mean**, not the median, and leave room for the tail.

Responses are stored gzipped. Alignment JSON compresses roughly five to eight
fold, so at 1000 queries/min, taking the median as a stand-in for the mean:

| ttl | compressed, at 1000 q/min | at today's 180/min peak |
|---|---|---|
| 5 min | ~160 MB | ~30 MB |
| 15 min | ~470 MB | ~85 MB |
| 60 min | ~1.9 GB | ~340 MB |

So the current 1Gi redis limit is ample at today's traffic with a 15 minute
ttl, and at 1000 q/min wants either a 5 minute ttl or a larger redis. Add
headroom over these figures: they use the median, and the mean is higher.

Set `maxmemory` below the container's memory limit, with
`maxmemory-policy volatile-lru`. A redis that reaches its container limit is
OOM killed and takes the whole path down; one that reaches `maxmemory` evicts
instead. **`volatile-lru` and not `allkeys-lru`**: only finished jobs carry a
TTL, so eviction can only ever drop results that are already expiring, never
the pending queue or the status of a job still queued or running. An evicted
result behaves exactly like an expired one — the ticket is unknown, and
resubmitting the query runs it again.

## Configuration

```json
"results" : {
    // minutes a finished job stays fetchable
    "ttl"   : 15,
    // seconds a running job survives without a heartbeat from its worker
    "lease" : 60,
    // minutes a queued job's ticket stays known
    "queue" : 5
}
```

Both have defaults, so an absent block still bounds what redis keeps.

`-local` mode is unaffected: there is one process, and the files it wrote are
all it needs.
