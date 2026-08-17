# Per-instance job directories

By default a server and a worker have to share the directory in `paths.results`,
because the server writes the input of a job there and reads its results back
out, while the worker does the opposite. Scaling that out means every pod
mounting the same read-write-many volume, and past a few pods it is that shared
volume, not CPU, that limits the service: every job is a directory of small
files, and the metadata traffic of all pods lands on one file system.

Setting `instances.address` removes the sharing. Each server and worker pair
keeps its jobs on its own disk, which can be an ordinary ephemeral volume, and
redis takes over the two jobs the shared volume used to do.

## What an instance is

An instance is one results directory plus the processes using it. Its identity
is a random id in `<results>/instance.id`, created on first use, so:

- the server and the worker of one pod agree on it without being told, because
  they read the same directory
- it is not derived from anything Kubernetes-specific, so the same mechanism
  works under docker-compose or on a plain host
- it disappears exactly when the directory does, which is the moment the jobs
  it held stopped existing

Each instance registers `mmseqs:instance:<id>` in redis with a short TTL and
refreshes it while its server runs. The entry holds the address other instances
use to reach it: by default the local address that routes to redis, which is a
reasonable guess in any network where the instances can see each other at all.
Set `instances.advertise` when it is not.

## How a job travels

1. A server accepts a submission and writes the whole job request, query
   included, to `mmseqs:job:<ticket>`, then queues the ticket. It does not
   create a job directory: the job is not going to run here.
2. Any worker dequeues the ticket, rebuilds the job directory on its own disk
   from the request, and writes `mmseqs:owner:<ticket>` before marking the job
   RUNNING. From that point the ticket has an address.
3. Requests for the results can arrive at any server, since the service in
   front of them balances freely. A server that does not own the job forwards
   the request to the one that does, over the listener on `instances.address`.

The endpoints that read job files are the only ones forwarded, and the only
ones that listener exposes. Submission stays on the public listener, behind
whatever proxy enforces upload limits.

## When an instance disappears

An ephemeral volume dies with its pod, and so do the results on it. The
registry entry expires a few seconds later, and after that:

- a COMPLETE job whose owner is gone is dropped, and its ticket goes back to
  UNKNOWN. Submitting the query again runs it again, rather than polling
  forever for a result that cannot be fetched.
- a RUNNING job whose owner is gone goes back on the queue and runs somewhere
  else. This also covers a worker that dies mid-job, which previously left the
  ticket RUNNING for good.
- a request forwarded to an instance that stops answering is treated the same
  way, so a server whose container died while its volume lives on does not
  strand its jobs for the length of the TTL.

A worker will not take jobs while its own instance is unregistered: results it
produced then would sit on a volume nobody can reach.

## Cleaning up

Nothing outside an instance can see its results directory, so no external cron
job can prune it. Each instance deletes its own finished jobs instead, dropping
the redis keys of the ones it still owns as it goes. This is not optional
housekeeping under this layout, so a configuration that enables instances
without bounding what they keep is refused at startup.

There are two bounds and they answer different questions:

- `cleanup.maxage` is the retention policy: how long after a search its results
  can still be fetched. Results are not something a job is finished with when
  it completes, they are what the interface reads while somebody looks at them,
  one query and one page of hits at a time. So this cannot go to zero, and
  "do not cache at all" is not a setting: it is the length of a session.
- `cleanup.maxsize` is what actually bounds the volume, because an age limit
  says nothing about how much a busy hour writes. Over the limit, the oldest
  finished jobs are deleted first, however young they are.

Set both. The age limit is the policy; the size limit is what stops a traffic
spike from filling a disk that, unlike a shared volume, is small and fixed.

The redis keys of a finished job carry a TTL a little longer than the age
limit. Normally the janitor deletes keys and files together and the TTL never
matters; it is there for the keys of an instance that died before its janitor
got to them, which nothing else would ever collect.

The queue itself is still shared, so pruning abandoned queue entries is still
something to do from outside.

## Configuration

```json
"instances" : {
    // where to listen for requests forwarded by other instances
    "address"   : ":8082",
    // what to tell them to connect to, defaults to the local address
    // that reaches redis
    "advertise" : "",
    // shared secret required on forwarded requests, optional
    "token"     : ""
},
"cleanup" : {
    // minutes a finished job stays fetchable, 0 for no age limit
    "maxage"   : 60,
    // megabytes the results directory may use, oldest deleted first,
    // 0 for no limit
    "maxsize"  : 4096,
    // minutes between sweeps
    "interval" : 10
}
```

Leaving `instances.address` empty keeps the shared volume behaviour, so the
same binary serves both layouts.

## Caveats

- An instance has to run a server, not only a worker: its results are reachable
  through its own server and nothing else.
- Index jobs go on the same shared queue as searches, while the databases they
  build are per instance. That predates this feature, and deployments that
  build their databases before starting the app are unaffected, but it does
  mean an index job can be run by a worker other than the one that needs it.
