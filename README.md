# TeleRM — Milestone 1: Distributed Operating System Foundation

A working implementation of the Milestone-1 design: a global resource
manager, three site nodes (edge-A, edge-B, core-1), and a traffic
generator, talking over length-prefixed JSON on TCP, with global
admission control, pluggable placement policies, and local stride
scheduling. Pure Python standard library — nothing to install.

## Requirements

- Python 3.9+ (developed/tested on 3.12), standard library only.

## Layout

```
telerm/
  config.json          topology, service catalogue, workload, policy
  manager.py           Global Resource Manager (its own OS process)
  site.py              a site node: stride scheduler + worker pool (its own OS process)
  generator.py         traffic generator (Poisson arrivals, resizes)
  run_demo.py           orchestrator: spawns manager+sites+generator, collects the report
  common/
    protocol.py         length-prefixed JSON over TCP, request/reply helper
    topology.py          graph, shortest-latency path, link bandwidth reservations
    util.py              dominant-share math, Jain's index, sid formatting
    counters.py          per-node message/byte counters
  tests/
    test_core.py          unit tests for the pure-logic pieces (no sockets)
  results/                one subfolder per run, created automatically
```

## Quick start

```bash
cd telerm
python3 run_demo.py --policy edge_first
```

That single command spawns the manager and all three sites as real,
independent OS processes (each on its own TCP port — see
`config.json`), runs the generator for the configured workload, waits
for every admitted instance to finish, shuts everything down cleanly,
and prints a summary:

```
=== TeleRM run summary ===
policy: edge_first
overall blocking probability: 22.0%  (offered=50, blocked=11)
  IMS-CSCF     blocking=0.0%  offered=11
  IoT-GW       blocking=0.0%  offered=9
  Transcoder   blocking=0.0%  offered=4
  UPF          blocking=12.5%  offered=16
  vRAN-DU      blocking=90.0%  offered=10
link utilisation (final):
  edge-A<->core-1      0.0%  (0.0/1000 Mb/s)
  ...
full results in: results/edge_first-seed42-.../
```

Compare policies on the same workload:

```bash
python3 run_demo.py --policy edge_first    --seed 42
python3 run_demo.py --policy least_loaded  --seed 42
python3 run_demo.py --policy best_fit      --seed 42
```

Useful flags: `--duration <s>`, `--rate <arrivals/s>`, `--seed <n>`,
`--results-dir <path>`.

### Running nodes by hand (multi-host / manual mode)

Each node is a standalone script and doesn't need `run_demo.py`. To
run on one host:

```bash
python3 manager.py --config config.json --results-dir results/manual
python3 site.py --site-id edge-A --config config.json
python3 site.py --site-id edge-B --config config.json
python3 site.py --site-id core-1 --config config.json
python3 generator.py --config config.json --results-dir results/manual
```

To run across several machines: edit each site's `host` in
`config.json` to that machine's real address, make sure the listed
ports are reachable, and point every `site.py` / `generator.py`
invocation at the manager's real host with `--manager-host` /
`--manager-port`. The wire protocol (`common/protocol.py`) doesn't
care whether the peer is on `localhost` or across the network.

### Tests

```bash
python3 -m unittest discover -s tests -v
```

## What's produced per run (`results/<run>/`)

| File | Contents |
|---|---|
| `config_used.json` / `config_used_raw.json` | exact config, argv, Python version, platform, seed — the reproducibility record from design-doc §11 |
| `manager_report.json` | blocking probability overall and per-service (with reasons), decision-time stats, final link utilisation, message/byte counts |
| `instances_log.json` | per-instance summary (jobs_done, busy_s, latency) as each instance terminates |
| `heartbeat_log.json` | every HEARTBEAT received: slot utilisation, jobs in interval, Jain's fairness index (only while saturated) |
| `utilisation_timeseries.json` | per-second CPU/mem/bw utilisation per site and per link |
| `generator_log.json` / `generator_summary.json` | every REQUEST/RESIZE the generator sent and the reply it got |
| `manager.log`, `site-*.log` | raw stdout/stderr of each subprocess |

## How the design doc's mechanisms map to code

- **Admission control (§5.1)** — `Manager._on_request` in `manager.py`
  checks, per active site: reachability, `path_latency ≤ max_latency`,
  node-vector capacity (`util.fits`), and link bandwidth along the
  shortest-latency path (`Topology.path_has_bandwidth`). All three
  must pass. The DU's 2 ms bound is *not* special-cased anywhere — it
  falls straight out of the latency check, since every link is ≥3 ms
  (`tests/test_core.py::test_du_never_reaches_core_within_2ms`
  demonstrates this).
- **Placement policies (§5.2)** — `Manager._apply_policy`.
  `edge_first` sorts by `(path_latency, dominant_share_after)`; since
  only the ingress site can ever have zero latency, "ingress first"
  falls directly out of "lowest latency first". `least_loaded` and
  `best_fit` sort by dominant share / dominant free share (see below).
- **Reserve-then-ALLOCATE, rollback on failure (§5.1)** — the manager
  reserves the node vector and path bandwidth *before* calling the
  site's `ALLOCATE`, and rolls the reservation back if the site
  doesn't answer `ALLOCATE_OK`.
- **Stride scheduling (§4.4)** — `Site._pick_next_locked` /
  `_worker_loop` in `site.py`: `stride = 10^6 / tickets`, dispatch the
  eligible instance with the smallest `pass_value`, then
  `pass += stride`. A new instance starts at the current minimum pass.
  An instance's in-flight job count is capped at `⌈cpu/1000⌉`.
- **Fairness sampling (§4.4)** — `Site._heartbeat_loop` computes Jain's
  index over `jobs_in_interval / tickets` only when slot utilisation
  exceeds `fairness_saturation_threshold` (default 0.9), matching the
  "only valid under contention" fix recorded in the doc's Week-1 log.
- **Failure detection (§3.2, §8)** — `Manager._failure_detector_loop`
  marks a site `SUSPECT` after `failure_timeout_heartbeats` missed
  heartbeat intervals and excludes it from placement; it is not
  demoted further and its instances are not recovered (matches the
  documented limitation). Manually verified by killing a site process
  mid-run and observing the status flip.
- **Serialised admission (§8, known limitation)** — `Manager._on_request`
  holds `self.lock` for the *entire* admission round trip, including
  the network call to the chosen site's `ALLOCATE`, exactly as
  described ("the manager holds a lock during ALLOCATE, so admission
  throughput is bounded by one site round trip at a time").
- **Global instance IDs (§2)** — `util.format_sid`, a monotonically
  increasing counter under the manager's lock: `S00001`, `S00002`, ...
- **Communication (§6)** — `common/protocol.py`: 4-byte big-endian
  length prefix + JSON, one request/one reply per exchange.

## Implementation choices worth knowing about

A few places where S1's prose leaves room for a concrete decision;
these are the ones this implementation made, and why.

- **Workers are threads, not OS processes.** The design doc says "the
  manager, each site, and each site's worker pool are OS processes."
  Here, the *manager* and *each site* are genuine, separate OS
  processes (own PID, own TCP port) — that's what the architecture
  diagram and node table are really about. Within a site, the worker
  pool is a fixed set of Python threads sharing the stride-scheduler
  state, rather than one OS process per worker. This keeps the
  scheduler's shared state (pass values, tickets) simple and
  lock-based instead of needing IPC, while still respecting the fixed
  worker-pool-size design ("Bounded overhead; allocation enforced by
  the scheduler, not by process count" — §7). If you need literal
  worker OS processes for the run report, swapping
  `threading.Thread` for `multiprocessing.Process` in
  `Site._worker_loop` and passing shared state through a
  `multiprocessing.Manager` is the natural extension point.
- **"Dominant share" is defined explicitly** (the doc names the
  concept for `least_loaded`/`best_fit` but doesn't give a formula):
  `dominant_share_after` = the *maximum*, across cpu/mem/bw, of
  allocated/capacity after hypothetically placing the request (the
  classic DRF bottleneck-resource definition). `dominant_free_share_after`
  = the *minimum*, across cpu/mem/bw, of (capacity−allocated)/capacity
  after placement — i.e. how much headroom is left on the scarcest
  resource. `least_loaded` picks the site minimising the former;
  `best_fit` picks the site minimising the latter (tightest fit,
  leaves bigger holes elsewhere). See `common/util.py`.
- **A "job"** has no real payload in S1 (there's no actual traffic to
  process), so each dispatched job simulates a short unit of work:
  duration drawn from an exponential distribution around
  `job_service_ms` (config, default 6 ms), capped at 5× the mean to
  avoid a long tail. This is what makes slot utilisation and
  jobs/s-per-core meaningful numbers instead of always being 0 or 1.
- **Ingress sites are the edges.** The generator picks each request's
  ingress uniformly from `workload.ingress_sites` (edge-A, edge-B by
  default) — matching the telecom scenario where traffic enters via
  the RAN at the edge, not at the core.
- **Blocking reason when several sites fail for different reasons:**
  the manager reports whichever reason was most common across the
  sites it checked, tie-broken `latency > node_capacity >
  link_bandwidth > unreachable` (`Manager._pick_reason`).
- **Resize target selection**: the generator keeps a rolling window of
  the last 50 accepted `sid`s and, with probability `resize_prob`,
  fires a `RESIZE` against a uniformly random one of them after a
  random delay — it does not try to track exactly which instances are
  still alive at that moment (nor does the design doc specify how to
  pick "a random accepted instance" precisely). A `RESIZE` against an
  already-terminated instance simply comes back `RESIZE_FAIL
  unknown_or_terminated`, which is logged, not treated as an error.

## Sample comparison (this implementation, seed 42, default workload)

```
python3 run_demo.py --policy edge_first   --seed 42
python3 run_demo.py --policy least_loaded --seed 42
```

| Metric | edge_first | least_loaded |
|---|---|---|
| Overall blocking probability | 22.0% | 12.7% |
| vRAN-DU blocking probability | 90.0% | 33.3% |

The exact numbers won't match design-doc §9's own reference run
byte-for-byte (a different implementation consumes its seeded RNG in
a different order even with the same seed), but the *direction* is
the same and for the same reason described there: `edge_first` lets
latency-tolerant services (IMS, IoT) fill the scarce edge sites first,
crowding out the DU, which — per `test_du_never_reaches_core_within_2ms`
— has nowhere else it can legally run. `least_loaded` spreads the
tolerant services out, leaving the DU more edge headroom. Re-run with
`--seed` a few different values before treating any single run's
numbers as more than illustrative — the doc makes the same caveat.

## Known limitations (inherited by design, §8 of the doc)

- Single point of failure: admission stops if the manager process dies.
- Serialised admission: one site round trip at a time (see above).
- Failure detection only — a `SUSPECT` site's instances are not
  recovered or migrated; their resources stay reserved until they
  naturally expire and the (unreachable) site would have reported
  `INSTANCE_EXIT` — in practice, once a site is gone those reservations
  are stuck until the manager restarts. This is a faithful
  reproduction of the documented limitation, not a bug to fix here.
- No migration — a `RESIZE` that doesn't fit the current site simply fails.
- Everything on one machine shares physical cores, so stride-scheduler
  enforcement is relative (share of *simulated* jobs), not an absolute
  CPU guarantee — exactly as §8 notes.

There are four major types of processes:

Manager — makes global resource/admission decisions.
Site nodes — represent distributed telecom computing/network locations.
Generator — creates telecom service requests.
Workers inside each site — execute simulated jobs using a stride scheduler.
run_demo] manager is up
[run_demo] all sites are up and registered
[run_demo] running generator ...
[generator] starting: rate=2.0/s duration=30s seed=42

Those messages indicate that the distributed components successfully started.
config.json defines the network

The configuration file contains the topology, sites, resources, services and workload parameters.

Conceptually, it defines something like:

Sites

edge-A
    CPU
    Memory
    Bandwidth

edge-B
    CPU
    Memory
    Bandwidth

core-1
    CPU
    Memory
    Bandwidth

and the network connections:

edge-A ───── core-1
   │
   │
   └──────── edge-B

with properties such as:

link latency
link bandwidth
site capacity
service requirements

The manager uses this information when deciding where a request can run.

4. The traffic generator creates telecom requests

generator.py represents incoming telecom traffic.

The README says the generator uses Poisson arrivals.

For example:

rate = 2 requests/second
duration = 30 seconds

means that requests are generated according to a Poisson arrival process with an average rate of two arrivals per second.

The generator also randomly chooses the ingress site.

By default:

edge-A
edge-B
manager.py performs global admission control and placement.

This is one of the most important parts of the TeleRM architecture.

When the manager receives a request, it doesn't immediately send it to a site.

It first determines:

Can this service legally and physically be placed at one of the available sites?

The README describes three major checks.

Check 1 — Reachability

The manager checks whether the candidate site can be reached from the request's ingress point.
Request enters at edge-A

Candidate:
edge-A → reachable

core-1 → reachable

edge-B → possibly reachable depending on topology

If there is no valid network path, that candidate cannot be used.


Check 2 — Latency

The manager calculates the shortest-latency path between the ingress site and the candidate site.

For example:

edge-A → core-1

latency = 5 ms

If the service requires:

maximum latency = 10 ms

then:

5 ms ≤ 10 ms

so the latency requirement passes.

But consider the vRAN-DU.

The README explains that the DU has a 2 ms latency requirement.

If every network link has at least 3 ms latency, then:

edge-A → core-1
       ≥ 3 ms

Therefore:

3 ms > 2 ms

and the DU cannot be placed in the core.

This is important because the implementation doesn't have a special rule saying "DU cannot go to core."

Instead, the topology and latency requirements naturally produce that result.
Check 3 — Node capacity

The manager also checks whether the candidate site has enough resources.

The resources include:

CPU
Memory
Bandwidth

Suppose a site has:

CPU      = 100 units
Memory   = 100 units
Bandwidth = 1000 Mb/s

and a request requires:

CPU      = 20
Memory   = 10
Bandwidth = 100 Mb/s

The manager checks whether:

allocated + requested ≤ capacity

for each resource.

If any resource would exceed its capacity, the site fails the node-capacity check.

9. Check 4 — Link bandwidth

Even if the destination site has enough CPU and memory, the network path itself must have sufficient bandwidth.

For example:

edge-A
   │
   │ 100 Mb/s available
   ▼
core-1

If the new service needs:

150 Mb/s

then the service cannot use that path.

The manager therefore checks:

available link bandwidth ≥ requested bandwidth

for every link along the selected path.

10. Admission decision

The manager therefore effectively performs:

                    Request
                       │
                       ▼
              ┌────────────────┐
              │ Find candidates │
              └───────┬────────┘
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
     Reachability   Latency    Capacity
          │           │           │
          └───────────┼───────────┘
                      ▼
                Link bandwidth
                      │
                      ▼
               Valid candidates
                      │
                      ▼
                 Apply policy
                      │
                      ▼
                 Select site

If there are no valid candidates:

REQUEST
   │
   ▼
BLOCK

The manager records the reason.

Possible reasons include:

latency
node_capacity
link_bandwidth
unreachable
11. Placement policies

After identifying valid sites, the manager chooses one according to the selected policy.

Your command:

python run_demo.py --policy edge_first

selects:

edge_first

There are three policies described in the README.

edge_first

This prioritizes the site with the lowest path latency.

Therefore, if the request originates at edge-A:

edge-A → latency 0 ms
core-1 → latency > 0 ms

the edge site normally gets preference.

This is useful for latency-sensitive telecom services.

least_loaded

This considers the site's dominant resource share.

The implementation calculates the share of:

CPU
Memory
Bandwidth

and considers the largest/most constrained share.

Conceptually:

CPU usage      = 30%
Memory usage   = 50%
Bandwidth      = 20%

dominant share = 50%

A site with a lower dominant share is considered less loaded.

best_fit

This looks at the remaining resource headroom.

Conceptually:

Site A:
CPU free       = 20%
Memory free    = 30%
Bandwidth free = 50%

dominant remaining constraint = 20%

The policy tries to tightly fit the request, leaving larger resource holes elsewhere.

12. Reserve-then-ALLOCATE

An important design feature is that the manager doesn't simply tell the site:

"Run this service."

It first reserves the resources globally.

The sequence is:

Request
   │
   ▼
Manager checks resources
   │
   ▼
Manager reserves CPU/memory
   │
   ▼
Manager reserves network bandwidth
   │
   ▼
Manager sends ALLOCATE to site
   │
   ├───────────────┐
   │               │
ALLOCATE_OK    ALLOCATE_FAIL
   │               │
   ▼               ▼
Keep reservation  Rollback

This prevents the manager from believing resources are available when the site could not actually allocate them.

13. What happens inside a Site?

Each site runs:

site.py

For example:

site.py --site-id edge-A --config config.json

A site has:

resource information
active service instances
a worker pool
stride scheduling
heartbeat generation
utilisation measurement

The site receives commands from the manager.

For example:

ALLOCATE
RESIZE
SHUTDOWN
14. Worker pool

The README makes an important implementation distinction.

The design originally describes workers as OS processes, but this implementation uses:

Python threads

inside each site.

So:

edge-A process
      │
      ├── worker thread
      ├── worker thread
      ├── worker thread
      └── worker thread

The site itself is an actual OS process.

The workers are threads sharing the site's scheduler state.

15. Stride scheduling

This is the site's local scheduling mechanism.

Suppose there are three service instances:

Instance A → 100 tickets
Instance B → 50 tickets
Instance C → 25 tickets

The scheduler calculates:

stride = 1,000,000 / tickets

Therefore:

A → 10,000
B → 20,000
C → 40,000

The scheduler maintains a pass_value for each instance.

It chooses the eligible instance with the smallest pass value.

After running a job:

pass_value += stride

Because instances with more tickets have smaller strides, they receive service more frequently.

Conceptually:

             Scheduler
                 │
       ┌─────────┼─────────┐
       ▼         ▼         ▼
    Instance A Instance B Instance C
     tickets     tickets    tickets
       │           │          │
       └───────────┼──────────┘
                   ▼
             smallest pass
                   │
                   ▼
             Execute job

This provides proportional sharing among competing instances.

16. Jobs are simulated

There is no real telecom packet-processing workload in Milestone 1.

Instead, the system simulates jobs.

The README says job duration is randomly generated around the configured:

job_service_ms

For example:

job_service_ms = 6 ms

A job might therefore take:

4 ms
7 ms
9 ms
3 ms
...

The duration is exponentially distributed and capped at five times the mean.

This makes the scheduler produce meaningful:

utilisation
jobs/second
latency
fairness

measurements.

17. CPU slot limitation

The number of jobs an instance can have in flight is limited according to its CPU allocation.

The README gives:

ceil(cpu / 1000)

as the cap.

So if an instance has a CPU allocation corresponding to, for example, 2,000 units:

ceil(2000 / 1000) = 2

it can have up to two in-flight jobs.

This prevents one instance from creating unlimited simultaneous work.

18. Heartbeats

The sites periodically send HEARTBEAT messages to the manager.

The heartbeat tells the manager information about the site's state.

Conceptually:

edge-A
   │
   │ HEARTBEAT
   ▼
Manager

The heartbeat contains information such as:

CPU utilisation
memory utilisation
slot utilisation
jobs completed
fairness information

This allows the manager to monitor whether sites are still alive.

19. Failure detection

Suppose:

edge-B

crashes.

The manager stops receiving its heartbeats.

After the configured number of missed heartbeat intervals:

edge-B
    │
    │ no heartbeat
    ▼
Manager
    │
    ▼
SUSPECT

The manager then excludes that site from future placement decisions.

Importantly, according to the README, TeleRM does not migrate the existing instances from that failed site.

So this is currently:

failure detection

rather than:

failure recovery
20. RESIZE operations

The generator can also send:

RESIZE

requests.

The README says the generator maintains a rolling list of up to 50 recently accepted instance IDs.

It can randomly select one and request a resize.

For example:

S00021
   │
   │ RESIZE
   ▼
Manager
   │
   ▼
Current site

If the new resource requirements fit, the resize succeeds.

If they don't fit:

RESIZE_FAIL

There is no migration to another site.

21. Global instance IDs

Every accepted service instance gets a global ID.

For example:

S00001
S00002
S00003
S00004
...

These IDs are generated by the manager under its lock.

This makes it possible to track an instance throughout the distributed system.

22. Communication protocol

The processes communicate over TCP.

The messages are not sent as arbitrary Python objects.

They use:

4-byte length prefix + JSON

Conceptually:

┌──────────────┬─────────────────────────────┐
│ 4-byte size  │ JSON message                │
└──────────────┴─────────────────────────────┘

For example:

{
  "type": "REQUEST",
  "service": "IoT-GW",
  "ingress": "edge-A"
}

The receiver first reads the 4-byte length and then reads exactly that many bytes for the JSON message.

This makes message boundaries explicit even though TCP itself is a byte stream.

23. Why the topology matters

The topology isn't just for drawing a network diagram.

It directly affects admission control.

Suppose:

edge-A ─── 3 ms ─── core-1

A service with:

max_latency = 2 ms

cannot use core-1.

But a service with:

max_latency = 20 ms

can potentially use it.

Therefore:

Service requirements
          +
Network topology
          +
Available resources
          ↓
    Placement decision

This is one of the main ideas being demonstrated by TeleRM.

24. Why different policies produce different blocking

This is particularly important for your Milestone 1.

Imagine the edge has limited resources:

edge-A
edge-B

and the core has more capacity but higher latency.

Now consider two types of services:

Latency-sensitive service
    max latency = 2 ms

Latency-tolerant service
    max latency = 20 ms

The latency-sensitive service might only be able to use:

edge-A
edge-B

while the tolerant service can potentially use:

edge-A
edge-B
core-1

With edge_first, the tolerant service tends to consume edge capacity first.

That can eventually produce:

edge resources
      ↓
become occupied
      ↓
DU arrives
      ↓
DU cannot use core because of latency
      ↓
DU blocked

With least_loaded, tolerant services can be distributed more broadly when the core is eligible.

That can leave more edge capacity available for services that have stricter latency requirements.

This is why the README's example shows different blocking behaviour between policies.

25. The complete request lifecycle

A single request therefore travels through approximately this process:

                 TRAFFIC GENERATOR
                        │
                        │ REQUEST
                        ▼
                ┌───────────────┐
                │ Global Manager│
                └───────┬───────┘
                        │
                        ▼
              Check candidate sites
                        │
             ┌──────────┼───────────┐
             ▼          ▼           ▼
         Reachability Latency    Resources
             │          │           │
             └──────────┼───────────┘
                        ▼
                  Check bandwidth
                        │
                        ▼
                 Apply policy
                        │
                        ▼
                  Select site
                        │
                        ▼
              Reserve resources
                        │
                        ▼
                 ALLOCATE
                        │
                        ▼
                  SITE PROCESS
                        │
                        ▼
                Stride scheduler
                        │
                        ▼
                   Worker
                        │
                        ▼
                 Execute jobs
                        │
                        ▼
                  Instance exit
                        │
                        ▼
                Report to manager
                        │
                        ▼
              Release resources
26. What happens after the workload finishes?

run_demo.py waits for the system to finish processing the admitted instances.

It then shuts down the processes and generates the run results.

For your run:

results/
└── edge_first-seed42-20260925-080513/

The exact folder name is automatically generated.

27. Understanding the result files
manager_report.json

This is one of the most important files.

It contains information such as:

overall blocking probability
per-service blocking
blocking reasons
decision-time statistics
final link utilisation
message/byte counts

For your project report, this is particularly useful for evaluating the global resource manager.

generator_summary.json

Summarizes what the traffic generator offered and what responses it received.

For example:

offered requests
accepted requests
blocked requests
resize requests
generator_log.json

Contains the individual requests and responses.

This allows you to trace what happened to particular requests.

instances_log.json

Tracks accepted instances.

It includes measurements such as:

jobs_done
busy_s
latency

This helps evaluate the execution/scheduling side.

heartbeat_log.json

Contains the heartbeat information received from sites.

It can be used to study:

site utilisation
jobs processed
Jain's fairness index
utilisation_timeseries.json

Provides resource utilisation over time.

For example:

time
edge-A CPU
edge-A memory
edge-A bandwidth
edge-B CPU
...

This is useful for plotting resource utilisation graphs.

site-*.log

These are the raw logs from individual site processes.

For example:

site-edge-A.log
site-edge-B.log
site-core-1.log

They help troubleshoot what happened inside each site.

28. How the entire system maps to your telecom engineering concept

From a Telecommunication and Information Engineering perspective, you can think of TeleRM as combining several network concepts:

TeleRM component	Telecom/distributed-system concept
Generator	Network/service traffic
Manager	Centralized resource orchestration
Sites	Edge/core computing nodes
Topology	Telecom network
Link latency	Network propagation/transmission delay
Link bandwidth	Network capacity
CPU/memory	Computing resources
Admission control	Service admission
Placement policy	Network/service orchestration
Heartbeats	Network/node monitoring
Failure detector	Fault management
Stride scheduler	Resource sharing
Jain's index	Fairness measurement
RESIZE	Dynamic resource allocation
JSON/TCP	Inter-process/network communication

So the project is essentially demonstrating:

How a distributed telecom infrastructure can dynamically decide where incoming network services should be placed while considering latency, bandwidth, computing resources, fairness, and node failures.

29. The three levels of decision-making

A particularly useful way to explain the architecture in your presentation is to divide it into three levels.
