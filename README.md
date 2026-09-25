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
