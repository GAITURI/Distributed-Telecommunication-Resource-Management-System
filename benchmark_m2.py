#!/usr/bin/env python3
"""
benchmark_m2.py — Automated Milestone 2 Benchmark Runner
Runs parametric sweeps across arrival rates and placement policies.
"""
import argparse
import json
import os
import subprocess
import sys
import time

POLICIES = ["edge_first", "least_loaded", "best_fit"]
RATES = [5, 15, 30]      # Requests per second offered load
RUN_DURATION_SEC = 10     # Duration in seconds per run
SEED = 42

print("[+] Starting TeleRM Milestone 2 Benchmark Suite...", flush=True)

def run_suite():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", default="results/m2_benchmark")
    args = parser.parse_args()

    os.makedirs(args.results_dir, exist_ok=True)
    summary_records = []

    print("=" * 76, flush=True)
    print("      TeleRM Milestone 2: Distributed Processing Benchmark Suite      ", flush=True)
    print(f"      Rates: {RATES} | Policies: {POLICIES} | Seed: {SEED}", flush=True)
    print("=" * 76 + "\n", flush=True)

    for policy in POLICIES:
        for rate in RATES:
            tag = f"{policy}_rate_{rate}"
            dest = os.path.join(args.results_dir, tag)
            print(f">>> Running: Policy = {policy:<12} | Rate = {rate:>2} req/s ... ", end="", flush=True)

            cmd = [
                sys.executable, "run_demo.py",
                "--policy", policy,
                "--rate", str(rate),
                "--duration", str(RUN_DURATION_SEC),
                "--seed", str(SEED),
                "--results-dir", dest
            ]

            t0 = time.time()
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            elapsed = time.time() - t0

            blocking_pct = 0.0
            throughput = 0.0
            p95_lat = 0.0
            jitter = 0.0

            mgr_path = os.path.join(dest, "manager_report.json")
            gen_path = os.path.join(dest, "generator_summary.json")

            if os.path.exists(mgr_path):
                try:
                    with open(mgr_path) as f:
                        m = json.load(f)
                        blocking_pct = round(m.get("blocking_probability_overall", 0.0) * 100, 2)
                except Exception:
                    pass

            if os.path.exists(gen_path):
                try:
                    with open(gen_path) as f:
                        g = json.load(f)
                        throughput = round(g.get("throughput_req_s", 0.0), 2)
                        p95_lat = round(g.get("latency_stats_ms", {}).get("p95", 0.0), 2)
                        jitter = round(g.get("jitter_ms", 0.0), 3)
                except Exception:
                    pass

            rec = {
                "policy": policy,
                "offered_rate": rate,
                "throughput_req_s": throughput,
                "blocking_pct": blocking_pct,
                "latency_p95_ms": p95_lat,
                "jitter_ms": jitter
            }
            summary_records.append(rec)
            print(f"Done in {elapsed:.1f}s | Thr: {throughput} r/s | p95: {p95_lat}ms | Block: {blocking_pct}%", flush=True)

    summary_file = os.path.join(args.results_dir, "m2_summary.json")
    with open(summary_file, "w") as f:
        json.dump(summary_records, f, indent=2)

    print("\n" + "=" * 76, flush=True)
    print(f"{'Policy':<14} | {'Rate':<5} | {'Throughput (r/s)':<17} | {'p95 Lat (ms)':<13} | {'Jitter (ms)':<11} | {'Loss %':<7}", flush=True)
    print("-" * 76, flush=True)
    for r in summary_records:
        print(f"{r['policy']:<14} | {r['offered_rate']:<5} | {r['throughput_req_s']:<17.2f} | {r['latency_p95_ms']:<13.2f} | {r['jitter_ms']:<11.3f} | {r['blocking_pct']:<7.1f}", flush=True)
    print("=" * 76, flush=True)
    print(f"\n[+] Results saved to: {summary_file}", flush=True)

if __name__ == "__main__":
    run_suite()