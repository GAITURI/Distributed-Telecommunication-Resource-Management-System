#!/usr/bin/env python3
"""
Flatten a LIVE run's JSON output (results/<run>/) into CSV files that
load straight into Power BI / Tableau alongside the simulated dataset.

    python3 export_results.py results/latency_slack-seed42-20260928-044556
    python3 export_results.py --all          # every run under results/
"""
import argparse
import csv
import glob
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def export(run_dir):
    out = os.path.join(run_dir, "csv")
    os.makedirs(out, exist_ok=True)
    cfg = json.load(open(os.path.join(run_dir, "config_used.json")))["config"]
    run_name = os.path.basename(run_dir.rstrip("/"))
    policy, seed = cfg["placement_policy"], cfg["seed"]

    def write(name, header, rows):
        with open(os.path.join(out, name), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
        return len(rows)

    n = {}
    p = os.path.join(run_dir, "generator_log.json")
    if os.path.exists(p):
        rows = []
        for r in json.load(open(p)):
            if r["req"]["type"] != "REQUEST":
                continue
            rep = r["reply"]
            rows.append([run_name, policy, seed, r["ts"], r["req"]["service"], r["req"]["ingress"],
                         rep.get("type"), rep.get("site", ""), rep.get("path_latency_ms", ""),
                         rep.get("reason", ""), rep.get("decision_ms", ""), r.get("rtt_ms", ""),
                         r["req"]["holding_s"]])
        n["live_requests"] = write("live_requests.csv",
            ["run", "policy", "seed", "ts", "service", "ingress", "outcome", "site", "path_latency_ms",
             "block_reason", "decision_ms", "rtt_ms", "holding_s"], rows)

    p = os.path.join(run_dir, "instances_log.json")
    if os.path.exists(p):
        rows = []
        for i in json.load(open(p)):
            jobs, busy = i.get("jobs_done", 0), i.get("busy_s", 0)
            rows.append([run_name, i["sid"], i["service"], i["site"], jobs, busy,
                         round(i["latency_sum_ms"] / jobs, 3) if jobs else "",
                         round(jobs / i["holding_s"], 2) if i.get("holding_s") else ""])
        n["live_instances"] = write("live_instances.csv",
            ["run", "sid", "service", "site", "jobs_done", "busy_s", "mean_job_latency_ms", "jobs_per_s"], rows)

    p = os.path.join(run_dir, "heartbeat_log.json")
    if os.path.exists(p):
        rows = [[run_name, h["ts"], h["site_id"], h["slot_utilisation"], h["jobs_interval"],
                 "" if h["fairness_j"] is None else h["fairness_j"], h["instances_running"]]
                for h in json.load(open(p))]
        n["live_heartbeats"] = write("live_heartbeats.csv",
            ["run", "ts", "site", "slot_utilisation", "jobs_interval", "fairness_j", "instances_running"], rows)

    p = os.path.join(run_dir, "utilisation_timeseries.json")
    if os.path.exists(p):
        rows = []
        for s in json.load(open(p)):
            for site, u in s["sites"].items():
                rows.append([run_name, "site", site, s["ts"], u["cpu_util"], u["mem_util"], u["bw_util"]])
            for link, u in s["links"].items():
                rows.append([run_name, "link", link, s["ts"], "", "", u["utilisation"]])
        n["live_utilisation"] = write("live_utilisation.csv",
            ["run", "kind", "name", "ts", "cpu_util", "mem_util", "bw_util"], rows)
    return out, n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", nargs="?")
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    dirs = sorted(glob.glob(os.path.join(HERE, "results", "*", ""))) if a.all else [a.run_dir]
    for d in dirs:
        if d and os.path.exists(os.path.join(d, "config_used.json")):
            out, n = export(d)
            print(f"{out}: {n}")
