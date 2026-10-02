#!/usr/bin/env python3
"""
Analyse the simulator's dim_run.csv: compare placement policies across
seeds and load levels with 95% confidence intervals, using PAIRED
differences against edge_first (every policy saw identical traffic per
seed/rate, so pairing removes workload noise). Writes analysis_report.md
and analysis_report.json, and can apply the winning policy to config.json.

    python3 analyze.py
    python3 analyze.py --data bigdata --design-rate 2 --apply
"""
import argparse
import collections
import csv
import json
import math
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))


def mean_ci(xs):
    n = len(xs)
    if n == 0:
        return (float("nan"), float("nan"))
    m = statistics.fmean(xs)
    if n < 2:
        return (m, float("nan"))
    return (m, 1.96 * statistics.stdev(xs) / math.sqrt(n))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(HERE, "bigdata"))
    ap.add_argument("--baseline", default="edge_first")
    ap.add_argument("--design-rate", type=float, default=2.0,
                    help="arrival rate the recommendation is judged at")
    ap.add_argument("--apply", action="store_true", help="write the winner into config.json")
    args = ap.parse_args()

    rows = list(csv.DictReader(open(os.path.join(args.data, "dim_run.csv"))))
    for r in rows:
        r["rate"] = float(r["arrival_rate"])
        r["blk"] = float(r["blocking_pct"])
        r["du"] = float(r["du_blocking_pct"])

    by = collections.defaultdict(dict)          # (rate, seed) -> policy -> row
    for r in rows:
        by[(r["rate"], r["seed"])][r["policy"]] = r

    policies = sorted({r["policy"] for r in rows})
    rates = sorted({r["rate"] for r in rows})
    out = {"policies": policies, "rates": rates, "by_rate": {}}
    md = ["# TeleRM policy analysis", "",
          f"{len(rows)} simulated runs, {len({r['seed'] for r in rows})} seeds per cell, "
          f"paired against `{args.baseline}` (95% CI).", ""]

    md += ["## Overall blocking % (mean ± CI) by load", "",
           "| rate/s | " + " | ".join(policies) + " |", "|---|" + "---|" * len(policies)]
    for rate in rates:
        cells = []
        for p in policies:
            m, ci = mean_ci([r["blk"] for r in rows if r["rate"] == rate and r["policy"] == p])
            cells.append(f"{m:.1f} ± {ci:.1f}")
        md.append(f"| {rate:g} | " + " | ".join(cells) + " |")

    md += ["", "## vRAN-DU blocking % (mean ± CI) by load", "",
           "| rate/s | " + " | ".join(policies) + " |", "|---|" + "---|" * len(policies)]
    for rate in rates:
        cells = []
        for p in policies:
            m, ci = mean_ci([r["du"] for r in rows if r["rate"] == rate and r["policy"] == p])
            cells.append(f"{m:.1f} ± {ci:.1f}")
        md.append(f"| {rate:g} | " + " | ".join(cells) + " |")

    md += ["", f"## Paired improvement vs `{args.baseline}` (percentage points; negative = better)", "",
           "| rate/s | policy | Δ overall blocking | Δ DU blocking | significant? |", "|---|---|---|---|---|"]
    verdicts = {}
    for rate in rates:
        for p in policies:
            if p == args.baseline:
                continue
            d_blk, d_du = [], []
            for (rt, _seed), cell in by.items():
                if rt == rate and p in cell and args.baseline in cell:
                    d_blk.append(cell[p]["blk"] - cell[args.baseline]["blk"])
                    d_du.append(cell[p]["du"] - cell[args.baseline]["du"])
            mb, cb = mean_ci(d_blk)
            md_, cd = mean_ci(d_du)
            sig = "yes (better)" if mb + cb < 0 else ("yes (WORSE)" if mb - cb > 0 else "no")
            verdicts[(rate, p)] = (mb, cb, sig)
            md.append(f"| {rate:g} | {p} | {mb:+.2f} ± {cb:.2f} | {md_:+.2f} ± {cd:.2f} | {sig} |")
            out["by_rate"].setdefault(str(rate), {})[p] = {
                "delta_blocking_pp": mb, "delta_blocking_ci": cb,
                "delta_du_pp": md_, "delta_du_ci": cd, "significant": sig}

    # recommendation at the design rate
    nearest = min(rates, key=lambda r: abs(r - args.design_rate))
    score = {p: mean_ci([r["blk"] for r in rows if r["rate"] == nearest and r["policy"] == p])[0]
             for p in policies}
    best = min(score, key=score.get)
    out["recommendation"] = {"design_rate": nearest, "best_policy": best, "mean_blocking_pct": score}
    md += ["", "## Recommendation", "",
           f"At the design load ({nearest:g} arrivals/s) the lowest mean blocking is "
           f"**`{best}`** ({score[best]:.1f}% vs {score[args.baseline]:.1f}% for `{args.baseline}`)."]

    # bottleneck: dominant block reason
    reasons = collections.Counter()
    path = os.path.join(args.data, "fact_block_reasons.csv")
    if os.path.exists(path):
        for r in csv.DictReader(open(path)):
            reasons[r["reason"]] += int(r["count"])
        tot = sum(reasons.values()) or 1
        md += ["", "## Bottleneck: why requests are blocked (all runs)", "",
               "| reason | share |", "|---|---|"]
        for k, v in reasons.most_common():
            md.append(f"| {k} | {100 * v / tot:.1f}% |")
        out["block_reasons_pct"] = {k: 100 * v / tot for k, v in reasons.items()}

    open(os.path.join(args.data, "analysis_report.md"), "w").write("\n".join(md) + "\n")
    json.dump(out, open(os.path.join(args.data, "analysis_report.json"), "w"), indent=2)
    print("\n".join(md))

    if args.apply:
        cfg_path = os.path.join(HERE, "config.json")
        cfg = json.load(open(cfg_path))
        cfg["placement_policy"] = best
        json.dump(cfg, open(cfg_path, "w"), indent=2)
        print(f"\n[analyze] config.json placement_policy -> {best}")


if __name__ == "__main__":
    main()
