# TeleRM policy analysis

1200 simulated runs, 50 seeds per cell, paired against `edge_first` (95% CI).

## Overall blocking % (mean ± CI) by load

| rate/s | best_fit | edge_first | latency_slack | least_loaded |
|---|---|---|---|---|
| 1 | 10.9 ± 0.6 | 10.9 ± 0.5 | 4.5 ± 0.5 | 5.5 ± 0.5 |
| 2 | 19.9 ± 0.5 | 19.6 ± 0.5 | 14.5 ± 0.5 | 16.5 ± 0.5 |
| 4 | 34.3 ± 0.6 | 35.1 ± 0.6 | 31.4 ± 0.6 | 32.8 ± 0.5 |
| 6 | 43.7 ± 0.4 | 44.3 ± 0.4 | 42.4 ± 0.4 | 42.7 ± 0.4 |
| 8 | 50.2 ± 0.4 | 50.5 ± 0.4 | 48.9 ± 0.4 | 49.1 ± 0.4 |
| 12 | 59.0 ± 0.3 | 59.0 ± 0.3 | 57.8 ± 0.3 | 57.4 ± 0.2 |

## vRAN-DU blocking % (mean ± CI) by load

| rate/s | best_fit | edge_first | latency_slack | least_loaded |
|---|---|---|---|---|
| 1 | 49.4 ± 1.9 | 50.0 ± 1.7 | 19.4 ± 1.7 | 22.0 ± 1.8 |
| 2 | 70.2 ± 1.0 | 72.5 ± 1.0 | 46.2 ± 1.5 | 51.8 ± 1.3 |
| 4 | 85.0 ± 0.7 | 89.8 ± 0.5 | 73.3 ± 0.9 | 81.4 ± 0.8 |
| 6 | 91.4 ± 0.4 | 95.8 ± 0.3 | 85.8 ± 0.4 | 91.6 ± 0.4 |
| 8 | 94.6 ± 0.4 | 97.8 ± 0.2 | 90.7 ± 0.4 | 95.9 ± 0.3 |
| 12 | 97.7 ± 0.2 | 99.4 ± 0.1 | 95.8 ± 0.2 | 98.6 ± 0.2 |

## Paired improvement vs `edge_first` (percentage points; negative = better)

| rate/s | policy | Δ overall blocking | Δ DU blocking | significant? |
|---|---|---|---|---|
| 1 | best_fit | +0.01 ± 0.33 | -0.59 ± 1.49 | no |
| 1 | latency_slack | -6.34 ± 0.31 | -30.64 ± 1.53 | yes (better) |
| 1 | least_loaded | -5.37 ± 0.35 | -27.99 ± 1.57 | yes (better) |
| 2 | best_fit | +0.30 ± 0.30 | -2.31 ± 1.13 | no |
| 2 | latency_slack | -5.13 ± 0.43 | -26.34 ± 1.69 | yes (better) |
| 2 | least_loaded | -3.12 ± 0.33 | -20.75 ± 1.46 | yes (better) |
| 4 | best_fit | -0.77 ± 0.31 | -4.84 ± 0.78 | yes (better) |
| 4 | latency_slack | -3.63 ± 0.38 | -16.54 ± 0.84 | yes (better) |
| 4 | least_loaded | -2.27 ± 0.35 | -8.43 ± 0.65 | yes (better) |
| 6 | best_fit | -0.59 ± 0.27 | -4.35 ± 0.43 | yes (better) |
| 6 | latency_slack | -1.93 ± 0.30 | -9.97 ± 0.50 | yes (better) |
| 6 | least_loaded | -1.57 ± 0.27 | -4.17 ± 0.43 | yes (better) |
| 8 | best_fit | -0.24 ± 0.27 | -3.22 ± 0.38 | no |
| 8 | latency_slack | -1.57 ± 0.30 | -7.08 ± 0.45 | yes (better) |
| 8 | least_loaded | -1.40 ± 0.26 | -1.91 ± 0.27 | yes (better) |
| 12 | best_fit | -0.02 ± 0.25 | -1.74 ± 0.23 | no |
| 12 | latency_slack | -1.27 ± 0.29 | -3.62 ± 0.25 | yes (better) |
| 12 | least_loaded | -1.61 ± 0.21 | -0.77 ± 0.17 | yes (better) |

## Recommendation

At the design load (2 arrivals/s) the lowest mean blocking is **`latency_slack`** (14.5% vs 19.6% for `edge_first`).

## Bottleneck: why requests are blocked (all runs)

| reason | share |
|---|---|
| node_capacity | 78.4% |
| link_bandwidth | 21.6% |
