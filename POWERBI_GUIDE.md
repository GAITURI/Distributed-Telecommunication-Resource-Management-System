# Power BI guide for TeleRM

Power BI is the **analysis layer** of a loop, not part of the running system:

```
simulate.py  ->  bigdata/*.csv  ->  Power BI  ->  finding  ->  code change  ->  simulate.py ...
```

Example of the loop already run once: the sweep showed `edge_first` starves the
vRAN-DU, `analyze.py` confirmed `latency_slack` beats it with 95% confidence at
every load, and the winner was written into `config.json`. The same drill-down
then showed the remaining DU blocking is **100% edge node-capacity** (never
latency) — which is the next thing to fix (e.g. more edge CPU, a reserved DU
quota, or migration in Week 9).

> Not tested inside Power BI: I can't run Power BI here, so the steps and DAX
> below are written to match the CSV schemas exactly but are untested in the
> app. `analyze.py` computes the same numbers independently, so you can
> cross-check any visual against `bigdata/analysis_report.md`.

## 1. Generate the data

```bash
python3 simulate.py --seeds 50 --duration 300 --rates 1,2,4,6,8,12   # ~4.4M fact rows, ~1 min
python3 analyze.py                                                    # stats + recommendation
python3 export_results.py --all                                       # live runs -> CSV (optional)
```

Scale up with `--seeds`, `--duration`, `--rates`. Rows grow roughly linearly
(100 seeds x 600 s ~ 17M rows, ~1 GB of CSV).

## 2. Load (Power BI Desktop)

Get Data -> **Folder** -> `bigdata/` -> Combine is *not* needed; instead choose
**Text/CSV** once per file (7 files + 2 dims). Use **Import** mode.
Set types: `run_id`, `seed`, counts = Whole number; `cpu_util`, `mem_util`,
`bw_util`, `utilisation`, `*_pct` = Decimal.

## 3. Data model (star schema)

| Table | Type | Key |
|---|---|---|
| `dim_run` | dimension (policy, seed, rate) | `run_id` |
| `dim_service` | dimension | `service` |
| `dim_site` | dimension (tier, capacity) | `site` |
| `fact_requests`, `fact_resizes`, `fact_utilisation`, `fact_link`, `fact_run_service`, `fact_block_reasons` | facts | `run_id` |

Relationships (all many-to-one, single direction, from fact to dim):
- every fact `run_id` -> `dim_run[run_id]`
- `fact_requests[service]`, `fact_run_service[service]`, `fact_block_reasons[service]` -> `dim_service[service]`
- `fact_requests[site]`, `fact_utilisation[site]` -> `dim_site[site]`

## 4. DAX measures

```DAX
Offered        = COUNTROWS ( fact_requests )
Blocked        = CALCULATE ( COUNTROWS ( fact_requests ), fact_requests[outcome] = "BLOCKED" )
Blocking %     = DIVIDE ( [Blocked], [Offered] )
DU Blocking %  = CALCULATE ( [Blocking %], fact_requests[service] = "vRAN-DU" )

Avg CPU Util   = AVERAGE ( fact_utilisation[cpu_util] )
Peak Link Util = MAX ( fact_link[utilisation] )
Edge Share %   = DIVIDE (
                    CALCULATE ( COUNTROWS ( fact_requests ),
                                fact_requests[outcome] = "ACCEPTED", dim_site[tier] = "edge" ),
                    CALCULATE ( COUNTROWS ( fact_requests ), fact_requests[outcome] = "ACCEPTED" ) )

-- statistics across seeds (run-level, matches analyze.py)
Run Blocking Mean = AVERAGE ( dim_run[blocking_pct] )
Run Blocking CI   = 1.96 * DIVIDE ( STDEV.S ( dim_run[blocking_pct] ), SQRT ( COUNTROWS ( dim_run ) ) )

-- improvement vs baseline policy (percentage points, positive = better)
Baseline Blocking % =
    CALCULATE ( [Blocking %], dim_run[policy] = "edge_first", ALL ( dim_run[policy] ) )
Improvement (pp) = ( [Baseline Blocking %] - [Blocking %] ) * 100
```

## 5. Suggested report pages

1. **Policy comparison** - clustered column: `Blocking %` by `dim_run[policy]`, slicer on `arrival_rate`; add `Run Blocking CI` as error bars. Card: `Improvement (pp)`.
2. **Load curve** - line chart: `Blocking %` (y) vs `arrival_rate` (x), legend `policy`. Shows the saturation knee.
3. **Who gets starved** - matrix `service` x `policy` with `Blocking %`, heat-map conditional formatting. The DU row is the story.
4. **Bottleneck** - stacked bar of `fact_block_reasons[count]` by `reason`, filtered per service. Expect `node_capacity` dominant; `link_bandwidth` secondary.
5. **Utilisation over time** - line chart `Avg CPU Util` by `fact_utilisation[t]`, legend `site`; small multiples by policy. Shows edge saturation while core idles under `edge_first`.
6. **Links** - `Peak Link Util` by `fact_link[link]`; the 400 Mb/s edge<->edge link is the thin one.
7. **Live vs simulated** (optional) - load `live_*.csv` from `export_results.py`; compare live `Blocking %` at rate 2 against the simulated 95% band to validate the simulator.

## 6. Tableau

Same CSVs, same relationships (Data Source tab -> drag tables, join on `run_id`).
Equivalent calculated field: `SUM(IF [outcome]="BLOCKED" THEN 1 ELSE 0 END) / COUNT([outcome])`.

## 7. Going beyond millions of rows

CSV + Import mode comfortably handles the default ~4M rows. For 100M+ rows,
convert the fact tables to Parquet (needs `pyarrow`, not included), store them in
a lakehouse (Microsoft Fabric / Azure Data Lake), and use Direct Lake or
DirectQuery with aggregated tables; the star schema stays the same.
