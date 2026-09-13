# Parallelizing What-If Index Cost Estimation

CSE449 (HPC / Parallel Computing) demo: speed up **what-if index analysis** by running independent HypoPG + `EXPLAIN` checks in parallel, instead of one pair at a time.

This keeps the **same cost model** as a real index advisor (PostgreSQL planner + HypoPG). It does **not** use cheaper approximate models such as INUM or C-PQO.

## Why this exists

Index advisors evaluate many candidate indexes against a query workload:

```text
for each candidate index C:
    for each query Q:
        create hypothetical index C
        EXPLAIN Q   # estimated cost as if C existed
        drop C
```

Each `(C, Q)` pair is independent, but HypoPG hypothetical indexes are **session-scoped**. Workers cannot share one database connection. Each process opens its **own** PostgreSQL connection.

The original thesis run hung for 2+ hours and crashed an 8GB laptop because **candidate count exploded** (column-permutation generation), not because a single `EXPLAIN` was slow.

## What the script does

1. Loads TPC-H queries from `tpch_balanced_42.json`.
2. Generates candidate indexes as **column permutations of width 1–3** (same idea as Index_EAB), then keeps `--num-candidates` of them.
3. Times a **sequential** baseline (one connection).
4. Times a **parallel** run (`multiprocessing` pool; one connection per worker).
5. Checks that sequential and parallel **planner costs match**.

Default workload: **41 queries × 1,000 candidates = 41,000** unique checks.

## Setup

The script connects to PostgreSQL on **localhost:5433** as `postgres` / `thesis123`, database `indextest`. Build that environment from this repo (PostgreSQL 15 + HypoPG 1.4.2). You do **not** need the Index_EAB codebase.

### 1. Database container

```bash
docker build -t postgres-hypopg .
docker run -d --name index-eab-db \
  -e POSTGRES_PASSWORD=thesis123 \
  -e POSTGRES_DB=indextest \
  -p 5433:5432 \
  postgres-hypopg
```

Wait until Postgres is ready, then confirm HypoPG and the TPC-H tables exist:

```bash
docker exec index-eab-db psql -U postgres -d indextest -c "SELECT * FROM hypopg();"
docker exec index-eab-db psql -U postgres -d indextest -c "\dt"
```

The first command should return an empty table (extension loaded). `\dt` should list `region`, `nation`, `supplier`, `part`, `partsupp`, `customer`, `orders`, and `lineitem`.

If port 5433 is taken, map another host port and change `port` in `DB_CONFIG` inside `whatif_parallel.py`.

### 2. Load TPC-H data (recommended)

The image creates the schema only. Planner costs are more realistic with data; scale factor **0.1** matches this demo (about 600k `lineitem` rows). Generate `.tbl` files with [TPC-H dbgen](https://github.com/electrum/tpch-dbgen) (`dbgen -s 0.1`), copy them into the container, then:

```bash
docker exec -it index-eab-db psql -U postgres -d indextest -c "\
COPY region   FROM '/data/region.tbl'   WITH (FORMAT csv, DELIMITER '|'); \
COPY nation   FROM '/data/nation.tbl'   WITH (FORMAT csv, DELIMITER '|'); \
COPY supplier FROM '/data/supplier.tbl' WITH (FORMAT csv, DELIMITER '|'); \
COPY part     FROM '/data/part.tbl'     WITH (FORMAT csv, DELIMITER '|'); \
COPY partsupp FROM '/data/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|'); \
COPY customer FROM '/data/customer.tbl' WITH (FORMAT csv, DELIMITER '|'); \
COPY orders   FROM '/data/orders.tbl'   WITH (FORMAT csv, DELIMITER '|'); \
COPY lineitem FROM '/data/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|'); \
ANALYZE;"
```

TPC-H `.tbl` rows often have a trailing `|`. If `COPY` fails, strip that last delimiter before loading. Mount the data directory when you start the container, for example `-v /path/to/tbls:/data`.

The script still runs on empty tables; speedup is valid, cost spreads will be smaller.

### 3. Python

```bash
pip install psycopg2-binary matplotlib
python -c "import psycopg2; print('ok')"
```

`matplotlib` is only needed for the optional Gantt plot (`plot_timeline.py`).

## Run

From this directory:

```bash
python whatif_parallel.py
```

That uses 41 queries, 1,000 candidates, and 4 workers (no per-worker logs).

Worker sweep (same workload, longer):

```bash
python whatif_parallel.py --workers 2,4,6,8
```

Use this **without** `--verbose` for speedup numbers. Logging flushes a line after every candidate and can skew timings (especially `parallel x1`).

Smaller / larger:

```bash
python whatif_parallel.py --num-queries 10 --num-candidates 300 --workers 1,2,4,6,8
python whatif_parallel.py --num-queries 41 --num-candidates 1000 --workers 4
```

`--num-candidates` cannot exceed the generated permutation pool (currently **3,468**). `--num-queries` cannot exceed the 41 statements in `tpch_balanced_42.json`.

### Concurrent-worker logs (optional)

`--verbose` writes one `worker_<pid>.log` per parallel process (START, per-candidate, END, with wall-clock `t=`). Sequential is not logged. Plot overlapping PIDs:

```bash
python whatif_parallel.py --workers 4 --verbose
python plot_timeline.py --log-dir worker_logs/<timestamp>/workers_4
```

That saves `worker_timeline.png`. Overlapping bars mean workers ran at the same time. `worker_logs/` is gitignored.

## Reading the output

| Field | Meaning |
|---|---|
| `sequential time` | Wall clock for all checks on one connection |
| `parallel time` | Wall clock with N worker processes / connections |
| `speedup` | sequential / parallel |
| `costs match` | Same `(query, index)` planner costs in both runs |
| `spread` | For one query, `max(cost) - min(cost)` across candidates. Large spread means HypoPG actually changed the estimated plan. Zero means none of those indexes helped that query. |

Speedup often **tapers after 4–6 workers** on an 8-core laptop: each worker is a Python process **plus** a Postgres backend, all hitting one database.

## Files

| File | Role |
|---|---|
| `whatif_parallel.py` | Sequential vs parallel experiment |
| `plot_timeline.py` | Gantt chart from `--verbose` worker logs |
| `tpch_balanced_42.json` | TPC-H-style query list |
| `Dockerfile` | PostgreSQL 15 image with HypoPG 1.4.2 compiled in |
| `docker/init/` | Creates HypoPG + TPC-H schema on first container start |

Do not commit `__pycache__/`, `worker_logs/`, or generated `worker_timeline.png`.
