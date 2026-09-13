"""
Sequential vs parallel what-if index cost estimation (HypoPG + EXPLAIN).

Candidate indexes are generated as column permutations of width 1-3, matching
Index_EAB's permutation strategy. Queries are loaded from the local TPC-H
balanced JSON. Each (candidate, query) pair is independent. HypoPG indexes are
session-scoped, so every parallel worker opens its own PostgreSQL connection.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import time
from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import Any, TextIO

import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "thesis123",
    "dbname": "indextest",
}

DEFAULT_NUM_QUERIES = 41
DEFAULT_NUM_CANDIDATES = 1000
DEFAULT_WORKERS = "4"
MAX_INDEX_WIDTH = 3
QUERY_FILE = Path(__file__).resolve().parent / "tpch_balanced_42.json"

# Wider TPC-H column sets so width-3 permutations reach a few thousand candidates.
# Skip bulky comment/text columns.
TABLE_COLUMNS = {
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipmode",
        "l_shipinstruct",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderdate",
        "o_orderpriority",
        "o_shippriority",
        "o_orderstatus",
        "o_totalprice",
    ],
    "customer": [
        "c_custkey",
        "c_nationkey",
        "c_mktsegment",
        "c_acctbal",
        "c_phone",
    ],
    "supplier": [
        "s_suppkey",
        "s_nationkey",
        "s_acctbal",
        "s_phone",
    ],
    "part": [
        "p_partkey",
        "p_type",
        "p_brand",
        "p_size",
        "p_container",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_supplycost",
        "ps_availqty",
    ],
    "nation": [
        "n_nationkey",
        "n_regionkey",
    ],
}

def load_queries(limit: int) -> list[str]:
    """
    Load the first ``limit`` statements from the local TPC-H JSON workload.

    :param limit: Number of queries to keep
    :returns: SQL strings
    """
    if not QUERY_FILE.is_file():
        raise SystemExit(f"Query file not found: {QUERY_FILE}")
    with QUERY_FILE.open(encoding="utf-8") as handle:
        queries: list[str] = json.load(handle)
    if limit < 1:
        raise SystemExit("--num-queries must be >= 1")
    if limit > len(queries):
        raise SystemExit(
            f"--num-queries {limit} exceeds the {len(queries)} queries in {QUERY_FILE.name}"
        )
    return queries[:limit]


@dataclass(frozen=True)
class CandidateIndex:
    """One generated hypothetical index."""

    index_id: int
    spec: str


@dataclass(frozen=True)
class WorkerJob:
    """Work sent to one process: queries, a candidate slice, and log settings."""

    queries: list[str]
    chunk: list[CandidateIndex]
    verbose: bool
    log_dir: str | None


@dataclass(frozen=True)
class EstimateResult:
    """Planner cost for one (query, index) pair."""

    query_id: int
    index_id: int
    cost: float


@dataclass(frozen=True)
class SweepRow:
    """Timing row for sequential or one parallel worker count."""

    label: str
    workers: int
    seconds: float
    speedup: float
    costs_match: bool | None


def connect() -> Any:
    """
    Open a new PostgreSQL connection for this process.

    :returns: A psycopg2 connection with autocommit enabled
    """
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def permutation_pool(max_width: int = MAX_INDEX_WIDTH) -> list[str]:
    """
    Enumerate all width-1..max_width column permutations for TABLE_COLUMNS.

    :param max_width: Longest index (number of columns)
    :returns: Stable-ordered ``table#col1,col2`` specs
    """
    specs: list[str] = []
    for table, columns in TABLE_COLUMNS.items():
        for width in range(1, max_width + 1):
            if width > len(columns):
                continue
            for combo in itertools.permutations(columns, width):
                specs.append(f"{table}#{','.join(combo)}")
    specs.sort()
    return specs


def generate_candidate_indexes(
    limit: int,
    max_width: int = MAX_INDEX_WIDTH,
) -> tuple[list[str], int]:
    """
    Build candidate indexes as column permutations of width 1..max_width.

    Mirrors Index_EAB's permutation candidate generation, then truncates to
    ``limit`` so the experiment stays controlled.

    :param limit: Maximum number of candidates to keep
    :param max_width: Longest index (number of columns)
    :returns: Truncated specs and the full permutation-pool size
    """
    specs = permutation_pool(max_width)
    if limit > len(specs):
        raise SystemExit(
            f"--num-candidates {limit} exceeds the generated pool of {len(specs)}"
        )
    if limit == len(specs):
        return specs, len(specs)
    step = len(specs) / limit
    sampled = [specs[int(i * step)] for i in range(limit)]
    return sampled, len(specs)


def index_ddl(index_spec: str) -> str:
    """
    Convert a compact spec like ``lineitem#l_shipdate,l_discount`` into DDL.

    :param index_spec: ``table#col1,col2`` candidate definition
    :returns: ``CREATE INDEX ON ...`` statement for HypoPG
    """
    table, columns = index_spec.split("#", 1)
    return f"CREATE INDEX ON {table} ({columns})"


def explain_cost(conn: Any, query: str) -> float:
    """
    Return the planner's estimated total cost for ``query``.

    :param conn: Open database connection
    :param query: SQL text to explain (not executed)
    :returns: ``Total Cost`` from the root plan node
    """
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        raw = cur.fetchone()[0]
    plan = json.loads(raw) if isinstance(raw, str) else raw
    return float(plan[0]["Plan"]["Total Cost"])


def estimate_pair(conn: Any, query: str, index_spec: str) -> float:
    """
    Create one hypothetical index, EXPLAIN the query, then drop the index.

    :param conn: Session that will own the HypoPG index
    :param query: SQL text to cost
    :param index_spec: Candidate index in ``table#cols`` form
    :returns: Estimated total cost with that hypothetical index present
    """
    ddl = index_ddl(index_spec)
    with conn.cursor() as cur:
        cur.execute("SELECT indexrelid FROM hypopg_create_index(%s);", (ddl,))
        oid = cur.fetchone()[0]
    try:
        return explain_cost(conn, query)
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT hypopg_drop_index(%s);", (oid,))


def split_candidates(
    candidates: list[CandidateIndex],
    n_chunks: int,
) -> list[list[CandidateIndex]]:
    """
    Round-robin partition of candidate indexes across workers.

    Each worker later costs every query against its assigned candidates.

    :param candidates: Full candidate list
    :param n_chunks: Number of worker chunks
    :returns: Non-empty candidate slices
    """
    chunks: list[list[CandidateIndex]] = [[] for _ in range(n_chunks)]
    for i, candidate in enumerate(candidates):
        chunks[i % n_chunks].append(candidate)
    return [chunk for chunk in chunks if chunk]


def open_worker_log(verbose: bool, log_dir: str | None, pid: int) -> TextIO | None:
    """
    Open this process's log file when verbose logging is enabled.

    :param verbose: Whether to write a per-worker log
    :param log_dir: Directory for ``worker_<pid>.log`` files
    :param pid: Current process id
    :returns: An open text handle, or None
    """
    if not verbose or not log_dir:
        return None
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    return (path / f"worker_{pid}.log").open("w", encoding="utf-8")


def write_log(handle: TextIO | None, message: str) -> None:
    """
    Write one log line and flush so a crash still leaves a complete trail.

    :param handle: Worker log file, or None when logging is off
    :param message: Line to append (no trailing newline required)
    """
    if handle is None:
        return
    handle.write(message + "\n")
    handle.flush()


def process_candidates(job: WorkerJob) -> list[EstimateResult]:
    """
    Cost every query against this worker's candidate indexes on one connection.

    :param job: Query list, candidate slice, and optional verbose log settings
    :returns: Cost results for all (query, candidate) pairs in the chunk
    """
    queries = job.queries
    chunk = job.chunk
    worker_pid = os.getpid()
    start_perf = time.perf_counter()
    log_handle = open_worker_log(job.verbose, job.log_dir, worker_pid)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT hypopg_reset();")
        write_log(
            log_handle,
            f"[worker pid={worker_pid}] START  chunk_size={len(chunk)} "
            f"queries={len(queries)} t={time.time():.4f}",
        )
        results: list[EstimateResult] = []
        for candidate in chunk:
            candidate_t = time.time()
            for query_id, query in enumerate(queries):
                results.append(
                    EstimateResult(
                        query_id=query_id,
                        index_id=candidate.index_id,
                        cost=estimate_pair(conn, query, candidate.spec),
                    )
                )
            write_log(
                log_handle,
                f"[worker pid={worker_pid}] candidate={candidate.index_id} "
                f"queries_run={len(queries)} t={candidate_t:.4f}",
            )
        elapsed = time.perf_counter() - start_perf
        write_log(
            log_handle,
            f"[worker pid={worker_pid}] END    checks_done={len(results)} "
            f"elapsed={elapsed:.4f}s t={time.time():.4f}",
        )
        return results
    finally:
        conn.close()
        if log_handle is not None:
            log_handle.close()


def run_sequential(
    queries: list[str],
    candidates: list[CandidateIndex],
) -> list[EstimateResult]:
    """
    Estimate every pair on a single connection in this process.

    Sequential runs never write verbose worker logs so a ``--workers 4 --verbose``
    log directory contains only the parallel PIDs.

    :param queries: SQL statements to cost
    :param candidates: Full candidate list
    :returns: Cost results in input order
    """
    return process_candidates(
        WorkerJob(queries=queries, chunk=candidates, verbose=False, log_dir=None)
    )


def run_parallel(
    queries: list[str],
    candidates: list[CandidateIndex],
    workers: int,
    verbose: bool = False,
    log_dir: Path | None = None,
) -> list[EstimateResult]:
    """
    Split candidates across worker processes, each with its own DB connection.

    :param queries: SQL statements to cost
    :param candidates: Full candidate list
    :param workers: Number of processes / connections
    :param verbose: Write per-worker START/END and per-candidate timestamps
    :param log_dir: Directory for ``worker_<pid>.log`` files
    :returns: Cost results sorted by (query_id, index_id)
    """
    chunks = split_candidates(candidates, workers)
    log_dir_str = str(log_dir) if verbose and log_dir is not None else None
    jobs = [
        WorkerJob(
            queries=queries,
            chunk=chunk,
            verbose=verbose,
            log_dir=log_dir_str,
        )
        for chunk in chunks
    ]
    with Pool(processes=len(chunks)) as pool:
        nested = pool.map(process_candidates, jobs)
    results = [row for chunk_rows in nested for row in chunk_rows]
    results.sort(key=lambda row: (row.query_id, row.index_id))
    return results


def warmup() -> None:
    """
    Open one connection and run a cheap EXPLAIN so cold-start cost is outside timing.
    """
    conn = connect()
    try:
        explain_cost(conn, "SELECT 1")
        with conn.cursor() as cur:
            cur.execute("SELECT hypopg_reset();")
    finally:
        conn.close()


def costs_match(
    sequential: list[EstimateResult],
    parallel: list[EstimateResult],
) -> bool:
    """
    Check that both runs produced the same planner costs.

    :param sequential: Sequential results
    :param parallel: Parallel results
    :returns: True if every (query, index) cost matches
    """
    seq_map = {(row.query_id, row.index_id): row.cost for row in sequential}
    par_map = {(row.query_id, row.index_id): row.cost for row in parallel}
    if seq_map.keys() != par_map.keys():
        return False
    return all(seq_map[key] == par_map[key] for key in seq_map)


def parse_worker_list(value: str) -> list[int]:
    """
    Parse a comma-separated worker list such as ``1,2,4,8``.

    :param value: CLI string
    :returns: Positive worker counts
    """
    workers = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not workers or any(count < 1 for count in workers):
        raise argparse.ArgumentTypeError("workers must be positive integers")
    return workers


def parse_args() -> argparse.Namespace:
    """
    Parse CLI options.

    :returns: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Sequential vs parallel HypoPG what-if cost estimation"
    )
    parser.add_argument(
        "--num-queries",
        type=int,
        default=DEFAULT_NUM_QUERIES,
        help=f"TPC-H queries to keep from {QUERY_FILE.name} (default: {DEFAULT_NUM_QUERIES})",
    )
    parser.add_argument(
        "--num-candidates",
        type=int,
        default=DEFAULT_NUM_CANDIDATES,
        help=f"Permutation candidates to keep (default: {DEFAULT_NUM_CANDIDATES})",
    )
    parser.add_argument(
        "--workers",
        type=parse_worker_list,
        default=parse_worker_list(DEFAULT_WORKERS),
        help=f"Parallel worker counts to sweep (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Write per-worker START/END and per-candidate timestamps to log files",
    )
    return parser.parse_args()


def print_sweep_table(rows: list[SweepRow], total_checks: int) -> None:
    """
    Print worker-count vs speedup results.

    :param rows: Sequential baseline plus each parallel configuration
    :param total_checks: Unique (query, candidate) pairs timed
    """
    print()
    print(f"Worker sweep  ({total_checks} unique checks)")
    print(f"  {'config':<14} {'workers':>8} {'time (s)':>10} {'speedup':>10} {'costs match':>12}")
    for row in rows:
        match = "-" if row.costs_match is None else str(row.costs_match)
        print(
            f"  {row.label:<14} {row.workers:>8} {row.seconds:>10.3f} "
            f"{row.speedup:>9.2f}x {match:>12}"
        )


def main() -> None:
    """Run sequential baseline, then a parallel worker sweep."""
    args = parse_args()
    if args.num_candidates < 1:
        raise SystemExit("--num-candidates must be >= 1")

    queries = load_queries(args.num_queries)
    specs, pool_size = generate_candidate_indexes(args.num_candidates)
    candidates = [
        CandidateIndex(index_id=i, spec=spec) for i, spec in enumerate(specs)
    ]
    total_checks = len(queries) * len(candidates)

    print("What-if index cost estimation: sequential vs parallel")
    print(f"  queries              : {len(queries)}")
    print(f"  permutation pool     : {pool_size} (width 1-{MAX_INDEX_WIDTH})")
    print(f"  candidates used      : {len(candidates)}")
    print(f"  unique checks        : {total_checks}")
    print(f"  worker sweep         : {args.workers}")
    print(f"  verbose logging      : {args.verbose}")
    print()

    run_log_root: Path | None = None
    if args.verbose:
        run_log_root = Path("worker_logs") / time.strftime("%Y%m%d_%H%M%S")
        run_log_root.mkdir(parents=True, exist_ok=True)
        print(f"  verbose log dir      : {run_log_root}")
        print()

    print("Warming up planner / connection...")
    warmup()

    print("Running sequential baseline (1 connection)...")
    t0 = time.perf_counter()
    sequential = run_sequential(queries, candidates)
    sequential_s = time.perf_counter() - t0
    print(f"  sequential time      : {sequential_s:.3f} s")

    rows = [
        SweepRow(
            label="sequential",
            workers=1,
            seconds=sequential_s,
            speedup=1.0,
            costs_match=None,
        )
    ]

    for workers in args.workers:
        print(f"Running parallel ({workers} workers, 1 connection each)...")
        worker_log_dir = None
        if args.verbose and run_log_root is not None:
            worker_log_dir = run_log_root / f"workers_{workers}"
            print(f"  verbose logs         : {worker_log_dir}")
        t1 = time.perf_counter()
        parallel = run_parallel(
            queries,
            candidates,
            workers,
            verbose=args.verbose,
            log_dir=worker_log_dir,
        )
        parallel_s = time.perf_counter() - t1
        speedup = sequential_s / parallel_s if parallel_s > 0 else 0.0
        matched = costs_match(sequential, parallel)
        print(f"  parallel time        : {parallel_s:.3f} s  ({speedup:.2f}x, match={matched})")
        rows.append(
            SweepRow(
                label=f"parallel x{workers}",
                workers=workers,
                seconds=parallel_s,
                speedup=speedup,
                costs_match=matched,
            )
        )

    print_sweep_table(rows, total_checks)

    print()
    print("Planner cost range per query (min/max across candidate indexes):")
    for q_id in range(len(queries)):
        costs = [row.cost for row in sequential if row.query_id == q_id]
        print(
            f"  Q{q_id + 1:<2}  min={min(costs):.2f}  max={max(costs):.2f}  "
            f"spread={max(costs) - min(costs):.2f}"
        )

    if args.verbose and run_log_root is not None:
        print()
        print("Verbose worker logs written. Plot a Gantt chart with:")
        print(
            f"  python plot_timeline.py --log-dir {run_log_root / f'workers_{args.workers[-1]}'}"
        )


if __name__ == "__main__":
    main()
