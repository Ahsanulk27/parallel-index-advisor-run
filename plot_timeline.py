"""
Plot a Gantt-style timeline of parallel workers from verbose whatif_parallel logs.

Overlapping horizontal bars (distinct PIDs active at the same wall-clock time)
are direct evidence of concurrent execution.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt


START_RE = re.compile(
    r"\[worker pid=(?P<pid>\d+)\] START\s+.* t=(?P<t>[0-9]+\.[0-9]+)"
)
END_RE = re.compile(
    r"\[worker pid=(?P<pid>\d+)\] END\s+.* t=(?P<t>[0-9]+\.[0-9]+)"
)


def parse_worker_spans(log_dir: Path) -> dict[int, tuple[float, float]]:
    """
    Read ``worker_*.log`` files and collect START/END wall-clock times per PID.

    :param log_dir: Directory containing per-worker log files
    :returns: Mapping of pid -> (start_epoch, end_epoch)
    """
    spans: dict[int, tuple[float, float]] = {}
    logs = sorted(log_dir.glob("worker_*.log"))
    if not logs:
        raise SystemExit(f"No worker_*.log files in {log_dir}")

    for path in logs:
        text = path.read_text(encoding="utf-8")
        start_match = START_RE.search(text)
        end_match = END_RE.search(text)
        if start_match is None or end_match is None:
            raise SystemExit(f"Missing START or END line in {path}")
        pid = int(start_match.group("pid"))
        start_t = float(start_match.group("t"))
        end_t = float(end_match.group("t"))
        spans[pid] = (start_t, end_t)
    return spans


def plot_timeline(spans: dict[int, tuple[float, float]], output: Path) -> None:
    """
    Draw one horizontal bar per worker from START to END.

    X-axis is seconds relative to the earliest START so bars can be compared.

    :param spans: pid -> (start_epoch, end_epoch)
    :param output: Image path to save
    """
    t0 = min(start for start, _ in spans.values())
    pids = sorted(spans)
    starts = [spans[pid][0] - t0 for pid in pids]
    widths = [spans[pid][1] - spans[pid][0] for pid in pids]
    labels = [f"pid {pid}" for pid in pids]
    y = range(len(pids))

    fig, ax = plt.subplots(figsize=(10, 1.2 + 0.45 * len(pids)))
    ax.barh(y, widths, left=starts, height=0.55, color="#3b6ea5")
    ax.set_yticks(list(y), labels)
    ax.set_xlabel("Wall-clock time (s from first worker START)")
    ax.set_title("Parallel worker overlap")
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    """
    Parse CLI options.

    :returns: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Gantt-style plot of verbose whatif_parallel worker logs"
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        required=True,
        help="Directory containing worker_<pid>.log files (e.g. worker_logs/.../workers_4)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("worker_timeline.png"),
        help="Output image path (default: worker_timeline.png)",
    )
    return parser.parse_args()


def main() -> None:
    """Parse logs and save a worker-overlap timeline image."""
    args = parse_args()
    if not args.log_dir.is_dir():
        raise SystemExit(f"Not a directory: {args.log_dir}")
    spans = parse_worker_spans(args.log_dir)
    plot_timeline(spans, args.output)
    print(f"workers plotted : {len(spans)}")
    t0 = min(start for start, _ in spans.values())
    for pid, (start, end) in sorted(spans.items()):
        print(f"  pid {pid}: {start - t0:.3f}s -> {end - t0:.3f}s  ({end - start:.3f}s)")
    print(f"saved           : {args.output}")


if __name__ == "__main__":
    main()
