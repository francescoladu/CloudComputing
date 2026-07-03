#!/usr/bin/env python3
"""Create a size-controlled Taobao UserBehavior subset.

The script:
- reads the complete CSV in streaming mode;
- removes Category ID from the output;
- samples at USER level, preserving each selected user's complete profile;
- favors users connected through shared buy/fav items;
- retains small low-activity and high-activity outlier groups;
- does NOT deduplicate repeated (user, item, behavior) events, because Job 1
  remains responsible for that operation;
- targets an output between 1.50 GB and 1.60 GB (decimal), with 1.55 GB as
  the default target.

No third-party packages are required.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

# User stats positions in the compact list stored in the dictionary.
PROJECTED_BYTES = 0
TOTAL_EVENTS = 1
USEFUL_EVENTS = 2
BUY_EVENTS = 3
FAV_EVENTS = 4

DEFAULT_INPUT = Path(r"C:\Users\fra-l\Desktop\UserBehavior.csv")
DEFAULT_OUTPUT = Path(r"C:\Users\fra-l\Desktop\UserBehaviorModified.csv")

GB = 1_000_000_000  # Decimal GB, matching Windows file-size conventions.


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a 1.5-1.6 GB user-level sample of Taobao UserBehavior.csv "
            "while preserving similarity-producing neighborhoods."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--target-gb", type=float, default=1.55)
    parser.add_argument("--min-gb", type=float, default=1.50)
    parser.add_argument("--max-gb", type=float, default=1.60)
    parser.add_argument("--seed", type=int, default=20260703)
    parser.add_argument(
        "--header",
        choices=("auto", "yes", "no"),
        default="auto",
        help="Whether the source CSV has a header. Default: auto-detect.",
    )
    parser.add_argument(
        "--write-header",
        action="store_true",
        help="Write canonical header: user_id,item_id,behavior,timestamp.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=2_000_000,
        help="Print progress every N input rows.",
    )
    return parser.parse_args()


def stable_hash64(value: int, seed: int) -> int:
    """Fast deterministic SplitMix64-style hash."""
    mask = (1 << 64) - 1
    x = (value ^ seed) & mask
    x = (x + 0x9E3779B97F4A7C15) & mask
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & mask
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & mask
    return (x ^ (x >> 31)) & mask


def is_integer(text: str) -> bool:
    try:
        int(text.strip())
        return True
    except ValueError:
        return False


def detect_header(path: Path) -> bool:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        first = next(reader, None)
    if not first:
        raise ValueError("The input CSV is empty.")
    return len(first) < 5 or not is_integer(first[0]) or not is_integer(first[1])


def iter_rows(path: Path, has_header: bool) -> Iterator[Tuple[int, str, str, str, str]]:
    """Yield (user_id_int, user_text, item_text, behavior, timestamp)."""
    with path.open("r", encoding="utf-8-sig", newline="", buffering=8 * 1024 * 1024) as handle:
        reader = csv.reader(handle)
        if has_header:
            next(reader, None)

        for row in reader:
            if len(row) < 5:
                continue

            user_text = row[0].strip()
            item_text = row[1].strip()
            behavior = row[3].strip().lower()
            timestamp = row[4].strip()

            try:
                user_id = int(user_text)
                int(item_text)
                int(timestamp)
            except ValueError:
                continue

            if not behavior:
                continue

            yield user_id, user_text, item_text, behavior, timestamp


def percentile(sorted_values: Sequence[int], fraction: float) -> int:
    if not sorted_values:
        return 0
    index = round((len(sorted_values) - 1) * fraction)
    return sorted_values[index]


def log_progress(pass_name: str, rows: int, started: float, every: int) -> None:
    if every <= 0 or rows % every != 0:
        return
    elapsed = max(time.time() - started, 0.001)
    rate = rows / elapsed
    print(f"[{pass_name}] {rows:,} rows | {rate:,.0f} rows/s", flush=True)


def collect_user_stats(
    input_path: Path,
    has_header: bool,
    progress_every: int,
) -> Tuple[Dict[int, List[int]], int, int]:
    """First pass: exact projected output bytes and user activity stats."""
    stats: Dict[int, List[int]] = {}
    valid_rows = 0
    started = time.time()

    for user_id, user_text, item_text, behavior, timestamp in iter_rows(input_path, has_header):
        valid_rows += 1
        # Output is user,item,behavior,timestamp plus three commas and one LF.
        line_bytes = len(user_text) + len(item_text) + len(behavior) + len(timestamp) + 4

        current = stats.get(user_id)
        if current is None:
            current = [0, 0, 0, 0, 0]
            stats[user_id] = current

        current[PROJECTED_BYTES] += line_bytes
        current[TOTAL_EVENTS] += 1

        if behavior == "buy":
            current[USEFUL_EVENTS] += 1
            current[BUY_EVENTS] += 1
        elif behavior == "fav":
            current[USEFUL_EVENTS] += 1
            current[FAV_EVENTS] += 1

        log_progress("pass 1/4: user statistics", valid_rows, started, progress_every)

    total_projected = sum(values[PROJECTED_BYTES] for values in stats.values())
    return stats, valid_rows, total_projected


def classify_users(
    stats: Dict[int, List[int]],
) -> Tuple[List[int], List[int], List[int], Dict[str, int]]:
    useful_distribution = sorted(values[USEFUL_EVENTS] for values in stats.values())
    total_distribution = sorted(values[TOTAL_EVENTS] for values in stats.values())

    useful_p10 = percentile(useful_distribution, 0.10)
    useful_p95 = percentile(useful_distribution, 0.95)
    total_p25 = percentile(total_distribution, 0.25)
    total_p95 = percentile(total_distribution, 0.95)

    low: List[int] = []
    high: List[int] = []
    regular: List[int] = []

    for user_id, values in stats.items():
        useful = values[USEFUL_EVENTS]
        total = values[TOTAL_EVENTS]

        # Low-activity outliers include sparse profiles and users with at most one
        # useful buy/fav event.
        is_low = useful <= max(1, useful_p10) and total <= total_p25

        # High-activity outliers are selected from the upper activity tail.
        is_high = useful >= max(2, useful_p95) or total >= total_p95

        if is_low:
            low.append(user_id)
        elif is_high:
            high.append(user_id)
        elif useful >= 2:
            regular.append(user_id)
        else:
            # Moderately sparse users are treated as low candidates for the final
            # fill stage, but they are not used as similarity anchors.
            low.append(user_id)

    thresholds = {
        "useful_p10": useful_p10,
        "useful_p95": useful_p95,
        "total_p25": total_p25,
        "total_p95": total_p95,
    }
    return regular, low, high, thresholds


def add_candidates_to_budget(
    candidates: Iterable[int],
    stats: Dict[int, List[int]],
    selected: Set[int],
    current_bytes: int,
    budget_limit: int,
    absolute_max: int,
) -> int:
    """Add complete user profiles without exceeding the supplied limits."""
    for user_id in candidates:
        if user_id in selected:
            continue
        size = stats[user_id][PROJECTED_BYTES]
        if current_bytes + size <= min(budget_limit, absolute_max):
            selected.add(user_id)
            current_bytes += size
    return current_bytes


def encode_item_behavior(item_text: str, behavior: str) -> Optional[int]:
    if behavior == "buy":
        behavior_bit = 0
    elif behavior == "fav":
        behavior_bit = 1
    else:
        return None
    return (int(item_text) << 1) | behavior_bit


def collect_anchor_items(
    input_path: Path,
    has_header: bool,
    core_users: Set[int],
    progress_every: int,
) -> Set[int]:
    anchor_items: Set[int] = set()
    rows = 0
    started = time.time()

    for user_id, _user_text, item_text, behavior, _timestamp in iter_rows(input_path, has_header):
        rows += 1
        if user_id in core_users:
            encoded = encode_item_behavior(item_text, behavior)
            if encoded is not None:
                anchor_items.add(encoded)
        log_progress("pass 2/4: anchor items", rows, started, progress_every)

    return anchor_items


def score_neighbor_users(
    input_path: Path,
    has_header: bool,
    anchor_items: Set[int],
    core_users: Set[int],
    regular_users: Set[int],
    progress_every: int,
) -> Dict[int, int]:
    """Score regular users sharing buy/fav items with the selected core."""
    scores: Dict[int, int] = {}
    rows = 0
    started = time.time()

    for user_id, _user_text, item_text, behavior, _timestamp in iter_rows(input_path, has_header):
        rows += 1
        if user_id not in core_users and user_id in regular_users:
            encoded = encode_item_behavior(item_text, behavior)
            if encoded is not None and encoded in anchor_items:
                scores[user_id] = scores.get(user_id, 0) + 1
        log_progress("pass 3/4: neighbor scoring", rows, started, progress_every)

    return scores


def choose_users(
    stats: Dict[int, List[int]],
    regular: List[int],
    low: List[int],
    high: List[int],
    input_path: Path,
    has_header: bool,
    seed: int,
    target_bytes: int,
    min_bytes: int,
    max_bytes: int,
    progress_every: int,
) -> Tuple[Set[int], int, Dict[str, int]]:
    selected: Set[int] = set()
    selected_bytes = 0

    # Byte budgets preserve a realistic mixture while prioritizing similarity.
    core_budget = int(target_bytes * 0.70)
    neighbor_budget_end = int(target_bytes * 0.90)
    low_budget_end = int(target_bytes * 0.95)
    high_budget_end = target_bytes

    regular_by_hash = sorted(regular, key=lambda user: stable_hash64(user, seed))
    selected_bytes = add_candidates_to_budget(
        regular_by_hash,
        stats,
        selected,
        selected_bytes,
        core_budget,
        max_bytes,
    )
    core_users = set(selected)

    print(
        f"Core selected: {len(core_users):,} users, "
        f"{selected_bytes / GB:.3f} GB projected",
        flush=True,
    )

    anchor_items = collect_anchor_items(
        input_path, has_header, core_users, progress_every
    )
    print(f"Anchor item-behavior keys: {len(anchor_items):,}", flush=True)

    regular_set = set(regular)
    neighbor_scores = score_neighbor_users(
        input_path,
        has_header,
        anchor_items,
        core_users,
        regular_set,
        progress_every,
    )

    neighbors = sorted(
        neighbor_scores,
        key=lambda user: (-neighbor_scores[user], stable_hash64(user, seed + 1)),
    )
    selected_bytes = add_candidates_to_budget(
        neighbors,
        stats,
        selected,
        selected_bytes,
        neighbor_budget_end,
        max_bytes,
    )
    after_neighbors_count = len(selected)

    low_by_hash = sorted(low, key=lambda user: stable_hash64(user, seed + 2))
    selected_bytes = add_candidates_to_budget(
        low_by_hash,
        stats,
        selected,
        selected_bytes,
        low_budget_end,
        max_bytes,
    )
    after_low_count = len(selected)

    high_by_hash = sorted(high, key=lambda user: stable_hash64(user, seed + 3))
    selected_bytes = add_candidates_to_budget(
        high_by_hash,
        stats,
        selected,
        selected_bytes,
        high_budget_end,
        max_bytes,
    )
    after_high_count = len(selected)

    # Fill any remaining gap. First prefer unselected users with neighborhood
    # evidence, then all remaining users deterministically.
    remaining_neighbors = [user for user in neighbors if user not in selected]
    selected_bytes = add_candidates_to_budget(
        remaining_neighbors,
        stats,
        selected,
        selected_bytes,
        target_bytes,
        max_bytes,
    )

    if selected_bytes < target_bytes:
        all_remaining = sorted(
            (user for user in stats if user not in selected),
            key=lambda user: stable_hash64(user, seed + 4),
        )
        selected_bytes = add_candidates_to_budget(
            all_remaining,
            stats,
            selected,
            selected_bytes,
            target_bytes,
            max_bytes,
        )

    # If atomic complete profiles leave us below the minimum, continue adding any
    # user that still fits under the hard maximum.
    if selected_bytes < min_bytes:
        for user_id in sorted(stats, key=lambda user: stable_hash64(user, seed + 5)):
            if user_id in selected:
                continue
            size = stats[user_id][PROJECTED_BYTES]
            if selected_bytes + size <= max_bytes:
                selected.add(user_id)
                selected_bytes += size
                if selected_bytes >= min_bytes:
                    break

    if selected_bytes < min_bytes:
        raise RuntimeError(
            f"Could not reach the minimum size: {selected_bytes / GB:.3f} GB. "
            "The source may be too small after removing Category ID."
        )

    composition = {
        "core_users": len(core_users),
        "neighbor_users": after_neighbors_count - len(core_users),
        "low_outlier_users": after_low_count - after_neighbors_count,
        "high_outlier_users": after_high_count - after_low_count,
        "total_selected_users": len(selected),
        "anchor_item_behavior_keys": len(anchor_items),
        "neighbor_candidates": len(neighbor_scores),
    }
    return selected, selected_bytes, composition


def write_output(
    input_path: Path,
    output_path: Path,
    has_header: bool,
    selected_users: Set[int],
    write_header: bool,
    progress_every: int,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")

    rows_written = 0
    rows_seen = 0
    started = time.time()

    try:
        with temporary_path.open(
            "w", encoding="utf-8", newline="", buffering=8 * 1024 * 1024
        ) as output_handle:
            writer = csv.writer(
                output_handle,
                delimiter=",",
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
            )
            if write_header:
                writer.writerow(["user_id", "item_id", "behavior", "timestamp"])

            for user_id, user_text, item_text, behavior, timestamp in iter_rows(
                input_path, has_header
            ):
                rows_seen += 1
                if user_id in selected_users:
                    # Category ID is intentionally omitted. Repeated events are kept so
                    # Reducer 1 can perform the required deduplication.
                    writer.writerow([user_text, item_text, behavior, timestamp])
                    rows_written += 1
                log_progress("pass 4/4: writing output", rows_seen, started, progress_every)

        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise

    return rows_written


def main() -> int:
    args = parse_args()

    input_path = args.input.expanduser().resolve()
    output_path = args.output.expanduser().resolve()

    if not input_path.is_file():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2
    if input_path == output_path:
        print("Input and output paths must be different.", file=sys.stderr)
        return 2

    min_bytes = int(args.min_gb * GB)
    target_bytes = int(args.target_gb * GB)
    max_bytes = int(args.max_gb * GB)

    if not (0 < min_bytes <= target_bytes <= max_bytes):
        print("Require: 0 < min-gb <= target-gb <= max-gb", file=sys.stderr)
        return 2

    if args.header == "auto":
        has_header = detect_header(input_path)
    else:
        has_header = args.header == "yes"

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Source header detected: {has_header}")
    print(
        f"Requested range: {args.min_gb:.2f}-{args.max_gb:.2f} GB; "
        f"target {args.target_gb:.2f} GB"
    )
    print("Repeated user-item-behavior events will be preserved for Job 1.")

    stats, valid_rows, total_projected = collect_user_stats(
        input_path, has_header, args.progress_every
    )
    print(
        f"Valid source rows: {valid_rows:,}; users: {len(stats):,}; "
        f"projected source without Category ID: {total_projected / GB:.3f} GB"
    )

    if total_projected < min_bytes:
        print(
            "After removing Category ID, the complete source is smaller than the "
            "requested minimum.",
            file=sys.stderr,
        )
        return 3

    regular, low, high, thresholds = classify_users(stats)
    print(
        f"User groups: regular={len(regular):,}, low={len(low):,}, "
        f"high={len(high):,}"
    )

    selected, projected_bytes, composition = choose_users(
        stats=stats,
        regular=regular,
        low=low,
        high=high,
        input_path=input_path,
        has_header=has_header,
        seed=args.seed,
        target_bytes=target_bytes,
        min_bytes=min_bytes,
        max_bytes=max_bytes,
        progress_every=args.progress_every,
    )

    print(
        f"Selected {len(selected):,} users; projected output "
        f"{projected_bytes / GB:.3f} GB"
    )

    rows_written = write_output(
        input_path,
        output_path,
        has_header,
        selected,
        args.write_header,
        args.progress_every,
    )

    actual_bytes = output_path.stat().st_size
    if not (min_bytes <= actual_bytes <= max_bytes):
        raise RuntimeError(
            f"Actual output size {actual_bytes / GB:.3f} GB is outside the "
            f"requested range {args.min_gb:.2f}-{args.max_gb:.2f} GB."
        )

    report = {
        "input": str(input_path),
        "output": str(output_path),
        "source_had_header": has_header,
        "output_has_header": args.write_header,
        "seed": args.seed,
        "valid_source_rows": valid_rows,
        "source_users": len(stats),
        "selected_users": len(selected),
        "rows_written": rows_written,
        "actual_output_bytes": actual_bytes,
        "actual_output_gb": actual_bytes / GB,
        "requested_min_gb": args.min_gb,
        "requested_target_gb": args.target_gb,
        "requested_max_gb": args.max_gb,
        "thresholds": thresholds,
        "composition": composition,
        "job1_deduplication_preserved": True,
    }
    report_path = output_path.with_suffix(output_path.suffix + ".report.json")
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Completed: {rows_written:,} rows written")
    print(f"Actual output size: {actual_bytes / GB:.3f} GB")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
