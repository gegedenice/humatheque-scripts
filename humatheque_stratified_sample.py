#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas",
# ]
# ///
"""
Stratified sampling for a subcorpus from a filtered theses CSV.

Strata are defined by: year, language, set_ddc
Goal: sample exactly N items (or as close as possible if data constraints),
proportionally to stratum sizes, with optional minimum per stratum (only feasible
when number of strata <= N).

Usage:
  uv run humatheque_stratified_sample.py --input path/_filtered_humatheque_theses_diffusable_openaccess_flat.csv --n 60
  uv run humatheque_stratified_sample.py --input path/_filtered_humatheque_theses_diffusable_openaccess_flat.csv --output path/_sample_filtered_humatheque_theses_diffusable_openaccess_flat.csv --n 40 --seed 42
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


STRATA_COLS = ["year", "language", "set_ddc"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stratified sampling (year, language, set_ddc)")
    p.add_argument("--input", required=True, help="Input CSV path (filtered dataset)")
    p.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: same dir, prefixed with _subcorpus_)",
    )
    p.add_argument("--n", type=int, default=60, help="Target number of samples (default: 60)")
    p.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")
    p.add_argument(
        "--min-per-stratum",
        type=int,
        default=0,
        help=(
            "Minimum samples per stratum. WARNING: if >0 and nb_strata > n, "
            "it is impossible to respect both; script will auto-disable the min."
        ),
    )
    return p.parse_args(argv)


def compute_allocation(
    group_sizes: pd.DataFrame, n: int, min_per_stratum: int = 0
) -> pd.DataFrame:
    """
    group_sizes columns: STRATA_COLS + ['count']
    returns group_sizes with ['num_samples'] allocated.
    Allocation method:
      - proportional floats
      - floor to ints
      - distribute remaining by largest fractional part
      - cap by availability
      - redistribute if cap creates shortfall
    Optionally enforce min_per_stratum ONLY if feasible (nb_strata <= n).
    """
    gs = group_sizes.copy()
    total = int(gs["count"].sum())
    if total == 0:
        gs["num_samples"] = 0
        return gs

    nb_strata = len(gs)

    # Feasibility check for minimum enforcement
    if min_per_stratum > 0 and nb_strata * min_per_stratum > n:
        # Auto-disable to preserve primary goal (total close to n)
        min_per_stratum = 0

    gs["proportion"] = gs["count"] / total
    gs["num_samples_float"] = gs["proportion"] * n

    # Start with floors
    gs["num_samples"] = gs["num_samples_float"].astype(int)

    # Optional minimum if feasible
    if min_per_stratum > 0:
        gs["num_samples"] = gs["num_samples"].clip(lower=min_per_stratum)

    # Cap by availability
    gs["num_samples"] = gs[["num_samples", "count"]].min(axis=1)

    # Distribute remaining (due to flooring) using fractional parts
    remaining = n - int(gs["num_samples"].sum())
    if remaining > 0:
        gs["fractional_part"] = gs["num_samples_float"] - gs["num_samples"].astype(float)
        eligible = gs[gs["num_samples"] < gs["count"]].sort_values(
            by="fractional_part", ascending=False
        )
        for idx in eligible.head(remaining).index:
            gs.loc[idx, "num_samples"] += 1

    # Re-cap (in case)
    gs["num_samples"] = gs[["num_samples", "count"]].min(axis=1)

    # If still short (because many groups were capped), redistribute to biggest groups that can accept more
    shortfall = n - int(gs["num_samples"].sum())
    if shortfall > 0:
        eligible2 = gs[gs["num_samples"] < gs["count"]].sort_values(
            by="num_samples_float", ascending=False
        )
        for idx in eligible2.index:
            if shortfall <= 0:
                break
            gs.loc[idx, "num_samples"] += 1
            shortfall -= 1

    # Final cap
    gs["num_samples"] = gs[["num_samples", "count"]].min(axis=1)

    return gs


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"[ERROR] Input not found: {in_path}", file=sys.stderr)
        return 2

    out_path = Path(args.output) if args.output else in_path.with_name(f"_subcorpus_{in_path.name}")

    df = pd.read_csv(in_path, encoding=args.encoding)

    missing = [c for c in STRATA_COLS if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing required columns: {missing}", file=sys.stderr)
        return 3

    # Build group sizes
    group_sizes = (
        df.groupby(STRATA_COLS)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    allocation = compute_allocation(group_sizes, n=args.n, min_per_stratum=args.min_per_stratum)

    # Sample per stratum
    parts: list[pd.DataFrame] = []
    for _, row in allocation.iterrows():
        k = int(row["num_samples"])
        if k <= 0:
            continue
        mask = (df["year"] == row["year"]) & (df["language"] == row["language"]) & (df["set_ddc"] == row["set_ddc"])
        stratum = df.loc[mask]
        # sample is safe because k <= count
        parts.append(stratum.sample(n=k, random_state=args.seed))

    subcorpus_df = pd.concat(parts, ignore_index=True) if parts else df.head(0)

    # If we somehow got > n (shouldn't), trim deterministically
    if len(subcorpus_df) > args.n:
        subcorpus_df = subcorpus_df.sample(n=args.n, random_state=args.seed).reset_index(drop=True)

    subcorpus_df.to_csv(out_path, index=False, encoding=args.encoding)

    # Light report
    nb_strata_total = len(group_sizes)
    nb_strata_sampled = subcorpus_df.groupby(STRATA_COLS).size().shape[0] if len(subcorpus_df) else 0
    print(f"Target total samples: {args.n}")
    print(f"Actual total samples: {len(subcorpus_df)}")
    print(f"Total strata in input: {nb_strata_total}")
    print(f"Strata represented in sample: {nb_strata_sampled}")
    print(f"Output: {out_path}")

    # Optional: write a small summary next to output (counts per stratum)
    summary_path = out_path.with_suffix(".summary.csv")
    (
        subcorpus_df.groupby(STRATA_COLS)
        .size()
        .reset_index(name="sampled_count")
        .sort_values("sampled_count", ascending=False)
        .to_csv(summary_path, index=False, encoding=args.encoding)
    )
    print(f"Summary: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))