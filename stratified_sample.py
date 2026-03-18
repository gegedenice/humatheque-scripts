#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas",
# ]
# ///
"""
Generalized stratified sampling for a subcorpus from a CSV.

This script samples exactly N rows (or as close as possible when constrained by the data),
proportionally across user-defined strata. Strata columns are passed at runtime with
--strata-cols, so the same script can be reused for different corpora and metadata schemas.

Examples:
  uv run stratified_sample.py \
    --input data.csv \
    --strata-cols year language set_ddc \
    --n 60

  uv run stratified_sample.py \
    --input memoires.csv \
    --strata-cols year diplome hal_domain \
    --dropna-cols hal_id url \
    --dedup-cols hal_id \
    --n 40 \
    --seed 42

  uv run stratified_sample.py \
    --input memoires.csv \
    --strata-cols year diplome hal_domain \
    --filters "is_diffusable=true" "language=fr|en" \
    --dropna-cols hal_id url \
    --n 40
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


DEFAULT_STRATA_COLS = ["year", "language", "set_ddc"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Generalized stratified sampling from a CSV. "
            "Provide strata columns with --strata-cols."
        )
    )
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: same dir, prefixed with _subcorpus_)",
    )
    p.add_argument(
        "--summary-output",
        default=None,
        help="Optional summary CSV path (default: output path with .summary.csv suffix)",
    )
    p.add_argument("--n", type=int, default=60, help="Target number of samples (default: 60)")
    p.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")
    p.add_argument(
        "--sep",
        default=",",
        help="CSV separator for input and output (default: ,)",
    )
    p.add_argument(
        "--strata-cols",
        nargs="+",
        default=DEFAULT_STRATA_COLS,
        help=(
            "Columns defining the strata. "
            "Example: --strata-cols year diplome hal_domain"
        ),
    )
    p.add_argument(
        "--min-per-stratum",
        type=int,
        default=0,
        help=(
            "Minimum samples per stratum. If impossible (nb_strata * min > n), "
            "the minimum is automatically disabled."
        ),
    )
    p.add_argument(
        "--dropna-cols",
        nargs="*",
        default=[],
        help="Optional list of columns that must be non-null before sampling.",
    )
    p.add_argument(
        "--dedup-cols",
        nargs="*",
        default=[],
        help="Optional list of columns used to drop duplicates before sampling.",
    )
    p.add_argument(
        "--filters",
        nargs="*",
        default=[],
        help=(
            "Optional simple filters applied before sampling. Supported syntax: "
            "col=value, col=a|b|c, col!=value. Example: --filters language=fr|en is_diffusable=true"
        ),
    )
    p.add_argument(
        "--na-policy",
        choices=["keep", "drop"],
        default="drop",
        help=(
            "How to handle rows with missing values in strata columns. "
            "drop = exclude them before grouping (default), keep = fill with '__NA__'."
        ),
    )
    p.add_argument(
        "--report-prefix",
        default="sampled",
        help="Prefix used for count column names in the summary report (default: sampled)",
    )
    return p.parse_args(argv)



def parse_scalar(value: str):
    v = value.strip()
    low = v.lower()
    if low in {"true", "false"}:
        return low == "true"
    if low in {"none", "null"}:
        return None
    return v



def apply_filters(df: pd.DataFrame, filters: list[str]) -> pd.DataFrame:
    out = df.copy()
    for raw in filters:
        raw = raw.strip()
        if not raw:
            continue

        if "!=" in raw:
            col, rhs = raw.split("!=", 1)
            col = col.strip()
            values = [parse_scalar(v) for v in rhs.split("|")]
            if col not in out.columns:
                raise ValueError(f"Filter column not found: {col}")
            out = out.loc[~out[col].isin(values)]
            continue

        if "=" not in raw:
            raise ValueError(f"Unsupported filter syntax: {raw}")

        col, rhs = raw.split("=", 1)
        col = col.strip()
        values = [parse_scalar(v) for v in rhs.split("|")]
        if col not in out.columns:
            raise ValueError(f"Filter column not found: {col}")
        out = out.loc[out[col].isin(values)]

    return out



def compute_allocation(
    group_sizes: pd.DataFrame,
    strata_cols: list[str],
    n: int,
    min_per_stratum: int = 0,
) -> pd.DataFrame:
    """
    group_sizes columns: strata_cols + ['count']
    returns group_sizes with ['num_samples'] allocated.

    Allocation method:
      - proportional floats
      - floor to ints
      - distribute remaining by largest fractional part
      - cap by availability
      - redistribute if cap creates shortfall

    min_per_stratum is enforced only if feasible.
    """
    gs = group_sizes.copy()
    total = int(gs["count"].sum())
    if total == 0:
        gs["num_samples"] = 0
        return gs

    if n <= 0:
        gs["num_samples"] = 0
        return gs

    nb_strata = len(gs)
    if min_per_stratum > 0 and nb_strata * min_per_stratum > n:
        min_per_stratum = 0

    gs["proportion"] = gs["count"] / total
    gs["num_samples_float"] = gs["proportion"] * n
    gs["num_samples"] = gs["num_samples_float"].astype(int)

    if min_per_stratum > 0:
        gs["num_samples"] = gs["num_samples"].clip(lower=min_per_stratum)

    gs["num_samples"] = gs[["num_samples", "count"]].min(axis=1)

    remaining = n - int(gs["num_samples"].sum())
    if remaining > 0:
        gs["fractional_part"] = gs["num_samples_float"] - gs["num_samples"].astype(float)
        eligible = gs[gs["num_samples"] < gs["count"]].sort_values(
            by="fractional_part", ascending=False
        )
        for idx in eligible.head(remaining).index:
            gs.loc[idx, "num_samples"] += 1

    gs["num_samples"] = gs[["num_samples", "count"]].min(axis=1)

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

    gs["num_samples"] = gs[["num_samples", "count"]].min(axis=1)
    return gs



def build_mask(df: pd.DataFrame, strata_cols: list[str], row: pd.Series) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for col in strata_cols:
        mask &= df[col] == row[col]
    return mask



def main(argv: list[str]) -> int:
    args = parse_args(argv)

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"[ERROR] Input not found: {in_path}", file=sys.stderr)
        return 2

    out_path = Path(args.output) if args.output else in_path.with_name(f"_subcorpus_{in_path.name}")
    summary_path = (
        Path(args.summary_output)
        if args.summary_output
        else out_path.with_suffix(".summary.csv")
    )

    df = pd.read_csv(in_path, encoding=args.encoding, sep=args.sep)

    missing = [c for c in args.strata_cols if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing required strata columns: {missing}", file=sys.stderr)
        return 3

    if args.dropna_cols:
        missing_dropna = [c for c in args.dropna_cols if c not in df.columns]
        if missing_dropna:
            print(f"[ERROR] Missing dropna columns: {missing_dropna}", file=sys.stderr)
            return 4
        df = df.dropna(subset=args.dropna_cols)

    if args.dedup_cols:
        missing_dedup = [c for c in args.dedup_cols if c not in df.columns]
        if missing_dedup:
            print(f"[ERROR] Missing dedup columns: {missing_dedup}", file=sys.stderr)
            return 5
        df = df.drop_duplicates(subset=args.dedup_cols)

    if args.filters:
        try:
            df = apply_filters(df, args.filters)
        except ValueError as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            return 6

    if args.na_policy == "drop":
        df = df.dropna(subset=args.strata_cols)
    else:
        df = df.copy()
        for col in args.strata_cols:
            df[col] = df[col].fillna("__NA__")

    if df.empty:
        print("[ERROR] No rows left after preprocessing/filtering.", file=sys.stderr)
        return 7

    group_sizes = (
        df.groupby(args.strata_cols)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    allocation = compute_allocation(
        group_sizes,
        strata_cols=args.strata_cols,
        n=args.n,
        min_per_stratum=args.min_per_stratum,
    )

    parts: list[pd.DataFrame] = []
    for _, row in allocation.iterrows():
        k = int(row["num_samples"])
        if k <= 0:
            continue
        stratum = df.loc[build_mask(df, args.strata_cols, row)]
        if stratum.empty:
            continue
        parts.append(stratum.sample(n=k, random_state=args.seed))

    subcorpus_df = pd.concat(parts, ignore_index=True) if parts else df.head(0)

    if len(subcorpus_df) > int(args.n):
        subcorpus_df = subcorpus_df.sample(n=args.n, random_state=args.seed).reset_index(drop=True)

    subcorpus_df.to_csv(out_path, index=False, encoding=args.encoding, sep=args.sep)

    sample_count_col = f"{args.report_prefix}_count"
    allocation_report = allocation.copy()
    allocation_report = allocation_report.rename(columns={"num_samples": sample_count_col})
    allocation_report.to_csv(summary_path, index=False, encoding=args.encoding, sep=args.sep)

    nb_strata_total = len(group_sizes)
    nb_strata_sampled = (
        subcorpus_df.groupby(args.strata_cols).size().shape[0] if len(subcorpus_df) else 0
    )

    print(f"Target total samples: {args.n}")
    print(f"Actual total samples: {len(subcorpus_df)}")
    print(f"Total rows after preprocessing: {len(df)}")
    print(f"Total strata in input: {nb_strata_total}")
    print(f"Strata represented in sample: {nb_strata_sampled}")
    print(f"Strata columns: {args.strata_cols}")
    print(f"Output: {out_path}")
    print(f"Summary: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
