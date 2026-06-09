"""Generate one increment — a ~2hr SLA window of mixed CDC changes.

Honors the change-type mix (incl. header-only updates that touch no detail) and
an optional single-provider hot spot via `--provider_concentration`. Lands one
parquet batch per affected table and advances `_demo_control`.

Skew demo: run twice with the same `--rows_per_increment` but
`--provider_concentration 0.0` then `0.45`, and compare the pipeline's task
distribution + wall-clock. They match — because AUTO CDC merges on the
high-cardinality claim key, not on provider_id.

    databricks bundle run generate_increment_job -t dev -- \
        --provider_concentration 0.45
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F


def _code_root():
    """Locate the deployed bundle files root (passed via --code_root) so the
    sibling `generator` module imports under a serverless spark_python_task,
    where __file__ is not defined."""
    for i, arg in enumerate(sys.argv):
        if arg.startswith("--code_root="):
            return arg.split("=", 1)[1]
        if arg == "--code_root" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return os.getcwd()


sys.path.insert(0, os.path.join(_code_root(), "src", "data_gen"))
import generator as g

spark = SparkSession.builder.getOrCreate()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--landing_volume", default="landing")
    p.add_argument("--rows_per_increment", type=int, default=417_000)  # claim headers
    p.add_argument("--provider_concentration", type=float, default=0.0)
    p.add_argument("--change_mix", default="")  # optional JSON override
    # Discrete mix overrides (robust to pass through CLI; preferred over JSON):
    p.add_argument("--pct_new", type=float, default=None)
    p.add_argument("--pct_header_only", type=float, default=None)
    p.add_argument("--pct_adjustment", type=float, default=None)
    p.add_argument("--pct_reversal", type=float, default=None)
    p.add_argument("--code_root", default="")  # consumed at import time
    a = p.parse_args()

    if any(x is not None for x in (a.pct_new, a.pct_header_only, a.pct_adjustment, a.pct_reversal)):
        mix = {"new": a.pct_new or 0.0, "header_only": a.pct_header_only or 0.0,
               "adjustment": a.pct_adjustment or 0.0, "reversal": a.pct_reversal or 0.0}
    elif a.change_mix:
        mix = json.loads(a.change_mix)
    else:
        mix = g.DEFAULT_CHANGE_MIX
    hot = a.provider_concentration

    control = f"{a.catalog}.{a.schema}._demo_control"
    c = spark.table(control).head()
    max_n, seq_base, run_index = int(c["max_claim_n"]), int(c["seq_base"]), int(c["run_index"])
    run = run_index + 1
    label = f"incr{run:04d}"
    n = a.rows_per_increment

    n_new = int(n * mix["new"])
    n_hdr = int(n * mix["header_only"])
    n_adj = int(n * mix["adjustment"])
    n_rev = int(n * mix["reversal"])
    print(f"Increment {label}: {n:,} header changes "
          f"(new={n_new:,} header_only={n_hdr:,} adjust={n_adj:,} reversal={n_rev:,}), "
          f"provider_concentration={hot}")

    # NEW claims get fresh ids beyond the high-water mark.
    new_base = (spark.range(max_n + 1, max_n + 1 + n_new)
                .withColumnRenamed("id", "n").withColumn("status", F.lit("SUBMITTED")))

    # Existing-claim changes: sample ONE distinct set, then partition into
    # disjoint buckets so no claim is both adjusted and reversed in the same
    # increment (which would otherwise leave a reversed claim with live detail).
    n_existing = n_hdr + n_adj + n_rev
    pool = (spark.range(n_existing * 3)
            .select((F.rand() * max_n + 1).cast("long").alias("n"))
            .distinct()
            .limit(n_existing)
            .withColumn("_b", F.rand()))
    p_hdr = n_hdr / n_existing
    p_adj = (n_hdr + n_adj) / n_existing

    hdr_base = pool.filter(F.col("_b") < p_hdr).withColumn("status", F.lit("DENIED"))      # header only, no detail
    adj_base = pool.filter((F.col("_b") >= p_hdr) & (F.col("_b") < p_adj)).withColumn("status", F.lit("ADJUSTED"))  # header + detail
    rev_base = pool.filter(F.col("_b") >= p_adj).withColumn("status", F.lit("REVERSED"))   # header + delete detail

    # accumulate (df, oper) per table, then union + write
    out = {t: [] for t in
           ["claim", "claim_detail", "claim_attribute", "claim_diagnosis", "claim_payment", "claim_audit"]}

    # claim header — every change type
    out["claim"] += [
        (g._claim_header(new_base, hot), "I"),
        (g._claim_header(hdr_base, hot), "U"),
        (g._claim_header(adj_base, hot), "U"),
        (g._claim_header(rev_base, hot), "U"),
    ]
    # claim_detail — new (I) + adjustment (U); reversal deletes; header_only touches NONE
    out["claim_detail"] += [
        (g._claim_details(new_base, hot), "I"),
        (g._claim_details(adj_base, hot), "U"),
        (g._claim_detail_keys_only(rev_base), "D"),
    ]
    # children for new claims
    out["claim_attribute"] += [(g._claim_attribute(new_base), "I")]
    out["claim_diagnosis"] += [(g._claim_diagnosis(new_base), "I")]
    # payments on adjustments + reversals
    out["claim_payment"] += [
        (g._claim_payment(adj_base, hot, run), "U"),
        (g._claim_payment(rev_base, hot, run), "U"),
    ]
    # audit row for every changed claim
    out["claim_audit"] += [
        (g._claim_audit(new_base, "SUBMIT", run), "I"),
        (g._claim_audit(hdr_base, "DENY", run), "I"),
        (g._claim_audit(adj_base, "ADJUST", run), "I"),
        (g._claim_audit(rev_base, "REVERSE", run), "I"),
    ]

    for table, parts in out.items():
        df = None
        for frame, oper in parts:
            stamped = g._cdc(frame, oper, seq_base)
            df = stamped if df is None else df.unionByName(stamped)
        path = f"/Volumes/{a.catalog}/{a.schema}/{a.landing_volume}/{table}/{label}"
        df.write.mode("append").parquet(path)
        print(f"  wrote {table:18} -> {path}")

    # small dimension drift (a few provider attribute updates) for parallelism realism
    prov_upd = (spark.range(100).select(
        F.format_string("PRV%06d", (F.rand() * g.N_PROVIDERS + 1).cast("long")).alias("provider_id"),
        F.format_string("%010d", (F.rand(1) * 9_999_999_999).cast("long")).alias("npi"),
        F.concat(F.lit("Provider "), (F.rand(2) * g.N_PROVIDERS).cast("int").cast("string")).alias("provider_name"),
        g._pick(F.rand(3), ["FAMILY_MED", "CARDIOLOGY", "ORTHO", "RADIOLOGY", "INTERNAL_MED"]).alias("specialty"),
        g._pick(F.rand(4), ["IN_NETWORK", "OUT_OF_NETWORK"]).alias("network_status"),
        g._pick(F.rand(5), ["CA", "TX", "FL", "WA", "OH", "MI"]).alias("state"),
        F.current_timestamp().alias("last_updated"),
    ))
    prov_path = f"/Volumes/{a.catalog}/{a.schema}/{a.landing_volume}/provider/{label}"
    g._cdc(prov_upd, "U", seq_base).write.mode("append").parquet(prov_path)
    print(f"  wrote provider (drift)  -> {prov_path}")

    # advance control
    spark.createDataFrame(
        [(int(max_n + n_new), int(seq_base + 1), int(run), datetime.now(timezone.utc))],
        ["max_claim_n", "seq_base", "run_index", "updated_at"],
    ).write.mode("overwrite").saveAsTable(control)
    print(f"Increment {label} complete. Control advanced: max_claim_n={max_n + n_new:,}, run_index={run}")


if __name__ == "__main__":
    main()
