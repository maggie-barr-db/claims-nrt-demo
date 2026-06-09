"""Seed historical depth — one large all-INSERT baseline into the landing zone.

Run ONCE before the demo. Generates ~`seed_rows` claim-detail rows (and the
proportional header / child / dimension rows) as `I` change records, lands them
as parquet, and writes the `_demo_control` high-water mark. The pipeline's first
run ingests this as the historical baseline so later increments' AUTO CDC merges
prune against real scale (this is what makes liquid clustering on claim_id matter).

Run as a serverless Python task, e.g.:
    databricks bundle run seed_history_job -t dev
or pass --catalog/--schema/--landing_volume/--seed_rows directly.
"""
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession


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


def write_table(df, catalog, schema, landing_volume, table, label):
    path = f"/Volumes/{catalog}/{schema}/{landing_volume}/{table}"
    (df.write.mode("append").parquet(f"{path}/{label}"))
    print(f"  wrote {table:20} -> {path}/{label}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--landing_volume", default="landing")
    p.add_argument("--seed_rows", type=int, default=100_000_000)  # claim-detail rows
    p.add_argument("--code_root", default="")  # consumed at import time
    a = p.parse_args()

    n_headers = max(1, a.seed_rows // g.DETAIL_LINES_PER_CLAIM)
    print(f"Seeding ~{a.seed_rows:,} detail rows from {n_headers:,} claim headers "
          f"into {a.catalog}.{a.schema} (volume {a.landing_volume})")

    seq_base = 0  # seed occupies the lowest sequence block

    # --- dimensions (built once) ---
    write_table(g.gen_provider(spark, seq_base), a.catalog, a.schema, a.landing_volume, "provider", "seed")
    write_table(g.gen_member(spark, seq_base), a.catalog, a.schema, a.landing_volume, "member", "seed")
    write_table(g.gen_payer(spark, seq_base), a.catalog, a.schema, a.landing_volume, "payer", "seed")

    # --- claim family: a PAID/adjudicated historical population ---
    base = (
        spark.range(1, n_headers + 1)
        .withColumnRenamed("id", "n")
        .withColumn("status", g.F.lit("PAID"))
    )
    base = base.repartition(max(8, n_headers // 1_000_000))

    hot = 0.0  # seed is uniform; hot spots are introduced in increments
    write_table(g._cdc(g._claim_header(base, hot), "I", seq_base), a.catalog, a.schema, a.landing_volume, "claim", "seed")
    write_table(g._cdc(g._claim_details(base, hot), "I", seq_base), a.catalog, a.schema, a.landing_volume, "claim_detail", "seed")
    write_table(g._cdc(g._claim_attribute(base), "I", seq_base), a.catalog, a.schema, a.landing_volume, "claim_attribute", "seed")
    write_table(g._cdc(g._claim_diagnosis(base), "I", seq_base), a.catalog, a.schema, a.landing_volume, "claim_diagnosis", "seed")
    write_table(g._cdc(g._claim_payment(base, hot), "I", seq_base), a.catalog, a.schema, a.landing_volume, "claim_payment", "seed")
    write_table(g._cdc(g._claim_audit(base, "SUBMIT"), "I", seq_base), a.catalog, a.schema, a.landing_volume, "claim_audit", "seed")

    # --- control table: high-water claim number + next sequence base ---
    control = f"{a.catalog}.{a.schema}._demo_control"
    row = [(int(n_headers), int(1_000_000_000_000), 0, datetime.now(timezone.utc))]
    cols = ["max_claim_n", "seq_base", "run_index", "updated_at"]
    spark.createDataFrame(row, cols).write.mode("overwrite").saveAsTable(control)
    print(f"Control table {control} initialized: max_claim_n={n_headers:,}, next seq_base=1e12")
    print("Seed complete. Run the pipeline once to establish the historical baseline.")


if __name__ == "__main__":
    main()
