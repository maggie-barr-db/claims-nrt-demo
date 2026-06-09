"""One-time UC setup — create the catalog (if permitted), schema, and volumes.

Run this ONCE per target workspace after `bundle deploy`, before seeding or
running the pipeline. It's idempotent (CREATE ... IF NOT EXISTS), so re-running
is safe.

The catalog name is a parameter, so the same bundle deploys anywhere:
  * If you have CREATE CATALOG on the metastore, it's created for you.
  * If you don't (e.g. a managed FE workspace), the catalog must already exist —
    pass its name and this script just creates the schema + volumes inside it.

    databricks bundle run setup_job -t dev
    # or override the catalog for another workspace:
    databricks bundle run setup_job -t dev -- --catalog=some_existing_catalog
"""
import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--landing_volume", default="landing")
    p.add_argument("--state_volume", default="pipeline_state")
    a = p.parse_args()

    # Catalog — best effort. No-op if it exists; clear message if we lack perms.
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {a.catalog}")
        print(f"catalog OK: {a.catalog}")
    except Exception as e:  # noqa: BLE001
        print(f"NOTE: could not create catalog `{a.catalog}` ({type(e).__name__}). "
              f"It must already exist in this workspace — continuing with schema/volumes.")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.{a.schema}")
    print(f"schema OK: {a.catalog}.{a.schema}")

    for vol in (a.landing_volume, a.state_volume):
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {a.catalog}.{a.schema}.{vol}")
        print(f"volume OK: {a.catalog}.{a.schema}.{vol}")

    print("Setup complete. Next: run seed_history_job, then the pipeline.")


if __name__ == "__main__":
    main()
