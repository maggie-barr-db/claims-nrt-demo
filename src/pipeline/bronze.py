"""L0 bronze — Auto Loader ingestion of Qlik-style CDC parquet.

One append-only streaming table per source table. Each landed parquet file
carries typed business columns plus the uniform CDC metadata
(`header__change_oper` / `_seq` / `_ts`). Bronze keeps everything as an
immutable change log; the merge happens downstream in silver via AUTO CDC.

Metadata-driven: loops over TABLE_SPECS so adding a table is a config entry,
not new code — this is what makes the pattern scale to 150+ tables.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from table_specs import TABLE_SPECS, bronze_schema_ddl

catalog = spark.conf.get("catalog_use")
schema = spark.conf.get("schema_use")
landing_volume = spark.conf.get("landing_volume_use")
state_volume = spark.conf.get("pipeline_state_volume_use")

# Append-only bronze: change log, CDF on so downstream/debug can read changes.
_BRONZE_PROPERTIES = {
    "quality": "bronze",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
}


def _make_bronze(table: str, spec: dict):
    """Factory closes over `table` so the loop doesn't hit late-binding."""
    landing_path = f"/Volumes/{catalog}/{schema}/{landing_volume}/{table}"
    schema_location = f"/Volumes/{catalog}/{schema}/{state_volume}/autoloader/{table}"
    cluster_col = spec["cluster_by"][0]

    @dp.table(
        name=f"{table}_bronze",
        comment=f"Append-only CDC change log for `{table}` (Auto Loader, parquet).",
        table_properties=_BRONZE_PROPERTIES,
        cluster_by=[cluster_col],
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("rescuedDataColumn", "_rescued_data")
            .schema(bronze_schema_ddl(table))
            .load(landing_path)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    return _bronze


for _table, _spec in TABLE_SPECS.items():
    _make_bronze(_table, _spec)
