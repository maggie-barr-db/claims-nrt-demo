"""L1 silver — AUTO CDC / APPLY CHANGES into SCD Type 1 tables.

For each source table: create the target streaming table (liquid-clustered on
the claim key, with data-quality expectations) and an AUTO CDC flow that merges
the bronze change log by primary key, ordered by `header__change_seq`, turning
`header__change_oper = 'D'` rows into deletes.

The merge keys on the high-cardinality primary key (claim_id / *_id), NOT on
provider_id — so a single provider responsible for a large share of an
increment does not create merge skew. This is the behavior the demo proves.

Each table's AUTO CDC flow is an independent flow in the pipeline DAG, so the
9 tables advance in parallel — the "150-table win".
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from table_specs import (
    TABLE_SPECS,
    SEQUENCE_BY,
    DELETE_PREDICATE,
    DROP_FROM_TARGET,
)

# Columns added in bronze that should not land in the curated silver target.
_BRONZE_ONLY_COLUMNS = ["_ingested_at", "_source_file", "_rescued_data"]

_SILVER_PROPERTIES = {
    "quality": "silver",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
}


def _make_silver(table: str, spec: dict):
    target = table
    source = f"{table}_bronze"

    # Expectations: warn = observe, drop = quarantine bad rows out of the target.
    expect_warn = spec.get("expectations", {}).get("warn", {})
    expect_drop = spec.get("expectations", {}).get("drop", {})

    dp.create_streaming_table(
        name=target,
        comment=f"Silver SCD-1 current state for `{table}` (AUTO CDC from {source}).",
        table_properties=_SILVER_PROPERTIES,
        cluster_by=spec["cluster_by"],
        expect_all=expect_warn or None,
        expect_all_or_drop=expect_drop or None,
    )

    dp.create_auto_cdc_flow(
        target=target,
        source=source,
        keys=spec["pkeys"],
        sequence_by=F.col(SEQUENCE_BY),
        apply_as_deletes=F.expr(DELETE_PREDICATE),
        except_column_list=DROP_FROM_TARGET + _BRONZE_ONLY_COLUMNS,
        stored_as_scd_type="1",
    )


for _table, _spec in TABLE_SPECS.items():
    _make_silver(_table, _spec)
