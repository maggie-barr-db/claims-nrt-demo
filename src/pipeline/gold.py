"""Gold — the modeled layer (star / snowflake schema).

NOT a 1:1 copy of L0. These are materialized views that converge the
CDC-resolved silver current-state tables into conformed dimensions and facts:
joins, detail explosion, aggregation, and derived metrics.

  Dimensions: dim_provider, dim_member, dim_payer, dim_date,
              dim_procedure, dim_diagnosis  (last two snowflaked off the facts)
  Facts:      fact_claim_line   — line grain (explodes detail, joins header + dims)
              fact_claim        — header grain (aggregates lines, derives metrics)
              agg_claims_daily   — day x payer x provider rollup (KPIs)

Built as materialized views (batch `spark.read`) so they stay consistent on
each pipeline run; LDP incrementally refreshes where the query shape allows.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog_use")
schema = spark.conf.get("schema_use")


def s(name: str) -> str:
    """Fully-qualified silver (current-state) table name."""
    return f"{catalog}.{schema}.{name}"


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------
@dp.table(name="dim_provider", comment="Provider dimension.", cluster_by=["provider_id"])
def dim_provider():
    return spark.read.table(s("provider")).select(
        "provider_id", "npi", "provider_name", "specialty", "network_status", "state"
    )


@dp.table(name="dim_member", comment="Member dimension.", cluster_by=["member_id"])
def dim_member():
    return spark.read.table(s("member")).select(
        "member_id", "member_name", "date_of_birth", "gender",
        "plan_id", "enrollment_status", "state"
    )


@dp.table(name="dim_payer", comment="Payer dimension.")
def dim_payer():
    return spark.read.table(s("payer")).select("payer_id", "payer_name", "payer_type", "state")


@dp.table(name="dim_procedure", comment="Procedure code dimension (snowflaked from claim lines).")
def dim_procedure():
    return (
        spark.read.table(s("claim_detail"))
        .select("procedure_code")
        .where(F.col("procedure_code").isNotNull())
        .distinct()
    )


@dp.table(name="dim_diagnosis", comment="Diagnosis code dimension (snowflaked from claim diagnoses).")
def dim_diagnosis():
    return (
        spark.read.table(s("claim_diagnosis"))
        .select("diagnosis_code", "diagnosis_type")
        .where(F.col("diagnosis_code").isNotNull())
        .distinct()
    )


@dp.table(name="dim_date", comment="Calendar dimension.")
def dim_date():
    return (
        spark.sql(
            "SELECT explode(sequence(to_date('2023-01-01'), to_date('2027-12-31'), interval 1 day)) AS date_key"
        )
        .select(
            "date_key",
            F.year("date_key").alias("year"),
            F.month("date_key").alias("month"),
            F.dayofmonth("date_key").alias("day"),
            F.date_format("date_key", "E").alias("day_of_week"),
            F.weekofyear("date_key").alias("week_of_year"),
        )
    )


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------
@dp.table(
    name="fact_claim_line",
    comment="Line-grain claims fact — detail exploded and joined to header + conformed dim keys.",
    cluster_by=["claim_id"],
)
def fact_claim_line():
    detail = spark.read.table(s("claim_detail"))
    header = spark.read.table(s("claim")).select(
        "claim_id", "member_id", "payer_id", "claim_status", "service_from_date"
    )
    return detail.join(header, "claim_id", "left").select(
        F.col("claim_detail_id"),
        F.col("claim_id"),
        F.col("member_id"),
        F.col("provider_id"),                       # line-level rendering provider (from detail)
        F.col("payer_id"),
        F.col("service_from_date").alias("service_date_key"),
        F.col("procedure_code"),
        F.col("claim_status"),
        F.col("units"),
        F.col("charge_amount"),
        F.col("allowed_amount"),
        F.col("paid_amount"),
        F.col("adjustment_amount"),
    )


@dp.table(
    name="fact_claim",
    comment="Header-grain claims fact — line measures aggregated up, with derived metrics.",
    cluster_by=["claim_id"],
)
def fact_claim():
    # Header keeps its own total_charge_amount; line measures are summed from
    # detail and namespaced `line_*` to avoid colliding with header columns.
    header = spark.read.table(s("claim")).select(
        "claim_id", "member_id", "provider_id", "payer_id",
        "claim_status", "service_from_date", "total_charge_amount",
    )
    line_agg = spark.read.table(s("claim_detail")).groupBy("claim_id").agg(
        F.sum("paid_amount").alias("line_paid_amount"),
        F.sum("adjustment_amount").alias("line_adjustment_amount"),
        F.count("claim_detail_id").alias("line_count"),
    )
    return header.join(line_agg, "claim_id", "left").select(
        "claim_id",
        "member_id",
        "provider_id",
        "payer_id",
        F.col("service_from_date").alias("service_date_key"),
        "claim_status",
        "total_charge_amount",
        F.coalesce("line_paid_amount", F.lit(0)).cast("decimal(14,2)").alias("total_paid_amount"),
        F.coalesce("line_adjustment_amount", F.lit(0)).cast("decimal(14,2)").alias("total_adjustment_amount"),
        F.coalesce("line_count", F.lit(0)).alias("line_count"),
        F.round(
            F.coalesce("line_paid_amount", F.lit(0))
            / F.when(F.col("total_charge_amount") == 0, None).otherwise(F.col("total_charge_amount")),
            4,
        ).alias("paid_ratio"),
    )


# ---------------------------------------------------------------------------
# Incremental twin of fact_claim_line — for side-by-side comparison vs the MV.
#
# Same grain/schema as fact_claim_line, but built as a STREAMING table fed by
# the bronze claim_detail change log (append-only — already carries I/U/D +
# sequence), stream-static-joined to the claim header, then APPLY CHANGES keyed
# by claim_detail_id. Cost scales with the CHANGED lines, not the full table.
#
# Trade-off (by design): header attributes (e.g. claim_status) are captured at
# the time the line change is processed; a header-only change to an existing
# claim won't re-touch already-written lines. The MV always reflects current
# header state. Counts match closely; some attribute values can differ.
# ---------------------------------------------------------------------------
@dp.temporary_view()
def fact_claim_line_changes():
    detail = spark.readStream.table(s("claim_detail_bronze"))
    header = spark.read.table(s("claim")).select(
        "claim_id", "member_id", "payer_id", "claim_status", "service_from_date"
    )
    return detail.join(header, "claim_id", "left").select(
        "claim_detail_id",
        "claim_id",
        "member_id",
        "provider_id",
        "payer_id",
        F.col("service_from_date").alias("service_date_key"),
        "procedure_code",
        "claim_status",
        "units",
        "charge_amount",
        "allowed_amount",
        "paid_amount",
        "adjustment_amount",
        "header__change_oper",
        "header__change_seq",
    )


dp.create_streaming_table(
    name="fact_claim_line_streaming",
    comment="Incremental twin of fact_claim_line (streaming + APPLY CHANGES).",
    cluster_by=["claim_id"],
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
    },
)

dp.create_auto_cdc_flow(
    target="fact_claim_line_streaming",
    source="fact_claim_line_changes",
    keys=["claim_detail_id"],
    sequence_by=F.col("header__change_seq"),
    apply_as_deletes=F.expr("header__change_oper = 'D'"),
    except_column_list=["header__change_oper", "header__change_seq"],
    stored_as_scd_type="1",
)


@dp.table(
    name="agg_claims_daily",
    comment="Daily claims KPIs by payer and provider (metrics rollup).",
)
def agg_claims_daily():
    f = spark.read.table(s("fact_claim"))
    return f.groupBy("service_date_key", "payer_id", "provider_id").agg(
        F.count("claim_id").alias("claim_count"),
        F.sum("total_paid_amount").alias("total_paid"),
        F.sum("total_adjustment_amount").alias("total_adjustment"),
        F.round(F.avg("paid_ratio"), 4).alias("avg_paid_ratio"),
        F.round(F.sum(F.when(F.col("claim_status") == "REVERSED", 1).otherwise(0)) / F.count("claim_id"), 4).alias("reversal_rate"),
        F.round(F.sum(F.when(F.col("claim_status") == "ADJUSTED", 1).otherwise(0)) / F.count("claim_id"), 4).alias("adjustment_rate"),
    )
