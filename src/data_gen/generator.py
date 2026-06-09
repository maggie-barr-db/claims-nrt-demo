"""Synthetic Qlik-style CDC generator (PySpark, scales on serverless).

Produces parquet "change batches" that look like what Qlik Replicate lands from
a SQL Server source: typed business columns plus CDC metadata
(`header__change_oper` I/U/D, `header__change_seq`, `header__change_ts`), one
sub-directory per source table under the landing volume.

Two entry points use this module:
  * 00_seed_history.py     — one large all-INSERT baseline (historical depth).
  * 01_generate_increment.py — one ~2hr SLA window of mixed changes, with an
                               optional single-provider hot spot (skew demo).

Design notes
------------
* All generation is set-based Spark (range + rand), so 100M+ rows are cheap.
* A small Delta control table (`_demo_control`) tracks the high-water claim
  number and a per-run sequence base, so increments allocate fresh keys and
  always out-sequence earlier runs.
* The merge key is the high-cardinality primary key; `provider_id` is just an
  attribute. Concentrating changes on one provider therefore does NOT skew
  AUTO CDC — which is exactly what the skew increment is built to show.
"""
from pyspark.sql import functions as F

HOT_PROVIDER_ID = "PRV000001"  # the deliberate hot spot for the skew demo

N_PROVIDERS = 5000
N_MEMBERS = 2_000_000
N_PAYERS = 25

DETAIL_LINES_PER_CLAIM = 5

CLAIM_STATUSES = ["SUBMITTED", "PAID", "ADJUSTED", "REVERSED", "DENIED"]
CLAIM_TYPES = ["PROFESSIONAL", "INSTITUTIONAL", "DENTAL", "RX"]
PROC_CODES = ["99213", "99214", "93000", "80053", "70450", "29881", "12001", "J3490"]
DX_CODES = ["E11.9", "I10", "J45.909", "M54.5", "Z00.00", "K21.9", "F41.1", "N39.0"]
PAYER_TYPES = ["MEDICAID", "MEDICARE", "COMMERCIAL"]

# Increment change-type mix (must sum to 1.0).
DEFAULT_CHANGE_MIX = {
    "new": 0.20,          # header I + detail I + children
    "header_only": 0.30,  # header U only (no detail) — proves independent propagation
    "adjustment": 0.35,   # header U + detail U (the 24hr auto-adjust case)
    "reversal": 0.15,     # header U (REVERSED) + detail D
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _pick(rand_col, options):
    """Pick from `options` by a [0,1) random column."""
    n = len(options)
    idx = (rand_col * n).cast("int")
    expr = F.when(idx == 0, F.lit(options[0]))
    for i in range(1, n):
        expr = expr.when(idx == i, F.lit(options[i]))
    return expr.otherwise(F.lit(options[-1]))


def _provider_col(seed, hot_fraction):
    """Assign provider_id; a `hot_fraction` share goes to the single hot provider."""
    rnd = F.rand(seed)
    # +1 so ids land in 1..N_PROVIDERS, matching gen_provider's range(1, N+1);
    # without it this produced PRV000000 (no such provider) and never PRV<N>.
    random_provider = F.format_string("PRV%06d", (F.rand(seed + 7) * N_PROVIDERS + 1).cast("int"))
    if hot_fraction <= 0:
        return random_provider
    return F.when(rnd < F.lit(hot_fraction), F.lit(HOT_PROVIDER_ID)).otherwise(random_provider)


def _cdc(df, oper, seq_base):
    """Attach CDC metadata. `seq_base` is the run index (0 = seed, then 1, 2, …
    per increment) and is used directly as `header__change_seq`: every row in a
    run shares one strictly-increasing sequence. This is correct because each
    primary key changes at most once per table per run (the new/header-only/
    adjustment/reversal buckets are disjoint, and new claims get fresh ids), so
    per-key ordering is fully determined by run order.

    Do NOT reintroduce `monotonically_increasing_id()` here: it encodes the
    partition id in the high bits (value ~ partition_id * 2**33), which for a
    high-partition frame can exceed the gap between runs and invert CDC ordering
    across runs — silently dropping later updates/deletes."""
    return (
        df.withColumn("header__change_oper", F.lit(oper))
        .withColumn("header__change_seq", F.lit(seq_base).cast("long"))
        .withColumn("header__change_ts", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# dimensions (built once at seed time)
# ---------------------------------------------------------------------------
def gen_provider(spark, seq_base):
    df = spark.range(1, N_PROVIDERS + 1).select(
        F.format_string("PRV%06d", F.col("id")).alias("provider_id"),
        F.format_string("%010d", (F.rand(1) * 9_999_999_999).cast("long")).alias("npi"),
        F.concat(F.lit("Provider "), F.col("id").cast("string")).alias("provider_name"),
        _pick(F.rand(2), ["FAMILY_MED", "CARDIOLOGY", "ORTHO", "RADIOLOGY", "INTERNAL_MED"]).alias("specialty"),
        _pick(F.rand(3), ["IN_NETWORK", "OUT_OF_NETWORK"]).alias("network_status"),
        _pick(F.rand(4), ["CA", "TX", "FL", "WA", "OH", "MI"]).alias("state"),
        F.current_timestamp().alias("last_updated"),
    )
    return _cdc(df, "I", seq_base)


def gen_member(spark, seq_base):
    df = spark.range(1, N_MEMBERS + 1).select(
        F.format_string("MBR%09d", F.col("id")).alias("member_id"),
        F.concat(F.lit("Member "), F.col("id").cast("string")).alias("member_name"),
        F.date_sub(F.current_date(), (F.rand(5) * 30000 + 6500).cast("int")).alias("date_of_birth"),
        _pick(F.rand(6), ["M", "F", "U"]).alias("gender"),
        F.format_string("PLAN%03d", (F.rand(7) * 50).cast("int")).alias("plan_id"),
        _pick(F.rand(8), ["ACTIVE", "TERMED", "PENDING"]).alias("enrollment_status"),
        _pick(F.rand(9), ["CA", "TX", "FL", "WA", "OH", "MI"]).alias("state"),
        F.current_timestamp().alias("last_updated"),
    )
    return _cdc(df, "I", seq_base)


def gen_payer(spark, seq_base):
    df = spark.range(1, N_PAYERS + 1).select(
        F.format_string("PAY%05d", F.col("id")).alias("payer_id"),
        F.concat(F.lit("Payer "), F.col("id").cast("string")).alias("payer_name"),
        _pick(F.rand(10), PAYER_TYPES).alias("payer_type"),
        _pick(F.rand(11), ["CA", "TX", "FL", "WA", "OH", "MI"]).alias("state"),
        F.current_timestamp().alias("last_updated"),
    )
    return _cdc(df, "I", seq_base)


# ---------------------------------------------------------------------------
# claim family — built from a base df of claim "numbers" (n) + assigned status
# ---------------------------------------------------------------------------
def _claim_header(base, hot_fraction):
    """base: df with columns n (long), status (string)."""
    return base.select(
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        F.format_string("MBR%09d", (F.rand(20) * N_MEMBERS + 1).cast("long")).alias("member_id"),
        _provider_col(21, hot_fraction).alias("provider_id"),
        F.format_string("PAY%05d", (F.rand(22) * N_PAYERS + 1).cast("long")).alias("payer_id"),
        F.col("status").alias("claim_status"),
        _pick(F.rand(23), CLAIM_TYPES).alias("claim_type"),
        F.round(F.rand(24) * 5000 + 50, 2).cast("decimal(12,2)").alias("total_charge_amount"),
        F.round(F.rand(25) * 4000, 2).cast("decimal(12,2)").alias("total_paid_amount"),
        F.date_sub(F.current_date(), (F.rand(26) * 60).cast("int")).alias("service_from_date"),
        F.date_sub(F.current_date(), (F.rand(27) * 30).cast("int")).alias("service_to_date"),
        F.current_timestamp().alias("submitted_date"),
        F.current_timestamp().alias("last_updated"),
    )


def _claim_details(base, hot_fraction, lines=DETAIL_LINES_PER_CLAIM):
    """Explode each claim into `lines` detail rows."""
    line_df = base.crossJoin(
        spark_active().range(1, lines + 1).withColumnRenamed("id", "line_number")
    )
    return line_df.select(
        F.format_string("CLM%012d-%02d", F.col("n"), F.col("line_number")).alias("claim_detail_id"),
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        F.col("line_number").cast("int").alias("line_number"),
        _pick(F.rand(30), PROC_CODES).alias("procedure_code"),
        F.format_string("%04d", (F.rand(31) * 1000).cast("int")).alias("revenue_code"),
        _pick(F.rand(32), ["11", "21", "22", "23", "81"]).alias("place_of_service"),
        _provider_col(33, hot_fraction).alias("provider_id"),
        (F.rand(34) * 4 + 1).cast("int").alias("units"),
        F.round(F.rand(35) * 1000 + 10, 2).cast("decimal(12,2)").alias("charge_amount"),
        F.round(F.rand(36) * 800, 2).cast("decimal(12,2)").alias("allowed_amount"),
        F.round(F.rand(37) * 700, 2).cast("decimal(12,2)").alias("paid_amount"),
        F.round((F.rand(38) - 0.5) * 200, 2).cast("decimal(12,2)").alias("adjustment_amount"),
        F.current_timestamp().alias("last_updated"),
    )


def _claim_detail_keys_only(base, lines=DETAIL_LINES_PER_CLAIM):
    """For reversals: just the detail PKs so AUTO CDC can delete by key."""
    line_df = base.crossJoin(
        spark_active().range(1, lines + 1).withColumnRenamed("id", "line_number")
    )
    null_cols = [
        ("line_number", "int"), ("procedure_code", "string"), ("revenue_code", "string"),
        ("place_of_service", "string"), ("provider_id", "string"), ("units", "int"),
        ("charge_amount", "decimal(12,2)"), ("allowed_amount", "decimal(12,2)"),
        ("paid_amount", "decimal(12,2)"), ("adjustment_amount", "decimal(12,2)"),
        ("last_updated", "timestamp"),
    ]
    out = line_df.select(
        F.format_string("CLM%012d-%02d", F.col("n"), F.col("line_number")).alias("claim_detail_id"),
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        *[F.lit(None).cast(t).alias(c) for c, t in null_cols],
    )
    return out


def _claim_audit(base, event_type, run_id):
    return base.select(
        # Deterministic + globally unique per (run, claim): one audit event per
        # claim per run, distinct across runs (append-style trail). A random id
        # collided (~n^2/2N) and APPLY CHANGES then silently dropped the dupes.
        F.format_string("AUD%04d%012d", F.lit(run_id), F.col("n")).alias("claim_audit_id"),
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        F.lit(event_type).alias("event_type"),
        _pick(F.rand(41), ["OK", "PENDED", "REJECTED"]).alias("event_status"),
        _pick(F.rand(42), ["svc_account", "adjuster_a", "adjuster_b", "auto_adjud"]).alias("event_user"),
        F.current_timestamp().alias("event_timestamp"),
        F.concat(F.lit(event_type), F.lit(" event")).alias("note"),
    )


def _claim_payment(base, hot_fraction, run_id):
    return base.select(
        # Deterministic + unique per (run, claim) — see _claim_audit note.
        F.format_string("PMT%04d%012d", F.lit(run_id), F.col("n")).alias("payment_id"),
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        F.format_string("PAY%05d", (F.rand(44) * N_PAYERS + 1).cast("long")).alias("payer_id"),
        _pick(F.rand(45), ["CHECK", "EFT", "CARD", "SYSTEM"]).alias("payment_method"),
        F.round(F.rand(46) * 3000, 2).cast("decimal(12,2)").alias("payment_amount"),
        F.current_date().alias("payment_date"),
        F.format_string("CHK%09d", (F.rand(47) * 1e9).cast("long")).alias("check_number"),
        F.current_timestamp().alias("last_updated"),
    )


def _claim_diagnosis(base):
    dx = base.crossJoin(
        spark_active().range(1, 3).withColumnRenamed("id", "diagnosis_seq")
    )
    return dx.select(
        F.format_string("DX%012d-%d", F.col("n"), F.col("diagnosis_seq")).alias("claim_diagnosis_id"),
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        F.col("diagnosis_seq").cast("int").alias("diagnosis_seq"),
        _pick(F.rand(48), DX_CODES).alias("diagnosis_code"),
        F.when(F.col("diagnosis_seq") == 1, F.lit("PRINCIPAL")).otherwise(F.lit("SECONDARY")).alias("diagnosis_type"),
        F.current_timestamp().alias("last_updated"),
    )


def _claim_attribute(base):
    at = base.crossJoin(
        spark_active().range(1, 4).withColumnRenamed("id", "attr_seq")
    )
    return at.select(
        F.format_string("ATR%012d-%d", F.col("n"), F.col("attr_seq")).alias("claim_attribute_id"),
        F.format_string("CLM%012d", F.col("n")).alias("claim_id"),
        _pick(F.rand(49), ["BILLING", "ELIGIBILITY", "AUTH"]).alias("attribute_group"),
        _pick(F.rand(50), ["priority", "channel", "auth_required", "cob_flag"]).alias("attribute_name"),
        _pick(F.rand(51), ["Y", "N", "HIGH", "LOW", "EDI", "PORTAL"]).alias("attribute_value"),
        F.current_timestamp().alias("last_updated"),
    )


# active SparkSession accessor (so module functions can build small range DFs)
def spark_active():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession()
