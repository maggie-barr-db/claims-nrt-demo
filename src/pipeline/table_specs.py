"""Single source of truth for the demo's data model.

Both the synthetic CDC generator and the Lakeflow Declarative Pipeline import
this module, so the bronze schema, the APPLY CHANGES keys, and the generator
output always agree.

Every source table carries three uniform Qlik-style CDC metadata columns in
addition to its business columns:

    header__change_oper   STRING     -- 'I' | 'U' | 'D'
    header__change_seq    BIGINT     -- monotonic; updates out-sequence inserts
    header__change_ts     TIMESTAMP  -- wall-clock of the change

`sequence_by` orders changes per key for APPLY CHANGES; `apply_as_deletes`
turns 'D' rows into deletes; the metadata columns are dropped from the silver
target via `except_column_list`.
"""

# Uniform CDC metadata present on every landed parquet row.
CDC_METADATA_COLUMNS = [
    ("header__change_oper", "STRING"),
    ("header__change_seq", "BIGINT"),
    ("header__change_ts", "TIMESTAMP"),
]

SEQUENCE_BY = "header__change_seq"
DELETE_PREDICATE = "header__change_oper = 'D'"
DROP_FROM_TARGET = ["header__change_oper", "header__change_seq", "header__change_ts"]

# category: "transactional" (high volume, claim-keyed) | "dimension" (low volume reference)
TABLE_SPECS = {
    # ---- header ---------------------------------------------------------
    "claim": {
        "category": "transactional",
        "pkeys": ["claim_id"],
        "cluster_by": ["claim_id"],
        "scd_type": 1,
        "columns": [
            ("claim_id", "STRING"),
            ("member_id", "STRING"),
            ("provider_id", "STRING"),
            ("payer_id", "STRING"),
            ("claim_status", "STRING"),      # SUBMITTED|PAID|ADJUSTED|REVERSED|DENIED
            ("claim_type", "STRING"),        # PROFESSIONAL|INSTITUTIONAL|DENTAL|RX
            ("total_charge_amount", "DECIMAL(12,2)"),
            ("total_paid_amount", "DECIMAL(12,2)"),
            ("service_from_date", "DATE"),
            ("service_to_date", "DATE"),
            ("submitted_date", "TIMESTAMP"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {
            "warn": {"claim_total_charge_nonneg": "total_charge_amount >= 0"},
            "drop": {"claim_id_not_null": "claim_id IS NOT NULL"},
        },
    },
    # ---- detail ---------------------------------------------------------
    "claim_detail": {
        "category": "transactional",
        "pkeys": ["claim_detail_id"],
        "cluster_by": ["claim_id"],
        "scd_type": 1,
        "columns": [
            ("claim_detail_id", "STRING"),
            ("claim_id", "STRING"),
            ("line_number", "INT"),
            ("procedure_code", "STRING"),
            ("revenue_code", "STRING"),
            ("place_of_service", "STRING"),
            ("provider_id", "STRING"),
            ("units", "INT"),
            ("charge_amount", "DECIMAL(12,2)"),
            ("allowed_amount", "DECIMAL(12,2)"),
            ("paid_amount", "DECIMAL(12,2)"),
            ("adjustment_amount", "DECIMAL(12,2)"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {
            "warn": {"detail_paid_nonneg": "paid_amount >= 0"},
            "drop": {"detail_id_not_null": "claim_detail_id IS NOT NULL"},
        },
    },
    # ---- claim attributes (claimattribute / qattribute / attributegroup) -
    "claim_attribute": {
        "category": "transactional",
        "pkeys": ["claim_attribute_id"],
        "cluster_by": ["claim_id"],
        "scd_type": 1,
        "columns": [
            ("claim_attribute_id", "STRING"),
            ("claim_id", "STRING"),
            ("attribute_group", "STRING"),
            ("attribute_name", "STRING"),
            ("attribute_value", "STRING"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {"drop": {"attr_id_not_null": "claim_attribute_id IS NOT NULL"}},
    },
    # ---- audit trail ----------------------------------------------------
    "claim_audit": {
        "category": "transactional",
        "pkeys": ["claim_audit_id"],
        "cluster_by": ["claim_id"],
        "scd_type": 1,
        "columns": [
            ("claim_audit_id", "STRING"),
            ("claim_id", "STRING"),
            ("event_type", "STRING"),        # SUBMIT|ADJUDICATE|ADJUST|REVERSE|DENY
            ("event_status", "STRING"),
            ("event_user", "STRING"),
            ("event_timestamp", "TIMESTAMP"),
            ("note", "STRING"),
        ],
        "expectations": {"drop": {"audit_id_not_null": "claim_audit_id IS NOT NULL"}},
    },
    # ---- payments (claimpay) -------------------------------------------
    "claim_payment": {
        "category": "transactional",
        "pkeys": ["payment_id"],
        "cluster_by": ["claim_id"],
        "scd_type": 1,
        "columns": [
            ("payment_id", "STRING"),
            ("claim_id", "STRING"),
            ("payer_id", "STRING"),
            ("payment_method", "STRING"),    # CHECK|EFT|CARD|SYSTEM
            ("payment_amount", "DECIMAL(12,2)"),
            ("payment_date", "DATE"),
            ("check_number", "STRING"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {"drop": {"payment_id_not_null": "payment_id IS NOT NULL"}},
    },
    # ---- diagnoses ------------------------------------------------------
    "claim_diagnosis": {
        "category": "transactional",
        "pkeys": ["claim_diagnosis_id"],
        "cluster_by": ["claim_id"],
        "scd_type": 1,
        "columns": [
            ("claim_diagnosis_id", "STRING"),
            ("claim_id", "STRING"),
            ("diagnosis_seq", "INT"),
            ("diagnosis_code", "STRING"),    # ICD-10-ish
            ("diagnosis_type", "STRING"),    # PRINCIPAL|SECONDARY
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {"drop": {"dx_id_not_null": "claim_diagnosis_id IS NOT NULL"}},
    },
    # ---- dimensions -----------------------------------------------------
    "provider": {
        "category": "dimension",
        "pkeys": ["provider_id"],
        "cluster_by": ["provider_id"],
        "scd_type": 1,
        "columns": [
            ("provider_id", "STRING"),
            ("npi", "STRING"),
            ("provider_name", "STRING"),
            ("specialty", "STRING"),
            ("network_status", "STRING"),    # IN_NETWORK|OUT_OF_NETWORK
            ("state", "STRING"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {"drop": {"provider_id_not_null": "provider_id IS NOT NULL"}},
    },
    "member": {
        "category": "dimension",
        "pkeys": ["member_id"],
        "cluster_by": ["member_id"],
        "scd_type": 1,
        "columns": [
            ("member_id", "STRING"),
            ("member_name", "STRING"),
            ("date_of_birth", "DATE"),
            ("gender", "STRING"),
            ("plan_id", "STRING"),
            ("enrollment_status", "STRING"),
            ("state", "STRING"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {"drop": {"member_id_not_null": "member_id IS NOT NULL"}},
    },
    "payer": {
        "category": "dimension",
        "pkeys": ["payer_id"],
        "cluster_by": ["payer_id"],
        "scd_type": 1,
        "columns": [
            ("payer_id", "STRING"),
            ("payer_name", "STRING"),
            ("payer_type", "STRING"),        # MEDICAID|MEDICARE|COMMERCIAL
            ("state", "STRING"),
            ("last_updated", "TIMESTAMP"),
        ],
        "expectations": {"drop": {"payer_id_not_null": "payer_id IS NOT NULL"}},
    },
}


def business_columns(table: str):
    """Business column (name, type) tuples for a table (no CDC metadata)."""
    return TABLE_SPECS[table]["columns"]


def all_columns(table: str):
    """Business + CDC-metadata columns, in landed-parquet order."""
    return TABLE_SPECS[table]["columns"] + CDC_METADATA_COLUMNS


def bronze_schema_ddl(table: str) -> str:
    """Comma-separated `name type` DDL string for the landed parquet schema."""
    return ", ".join(f"{name} {sql_type}" for name, sql_type in all_columns(table))
