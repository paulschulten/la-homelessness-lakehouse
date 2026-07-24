# lh_orch/lh_assets/silver_acs.py

import sys
from pathlib import Path

import dagster as dg
import numpy as np
import pandas as pd
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

from lh_orch.lh_assets.bronze_acs import bronze_acs_estimates

ICEBERG_NAMESPACE = "silver"
ICEBERG_TABLE_NAME = "acs_estimates"

# Standard ACS reliability convention: MOEs are published at 90% confidence,
# so divide by 1.645 to get the standard error, then express as a
# coefficient of variation (SE / estimate). Thresholds below follow the
# commonly used convention from ACS methodology guides:
#   CV <= 12%         -> reliable
#   12% < CV <= 40%    -> use with caution
#   CV > 40%           -> not reliable
# Zero or unparseable estimates have an undefined CV -> "not applicable".
MOE_TO_SE_FACTOR = 1.645
CV_RELIABLE_THRESHOLD = 12.0
CV_CAUTION_THRESHOLD = 40.0

SILVER_SCHEMA = pa.schema([
    ("table_id", pa.string()),
    ("variable", pa.string()),
    ("variable_label", pa.string()),
    ("tract_fips", pa.string()),
    ("tract_name", pa.string()),
    ("year", pa.int32()),
    ("estimate", pa.float64()),
    ("moe", pa.float64()),
    ("standard_error", pa.float64()),
    ("cv_pct", pa.float64()),
    ("reliability", pa.string()),
    ("processed_at", pa.string()),
])


@dg.asset(
    name="silver_acs_estimates",
    deps=[bronze_acs_estimates],
    description=(
        "ACS estimates with margin of error converted to standard error and "
        "coefficient of variation (CV), plus a reliability flag (reliable / "
        "use with caution / not reliable / not applicable) per the standard "
        "ACS CV convention. Landed in the Iceberg silver table "
        "(silver.acs_estimates)."
    ),
)
def silver_acs_estimates(context: dg.AssetExecutionContext):
    catalog = get_catalog()
    bronze_table = catalog.load_table("bronze.acs_estimates")

    df = bronze_table.scan().to_arrow().to_pandas()
    context.log.info(f"Loaded {len(df)} rows from bronze.acs_estimates")

    df["estimate"] = pd.to_numeric(df["estimate"], errors="coerce")
    df["moe"] = pd.to_numeric(df["moe"], errors="coerce")

    df["standard_error"] = df["moe"] / MOE_TO_SE_FACTOR

    df["cv_pct"] = np.nan
    nonzero = df["estimate"] != 0
    df.loc[nonzero, "cv_pct"] = (
        (df.loc[nonzero, "standard_error"] / df.loc[nonzero, "estimate"]).abs() * 100
    )

    conditions = [
        df["cv_pct"].isna(),
        df["cv_pct"] <= CV_RELIABLE_THRESHOLD,
        df["cv_pct"] <= CV_CAUTION_THRESHOLD,
    ]
    choices = ["not applicable", "reliable", "use with caution"]
    df["reliability"] = np.select(conditions, choices, default="not reliable")

    df["processed_at"] = pd.Timestamp.utcnow().isoformat()

    df = df[[
        "table_id", "variable", "variable_label", "tract_fips", "tract_name",
        "year", "estimate", "moe", "standard_error", "cv_pct", "reliability",
        "processed_at",
    ]]

    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)
    identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"

    if not catalog.table_exists(identifier):
        silver_table = catalog.create_table(identifier, schema=SILVER_SCHEMA)
    else:
        silver_table = catalog.load_table(identifier)

    arrow_table = pa.Table.from_pandas(df, schema=SILVER_SCHEMA, preserve_index=False)

    # Full recompute off bronze each run, not incremental -> overwrite
    # rather than append, so re-running doesn't create duplicate rows.
    silver_table.overwrite(arrow_table)

    reliability_counts = df["reliability"].value_counts().to_dict()
    context.log.info(f"Wrote {len(df)} rows to silver.acs_estimates. Reliability: {reliability_counts}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "reliable_count": dg.MetadataValue.int(int(reliability_counts.get("reliable", 0))),
            "use_with_caution_count": dg.MetadataValue.int(int(reliability_counts.get("use with caution", 0))),
            "not_reliable_count": dg.MetadataValue.int(int(reliability_counts.get("not reliable", 0))),
            "not_applicable_count": dg.MetadataValue.int(int(reliability_counts.get("not applicable", 0))),
        }
    )