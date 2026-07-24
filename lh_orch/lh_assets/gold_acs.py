# lh_orch/lh_assets/gold_acs.py

import sys
from pathlib import Path

import dagster as dg
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

from lh_orch.lh_assets.silver_acs import silver_acs_estimates

ICEBERG_NAMESPACE = "gold"

DIM_VARIABLE_SCHEMA = pa.schema([
    ("variable", pa.string()),
    ("variable_label", pa.string()),
    ("table_id", pa.string()),
])

DIM_TRACT_SCHEMA = pa.schema([
    ("tract_fips", pa.string()),
    ("tract_name", pa.string()),
])

FACT_SCHEMA = pa.schema([
    ("tract_fips", pa.string()),
    ("variable", pa.string()),
    ("year", pa.int32()),
    ("estimate", pa.float64()),
    ("moe", pa.float64()),
    ("standard_error", pa.float64()),
    ("cv_pct", pa.float64()),
    ("reliability", pa.string()),
])


def _ensure_namespace(catalog):
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)


def _get_or_create_table(catalog, table_name, schema):
    identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=schema)
    return catalog.load_table(identifier)


def _load_silver_df():
    catalog = get_catalog()
    silver_table = catalog.load_table("silver.acs_estimates")
    return catalog, silver_table.scan().to_arrow().to_pandas()


@dg.asset(
    name="gold_dim_variable",
    deps=[silver_acs_estimates],
    description=(
        "Dimension table of distinct ACS variables: variable code, human-readable "
        "label, and the B-table it belongs to. Landed in gold.dim_variable."
    ),
)
def gold_dim_variable(context: dg.AssetExecutionContext):
    catalog, df = _load_silver_df()

    dim_df = (
        df[["variable", "variable_label", "table_id"]]
        .drop_duplicates(subset=["variable"])
        .reset_index(drop=True)
    )

    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_variable", DIM_VARIABLE_SCHEMA)
    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_VARIABLE_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_variable")
    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})


@dg.asset(
    name="gold_dim_tract",
    deps=[silver_acs_estimates],
    description=(
        "Dimension table of distinct LA County census tracts: FIPS code and "
        "human-readable name. Landed in gold.dim_tract."
    ),
)
def gold_dim_tract(context: dg.AssetExecutionContext):
    catalog, df = _load_silver_df()

    dim_df = (
        df[["tract_fips", "tract_name"]]
        .drop_duplicates(subset=["tract_fips"])
        .reset_index(drop=True)
    )

    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_tract", DIM_TRACT_SCHEMA)
    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_TRACT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_tract")
    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})


@dg.asset(
    name="gold_fact_acs_estimates",
    deps=[silver_acs_estimates],
    description=(
        "Fact table of ACS estimates at the tract/variable/year grain: estimate, "
        "MOE, standard error, coefficient of variation, and reliability flag. "
        "Keeps genuine zero estimates; drops rows where the estimate itself "
        "couldn't be parsed from the Census API response. No reliability "
        "filtering - clients decide what to include via the reliability column. "
        "Landed in gold.fact_acs_estimates; join to gold.dim_variable and "
        "gold.dim_tract for labels."
    ),
)
def gold_fact_acs_estimates(context: dg.AssetExecutionContext):
    catalog, df = _load_silver_df()

    before = len(df)
    fact_df = df[df["estimate"].notna()].copy()
    dropped = before - len(fact_df)
    context.log.info(f"Dropped {dropped} rows with unparseable estimates; kept {len(fact_df)}")

    fact_df = fact_df[[
        "tract_fips", "variable", "year", "estimate", "moe",
        "standard_error", "cv_pct", "reliability",
    ]].reset_index(drop=True)

    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "fact_acs_estimates", FACT_SCHEMA)
    arrow_table = pa.Table.from_pandas(fact_df, schema=FACT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    reliability_counts = fact_df["reliability"].value_counts().to_dict()
    context.log.info(f"Wrote {len(fact_df)} rows to gold.fact_acs_estimates. Reliability: {reliability_counts}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(fact_df)),
            "dropped_unparseable": dg.MetadataValue.int(dropped),
            "reliable_count": dg.MetadataValue.int(int(reliability_counts.get("reliable", 0))),
            "use_with_caution_count": dg.MetadataValue.int(int(reliability_counts.get("use with caution", 0))),
            "not_reliable_count": dg.MetadataValue.int(int(reliability_counts.get("not reliable", 0))),
            "not_applicable_count": dg.MetadataValue.int(int(reliability_counts.get("not applicable", 0))),
        }
    )