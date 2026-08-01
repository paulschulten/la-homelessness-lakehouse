# lh_orch/lh_assets/gold_pit.py

import sys
from pathlib import Path

import dagster as dg
import pandas as pd
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

from lh_orch.lh_assets.silver_pit import silver_pit_count, SILVER_PATH

ICEBERG_NAMESPACE = "gold"

CORE_COUNT_COLUMNS = [
    "totStreetSingAdult", "totStreetFamHH", "totStreetFamMem",
    "totSafeParkSingAdult", "totSafeParkFamHH", "totSafeParkFamMem",
    "totCars", "totVans", "totCampers", "totTents",
    "totESAdultSingAdult", "totESAdultFamHH", "totESAdultFamMem",
    "totESYouthSingYouth", "totESYouthFamHH", "totESYouthFamMem", "totESYouthUnaccYouth",
    "totTHAdultSingAdult", "totTHAdultFamHH", "totTHAdultFamMem",
    "totTHYouthSingYouth", "totTHYouthFamHH", "totTHYouthFamMem", "totTHYouthUnaccYouth",
    "totSHAdultSingAdult", "totSHAdultFamHH", "totSHAdultFamMem",
    "totSHYouthSingYouth", "totSHYouthFamHH", "totSHYouthFamMem", "totSHYouthUnaccYouth",
    "totESPeople", "totTHPeople", "totSHPeople", "totSheltPeople",
]

EXTENDED_COUNT_COLUMNS = [
    "ShelterCountAny", "StreetCountAny",
    "totEncamp",
    "totCarPeople", "totVanPeople", "totCamperPeople", "totTentPeople", "totEncampPeople",
    "FamCarHH", "FamVanHH", "FamCamperHH", "FamTentHH", "FamEncampHH",
    "FamCarPeople", "FamVanPeople", "FamCamperPeople", "FamTentPeople", "FamEncampPeople",
    "IndCarPeople", "IndVanPeople", "IndCamperPeople", "IndTentPeople", "IndEncampPeople",
    "totUnsheltPeople", "totPeople",
]

COUNT_COLUMNS = CORE_COUNT_COLUMNS + EXTENDED_COUNT_COLUMNS

FACT_SCHEMA = pa.schema(
    [("tract_fips", pa.string()), ("year", pa.int32())]
    + [(c, pa.int64()) for c in COUNT_COLUMNS]
)

DIM_GEOGRAPHY_SCHEMA = pa.schema([
    ("tract_fips", pa.string()),
    ("city", pa.string()),
    ("la_city", pa.string()),
    ("community_name", pa.string()),
    ("spa", pa.string()),
    ("sd", pa.string()),
    ("cd", pa.string()),
    ("ca_ssd", pa.string()),
    ("ca_sad", pa.string()),
    ("us_cd", pa.string()),
])


def _ensure_namespace(catalog):
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)


def _get_or_create_table(catalog, table_name, schema):
    identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=schema)
    return catalog.load_table(identifier)


@dg.asset(
    deps=[silver_pit_count],
    group_name="pit_count",
    description=(
        "Fact table of PIT Count sheltered/unsheltered counts by tract and year, 2020-2026 "
        "(2021 not conducted). Core category totals (street/ES/TH/SH counts) are populated "
        "for every year. Detailed dwelling-type breakdowns (e.g. totCarPeople, FamVanHH) and "
        "grand totals (totPeople, totUnsheltPeople) are only available for 2020 and 2022 - "
        "LAHSA simplified its public data files starting in 2023, so these columns are NULL "
        "for 2023 onward, not missing due to a pipeline error. Sub-tract split rows are "
        "aggregated (summed) to the standard tract level to match ACS's grain, so this table "
        "joins cleanly to gold.dim_tract and gold.fact_acs_estimates on tract_fips. "
        "Landed in gold.fact_pit_count."
    ),
)
def gold_fact_pit_count(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)

    present_count_cols = [c for c in COUNT_COLUMNS if c in df.columns]
    missing_count_cols = [c for c in COUNT_COLUMNS if c not in df.columns]

    fact_df = df[["tract_fips", "Year"] + present_count_cols].rename(columns={"Year": "year"})

    for col in missing_count_cols:
        fact_df[col] = None

    fact_df = fact_df.groupby(["tract_fips", "year"], as_index=False).sum(min_count=1)
    fact_df = fact_df[["tract_fips", "year"] + COUNT_COLUMNS]

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "fact_pit_count", FACT_SCHEMA)

    arrow_table = pa.Table.from_pandas(fact_df, schema=FACT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(fact_df)} rows to gold.fact_pit_count")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(fact_df))})


@dg.asset(
    deps=[silver_pit_count],
    group_name="pit_count",
    description=(
        "Dimension table of tract-level geography: city, SPA, council/legislative districts, "
        "2020-2026. Landed in gold.dim_geography."
    ),
)
def gold_dim_geography(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)

    geo_source_cols = ["tract_fips", "City", "LACity", "Community_Name", "SPA", "sd", "cd", "ca_ssd", "ca_sad", "us_cd"]
    available_cols = [c for c in geo_source_cols if c in df.columns]

    dim_df = (
        df[available_cols]
        .astype(str)
        .drop_duplicates(subset=["tract_fips"])
        .rename(columns={
            "City": "city",
            "LACity": "la_city",
            "Community_Name": "community_name",
            "SPA": "spa",
        })
        .reset_index(drop=True)
    )

    for col in ["city", "la_city", "community_name", "spa", "sd", "cd", "ca_ssd", "ca_sad", "us_cd"]:
        if col not in dim_df.columns:
            dim_df[col] = None

    dim_df = dim_df[["tract_fips", "city", "la_city", "community_name", "spa", "sd", "cd", "ca_ssd", "ca_sad", "us_cd"]]

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_geography", DIM_GEOGRAPHY_SCHEMA)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_GEOGRAPHY_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_geography")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})