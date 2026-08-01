# lh_orch/lh_assets/bronze_pit.py

import urllib.request
from pathlib import Path

import dagster as dg

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "02_data" / "01_raw" / "lahsa" / "01_pit_count"

SOURCE_URLS = {
    2020: "https://www.lahsa.org/item.ashx?id=4697-2020-homeless-count-data-by-census-tract.xlsx&dl=true",
    2022: "https://www.lahsa.org/item.ashx?id=6543-hc22-data-by-census-tract-split.xlsx&dl=true",
    2023: "https://www.lahsa.org/item.ashx?id=7722-hc23-data-by-census-subtract.xlsx&dl=true",
    2024: "https://www.lahsa.org/item.ashx?id=8299-hc24-data-by-census-subtract.xlsx&dl=true",
    2025: "https://www.lahsa.org/item.ashx?id=9559-hc25-data-by-census-subtract-revised-october-2025.xlsx&dl=true",
    2026: "https://www.lahsa.org/item.ashx?id=10119-hc-26-raw-data-by-census-sub-tract-final.xlsx&dl=true",
}


def raw_path_for_year(year: int) -> Path:
    return RAW_DIR / f"hc{year}_census_subtract_raw.xlsx"


@dg.asset(
    group_name="pit_count",
    description=(
        "Raw LAHSA Point-in-Time Count data by census tract/sub-tract, 2020-2026 "
        "(2021 not conducted), downloaded as-is from lahsa.org for each year."
    ),
)
def bronze_pit_count(context: dg.AssetExecutionContext):
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = {}
    for year, url in SOURCE_URLS.items():
        path = raw_path_for_year(year)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as response, open(path, "wb") as f:
            f.write(response.read())
        context.log.info(f"Downloaded {year} PIT count file to: {path}")
        downloaded[year] = str(path)

    return dg.MaterializeResult(
        metadata={f"path_{year}": dg.MetadataValue.path(p) for year, p in downloaded.items()}
    )
