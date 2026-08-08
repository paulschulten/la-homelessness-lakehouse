# lh_orch/lh_assets/bronze_hic.py

from pathlib import Path

import dagster as dg
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "02_data" / "01_raw" / "lahsa" / "02_hic"

# Confirmed direct-download URLs (item.ashx?...&dl=true pattern — the
# "documents?id=..." links on LAHSA's HIC page are landing pages, not files).
# 2026 not yet published as of this writing (submission period opened Feb 2026);
# add it here once available. 2020 is the earliest year in the project's 2020+ scope.
HIC_URLS = {
    # 2020: "https://www.lahsa.org/item.ashx?id=4659-2020-housing-inventory-count.xlsx&dl=true",
      2021: "https://www.lahsa.org/item.ashx?id=5506-2021-housing-inventory-count.xlsx&dl=true",
      2022: "https://www.lahsa.org/item.ashx?id=6544-2022-housing-inventory-count.xlsx&dl=true",
      2023: "https://www.lahsa.org/item.ashx?id=7698-2023-housing-inventory-count.xlsx&dl=true",
      2024: "https://www.lahsa.org/item.ashx?id=8162-2024-housing-inventory-count.xlsx&dl=true",
      2025: "https://www.lahsa.org/item.ashx?id=9369-housing-inventory-count-hic-.xlsx&dl=true",
}


def raw_path_for_year(year: int) -> Path:
    return BRONZE_DIR / f"hic_{year}.xlsx"


@dg.asset(
    group_name="hic",
    description=(
        "Raw LAHSA Housing Inventory Count (HIC) files downloaded as-is per year. "
        "Project/site-level shelter and housing bed/unit inventory — geography is "
        "City/SPA/CD/SD, not Census tract (no address field in source)."
    ),
)
def bronze_hic(context: dg.AssetExecutionContext):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = []

    for year, url in HIC_URLS.items():
        out_path = raw_path_for_year(year)
        if out_path.exists():
            context.log.info(f"{year}: already present at {out_path}, skipping download")
            downloaded.append(year)
            continue

        context.log.info(f"{year}: downloading from {url}")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        context.log.info(f"{year}: saved to {out_path} ({len(resp.content)} bytes)")
        downloaded.append(year)

    return dg.MaterializeResult(
        metadata={
            "years_downloaded": dg.MetadataValue.text(str(sorted(downloaded))),
            "bronze_dir": dg.MetadataValue.path(str(BRONZE_DIR)),
        }
    )