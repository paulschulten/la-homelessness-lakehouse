
import os
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.catalog.glue import GlueCatalog


def get_catalog():
    if os.environ.get("LAKEHOUSE_ENV") == "aws":
        return GlueCatalog(
            "default",
            **{
                "warehouse": "s3://la-homelessness-lakehouse-277607772876-us-east-2-an",
                "region_name": "us-east-2",
            },
        )

    warehouse_path = Path(__file__).parent.parent / "02_data" / "01_iceberg"
    return SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )