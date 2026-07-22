from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog

def get_catalog():
    warehouse_path = Path(__file__).parent.parent / "02_data" / "01_iceberg"
    return SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )