import os
import json
import pandas as pd
from rich.console import Console

console = Console()

RAW_PATH = "data/raw/la_homelessness.json"
PROCESSED_PATH = "data/clean/la_homelessness.parquet"


def load_raw_json(path: str) -> pd.DataFrame:
    console.print("[yellow]Loading raw JSON data...[/yellow]")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    console.print(f"[green]Loaded {len(df)} records.[/green]")
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    console.print("[yellow]Cleaning dataframe...[/yellow]")

    # Standardize column names
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Example type fixes (depends on dataset fields)
    for col in df.columns:
        if col.endswith("_count") or col.endswith("_number"):
            df[col] = pd.to_numeric(df[col], errors="ignore")

    console.print("[green]Dataframe cleaned.[/green]")
    return df


def save_parquet(df: pd.DataFrame, path: str):
    console.print("[yellow]Saving processed data to Parquet...[/yellow]")

    # Override the path so we always write to clean/
    path = "data/clean/la_homelessness.parquet"
    print("Writing to:", path)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)

    console.print(f"[green]Saved processed dataset to:[/green] {path}")


def main():
    console.rule("[bold blue]LA Homelessness Data Transformation")

    df_raw = load_raw_json(RAW_PATH)
    df_clean = clean_dataframe(df_raw)
    save_parquet(df_clean, PROCESSED_PATH)

    console.print("[bold green]Transformation complete![/bold green]")


if __name__ == "__main__":
    main()
