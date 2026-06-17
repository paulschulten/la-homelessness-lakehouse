import os
import pandas as pd
from rich.console import Console

console = Console()

SILVER_PATH = "data/clean/la_homelessness.parquet"
GOLD_PATH = "data/gold/pit_yearly.parquet"


def load_silver(path: str) -> pd.DataFrame:
    console.print("[yellow]Loading Silver dataset...[/yellow]")
    df = pd.read_parquet(path)
    console.print(f"[green]Loaded {len(df)} rows.[/green]")
    return df


def build_gold_yearly(df: pd.DataFrame) -> pd.DataFrame:
    console.print("[yellow]Aggregating yearly metrics...[/yellow]")
    df["year"] = 2024

    # Group by year and sum numeric fields
    yearly = df.groupby("year").sum(numeric_only=True)

    # Derived metrics
    yearly["pct_unsheltered"] = (
        yearly["unsheltered"] / yearly["total_homeless"]
    )

    yearly["pct_change_total"] = (
        yearly["total_homeless"].pct_change()
    )

    console.print("[green]Gold table created.[/green]")
    return yearly.reset_index()


def save_gold(df: pd.DataFrame, path: str):
    console.print("[yellow]Saving Gold dataset...[/yellow]")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    console.print(f"[green]Saved Gold table to:[/green] {path}")


def main():
    console.rule("[bold blue]Building Gold: PIT Yearly Table")

    df_silver = load_silver(SILVER_PATH)
    df_gold = build_gold_yearly(df_silver)
    save_gold(df_gold, GOLD_PATH)

    console.print("[bold green]Gold transformation complete![/bold green]")


if __name__ == "__main__":
    main()
