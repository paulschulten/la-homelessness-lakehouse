import os
import requests
from datetime import datetime
from rich.console import Console

console = Console()

# LA City Business Licenses dataset (Socrata)
DATA_URL = "https://data.lacity.org/resource/6rrh-rzua.json"

def ingest_lacity_business(output_dir: str = "data/raw/lacity_business"):
    console.rule("[bold blue]LA City Business License Ingestion[/bold blue]")

    try:
        console.print("[yellow]Requesting Business License data from LA Open Data API...[/yellow]")
        response = requests.get(DATA_URL)

        if response.status_code != 200:
            console.print(f"[red]Failed to download data. Status code: {response.status_code}[/red]")
            return

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Timestamped filename for Bronze layer
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = os.path.join(output_dir, f"lacity_business_{timestamp}.json")

        # Write raw JSON response
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        console.print(f"[green]Success! Business License data saved to:[/green] {output_path}")

    except Exception as e:
        console.print(f"[red]Error during ingestion: {e}[/red]")


if __name__ == "__main__":
    ingest_lacity_business()
