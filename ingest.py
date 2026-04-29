"""Ingest Square order CSV exports into a single parquet file."""

from pathlib import Path
import polars as pl

REPORTS_DIR = Path("data/reports")
OUTPUT_PATH = Path("data/orders.parquet")


def ingest() -> pl.DataFrame:
    csv_files = sorted(REPORTS_DIR.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {REPORTS_DIR}")
        raise SystemExit(1)

    print(f"Found {len(csv_files)} CSV file(s):")
    for f in csv_files:
        print(f"  - {f.name}")

    frames = [pl.read_csv(f) for f in csv_files]
    df = pl.concat(frames)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUTPUT_PATH)

    print(f"\nRows:    {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")
    print(f"Written: {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    ingest()
