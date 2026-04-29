"""Interactive REPL for querying ingested Square order data."""

from pathlib import Path
import polars as pl

PARQUET_PATH = Path("data/orders.parquet")

SHOW_COLUMNS = [
    "Recipient Name",
    "Recipient Email",
    "Item Name",
    "Item Variation",
    "Item Modifiers",
    "Order Date",
    "Item Total Price",
]


def load_data() -> pl.DataFrame:
    if not PARQUET_PATH.exists():
        print(f"Parquet file not found at {PARQUET_PATH}")
        print("Run ingest.py first to generate it.")
        raise SystemExit(1)

    df = pl.read_parquet(PARQUET_PATH)
    before = df.shape[0]
    df = df.filter(pl.col("Item Name").is_not_null() & (pl.col("Item Name") != ""))
    after = df.shape[0]
    if before != after:
        print(f"Dropped {before - after} rows with empty Item Name")
    df = df.sort("Order Date", descending=True)
    return df


def print_summary(df: pl.DataFrame) -> None:
    print(f"\n[{df.shape[0]} rows x {df.shape[1]} columns]")
    if df.shape[0] > 0:
        display_cols = [c for c in SHOW_COLUMNS if c in df.columns]
        with pl.Config(
            tbl_hide_column_data_types=True, tbl_hide_dtype_separator=True
        ):
            print(df.select(display_cols).head(20))


def print_full(df: pl.DataFrame) -> None:
    if df.shape[0] == 0:
        print("No rows to display.")
        return
    display_cols = [c for c in SHOW_COLUMNS if c in df.columns]
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=len(display_cols),
        fmt_str_lengths=60,
        tbl_hide_column_data_types=True,
        tbl_hide_dtype_separator=True,
    ):
        print(df.select(display_cols))


def print_help() -> None:
    print(
        """
Commands:
  item <text>          Filter where Item Name contains text
  date <start> <end>   Filter by Order Date range (YYYY-MM-DD or YYYY/MM/DD)
  name <text>          Filter where Recipient Name contains text
  email <text>         Filter where Recipient Email contains text
  show                 Show all matching rows (key columns)
  columns              List all available columns
  count                Show current row count
  reset                Clear all filters
  export <file.csv>    Export current results to CSV
  help                 Show this help
  quit / exit          Exit
"""
    )


def run_repl() -> None:
    base = load_data()
    df = base.clone()
    print(f"Loaded {df.shape[0]} rows from {PARQUET_PATH}")
    print('Type "help" for available commands.\n')
    print_summary(df)

    while True:
        try:
            raw = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd in ("quit", "exit"):
            break

        elif cmd == "help":
            print_help()

        elif cmd == "reset":
            df = base.clone()
            print("Filters cleared.")
            print_summary(df)

        elif cmd == "count":
            print(f"{df.shape[0]} rows")

        elif cmd == "columns":
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2}. {col}")

        elif cmd == "show":
            print_full(df)

        elif cmd == "item":
            if not arg:
                print("Usage: item <text>")
                continue
            df = df.filter(pl.col("Item Name").str.contains(f"(?i){arg}"))
            print_summary(df)

        elif cmd == "name":
            if not arg:
                print("Usage: name <text>")
                continue
            df = df.filter(pl.col("Recipient Name").str.contains(f"(?i){arg}"))
            print_summary(df)

        elif cmd == "email":
            if not arg:
                print("Usage: email <text>")
                continue
            df = df.filter(pl.col("Recipient Email").str.contains(f"(?i){arg}"))
            print_summary(df)

        elif cmd == "date":
            date_parts = arg.split()
            if len(date_parts) != 2:
                print("Usage: date <start> <end>  (e.g. date 2026-01-01 2026-04-01)")
                continue
            start, end = date_parts
            start = start.replace("-", "/")
            end = end.replace("-", "/")
            df = df.filter(
                (pl.col("Order Date") >= start) & (pl.col("Order Date") <= end)
            )
            print_summary(df)

        elif cmd == "export":
            if not arg:
                print("Usage: export <filename.csv>")
                continue
            path = Path("data") / arg
            df.write_csv(path)
            print(f"Exported {df.shape[0]} rows to {path}")

        else:
            print(f'Unknown command: "{cmd}". Type "help" for available commands.')


if __name__ == "__main__":
    run_repl()
