"""Interactive REPL for exploring Square order registration data."""

import re
from pathlib import Path
import polars as pl

PARQUET_PATH = Path("data/orders.parquet")
PREVIEW_ROWS = 20

pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_hide_dtype_separator(True)

CONFERENCES = ["Hackboat", "BSidesPDX"]

ADDRESS_FIELDS = [
    "Recipient Address",
    "Recipient Address 2",
    "Recipient City",
    "Recipient Region",
    "Recipient Postal Code",
    "Recipient Country",
]

# XXL/XXXL are aliases for 2XL/3XL.
SIZE_ORDER = {"XS": 0, "S": 1, "M": 2, "L": 3, "XL": 4,
              "2XL": 5, "3XL": 6, "4XL": 7, "XXL": 5, "XXXL": 6}
CUT_RANK = {"plain": 0, "fitted": 1, "standard": 2}
CUT_RE = re.compile(r"^(fitted|standard)\s+cut\s+(\S+)", re.IGNORECASE)
QTY_PREFIX = re.compile(r"^\s*\d+\s*x\s*", re.IGNORECASE)

FILTER_COLUMNS = ["Recipient Name", "Recipient Email"]

# friendly key -> (df column, default descending)
SORTABLE = {
    "date":  ("Order Date", True),
    "name":  ("Recipient Name", False),
    "email": ("Recipient Email", False),
    "shirt": ("Shirt Sort Key", False),
}


def parse_shirt(modifiers: str | None) -> tuple[str, int]:
    """Return (display, sort_key) parsed from an Item Modifiers string."""
    if not modifiers:
        return ("", 999)
    for part in modifiers.split(","):
        value = QTY_PREFIX.sub("", part).strip()
        if not value or value.upper() == "NONE":
            continue
        if value.upper() in SIZE_ORDER:
            return (value, SIZE_ORDER[value.upper()])
        m = CUT_RE.match(value)
        if m:
            cut = m.group(1).lower()
            size_rank = SIZE_ORDER.get(m.group(2).upper(), 99)
            return (value, CUT_RANK[cut] * 100 + size_rank)
    return ("", 999)


def build_address(row: dict) -> str:
    parts = [str(row[f]).strip() for f in ADDRESS_FIELDS
             if row.get(f) and str(row[f]).strip()]
    return ", ".join(parts)


def load_data() -> pl.DataFrame:
    if not PARQUET_PATH.exists():
        print(f"Parquet file not found at {PARQUET_PATH}")
        print("Run ingest.py first to generate it.")
        raise SystemExit(1)

    df = pl.read_parquet(PARQUET_PATH)
    before = df.shape[0]
    df = df.filter(pl.col("Item Name").is_not_null() & (pl.col("Item Name") != ""))
    if before != df.shape[0]:
        print(f"Dropped {before - df.shape[0]} rows with empty Item Name")

    shirts = [parse_shirt(m) for m in df.get_column("Item Modifiers").to_list()]
    addresses = [build_address(r) for r in df.select(ADDRESS_FIELDS).to_dicts()]
    df = df.with_columns(
        pl.Series("Shirt Size", [s[0] for s in shirts]),
        pl.Series("Shirt Sort Key", [s[1] for s in shirts]),
        pl.Series("Address", addresses),
    )
    return df.sort("Order Date", descending=True)


def regs_view(df: pl.DataFrame, include_address: bool,
              sort_col: str, sort_desc: bool) -> pl.DataFrame:
    cols = ["Recipient Name", "Recipient Email", "Shirt Size", "Shirt Sort Key"]
    if include_address:
        cols.append("Address")
    cols.append("Order Date")
    out = df.select(cols).sort(sort_col, descending=sort_desc).drop("Shirt Sort Key")
    return out.rename({
        "Recipient Name": "Name",
        "Recipient Email": "Email",
        "Order Date": "Registration Date",
    })


def print_table(df: pl.DataFrame, full: bool = False) -> None:
    if df.shape[0] == 0:
        print("\n[0 rows]")
        return
    if not full:
        print(f"\n[{df.shape[0]} rows]")
        print(df.head(PREVIEW_ROWS))
    else:
        with pl.Config(tbl_rows=-1, tbl_cols=df.shape[1], fmt_str_lengths=60):
            print(df)


def print_help() -> None:
    print(
        """
Commands:
  filter <text>          Filter by name or email
  range <start> <end>    Filter by date range (YYYY-MM-DD or YYYY/MM/DD)
  sort <col>             Sort (date|name|email|shirt, toggles asc/desc)
  address                Toggle address column
  count                  Show current row count
  full                   Show all rows (untruncated)
  reset                  Clear all filters
  export <filename>      Export to CSV
  back                   Return to conference selection
  help                   Show this help
  quit / exit            Exit
"""
    )


def select_conference(all_data: pl.DataFrame) -> pl.DataFrame | None:
    while True:
        print("Select conference:")
        for i, label in enumerate(CONFERENCES, 1):
            print(f"  {i}. {label}")
        print("  a. All (no filter)")
        print("  q. Quit")
        choice = input("\n> ").strip().lower()

        if choice in ("q", "quit", "exit"):
            return None
        if choice == "a":
            print(f"\nAll orders: {all_data.shape[0]} rows")
            return all_data.clone()

        try:
            idx = int(choice) - 1
            if idx < 0:
                raise IndexError
            label = CONFERENCES[idx]
        except (ValueError, IndexError):
            print("Invalid selection.\n")
            continue

        filtered = all_data.filter(pl.col("Item Name").str.contains(f"(?i){label}"))
        print(f"\n{label}: {filtered.shape[0]} rows")
        return filtered


def run() -> None:
    all_data = load_data()
    print("\n=== Square Order Explorer ===")
    print(f"Loaded {all_data.shape[0]} orders. Type \"help\" for commands.\n")

    include_address = False
    sort_key = "date"
    sort_desc = SORTABLE[sort_key][1]

    def show(df: pl.DataFrame, full: bool = False) -> None:
        col, _ = SORTABLE[sort_key]
        print_table(regs_view(df, include_address, col, sort_desc), full=full)

    while True:
        base = select_conference(all_data)
        if base is None:
            break

        df = base.clone()
        show(df)

        while True:
            try:
                raw = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return

            if not raw:
                continue

            parts = raw.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""

            if cmd in ("quit", "exit"):
                return

            elif cmd == "back":
                print()
                break

            elif cmd == "help":
                print_help()

            elif cmd == "reset":
                df = base.clone()
                print("Filters cleared.")
                show(df)

            elif cmd == "count":
                print(f"{df.shape[0]} rows")

            elif cmd == "full":
                show(df, full=True)

            elif cmd == "sort":
                key = arg.lower()
                if key not in SORTABLE:
                    print(f"Usage: sort <{'|'.join(SORTABLE)}>")
                    continue
                _, default_desc = SORTABLE[key]
                if sort_key == key:
                    sort_desc = not sort_desc
                else:
                    sort_key = key
                    sort_desc = default_desc
                direction = "desc" if sort_desc else "asc"
                print(f"Sorted by {key} ({direction})")
                show(df)

            elif cmd == "address":
                include_address = not include_address
                print(f"Address column: {'on' if include_address else 'off'}")
                show(df)

            elif cmd == "filter":
                if not arg:
                    print("Usage: filter <text>")
                    continue
                pattern = f"(?i){arg}"
                mask = pl.lit(False)
                for col_name in FILTER_COLUMNS:
                    mask = mask | pl.col(col_name).str.contains(pattern)
                df = df.filter(mask)
                show(df)

            elif cmd == "range":
                date_parts = arg.split()
                if len(date_parts) != 2:
                    print("Usage: range <start> <end>  (e.g. range 2026-01-01 2026-04-01)")
                    continue
                start, end = (s.replace("-", "/") for s in date_parts)
                df = df.filter(
                    (pl.col("Order Date") >= start) & (pl.col("Order Date") <= end)
                )
                show(df)

            elif cmd == "export":
                if not arg:
                    print("Usage: export <filename>")
                    continue
                filename = arg
                if not filename.endswith(".csv"):
                    filename += ".csv"
                col, _ = SORTABLE[sort_key]
                out = regs_view(df, include_address, col, sort_desc)
                path = Path("data") / filename
                out.write_csv(path)
                print(f"Exported {out.shape[0]} rows to {path}")

            else:
                print(f'Unknown command: "{cmd}". Type "help" for available commands.')


if __name__ == "__main__":
    run()
