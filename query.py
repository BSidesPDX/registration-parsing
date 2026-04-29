"""Interactive REPL for exploring and querying Square order data."""

import re
from pathlib import Path
import polars as pl

PARQUET_PATH = Path("data/orders.parquet")
PREVIEW_ROWS = 20

pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_hide_dtype_separator(True)

CONFERENCES = ["Hackboat", "BSidesPDX"]

SHOW_COLUMNS = [
    "Recipient Name",
    "Recipient Email",
    "Item Name",
    "Item Variation",
    "Item Modifiers",
    "Order Date",
    "Item Total Price",
]

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

FILTERS = {
    "item": "Item Name",
    "name": "Recipient Name",
    "email": "Recipient Email",
}

# friendly key -> (df column, default descending, regs-only)
SORTABLE = {
    "date":  ("Order Date", True, False),
    "name":  ("Recipient Name", False, False),
    "email": ("Recipient Email", False, False),
    "shirt": ("Shirt Sort Key", False, True),
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


def print_table(df: pl.DataFrame, columns: list[str] | None = None,
                full: bool = False) -> None:
    if columns is None:
        columns = [c for c in SHOW_COLUMNS if c in df.columns] or df.columns
    if df.shape[0] == 0:
        print("No rows to display." if full else "\n[0 rows]")
        return
    if not full:
        print(f"\n[{df.shape[0]} rows]")
    view = df.select(columns) if full else df.select(columns).head(PREVIEW_ROWS)
    if full:
        with pl.Config(tbl_rows=-1, tbl_cols=len(columns), fmt_str_lengths=60):
            print(view)
    else:
        print(view)


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


def print_help() -> None:
    print(
        """
Commands:
  item <text>            Filter where Item Name contains text
  date <start> <end>     Filter by Order Date range (YYYY-MM-DD or YYYY/MM/DD)
  name <text>            Filter where Recipient Name contains text
  email <text>           Filter where Recipient Email contains text
  show                   Show all matching rows (key columns)
  sort <col>             Sort results (date|name|email|shirt, toggles asc/desc)
  regs                   Show registration list (Name, Email, Shirt, Date)
  address                Toggle address column in registration views
  columns                List all available columns
  count                  Show current row count
  reset                  Reset to conference baseline
  export <file.csv>      Export current full data to CSV
  export regs <file.csv> Export registration list to CSV
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

    while True:
        base = select_conference(all_data)
        if base is None:
            break

        df = base.clone()
        print_table(df)

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
                col, _, regs_only = SORTABLE[sort_key]
                if not regs_only:
                    df = df.sort(col, descending=sort_desc)
                print("Filters cleared.")
                print_table(df)

            elif cmd == "count":
                print(f"{df.shape[0]} rows")

            elif cmd == "columns":
                for i, col in enumerate(df.columns, 1):
                    print(f"  {i:2}. {col}")

            elif cmd == "show":
                print_table(df, full=True)

            elif cmd == "sort":
                key = arg.lower()
                if key not in SORTABLE:
                    print(f"Usage: sort <{'|'.join(SORTABLE)}>")
                    continue
                col, default_desc, regs_only = SORTABLE[key]
                if sort_key == key:
                    sort_desc = not sort_desc
                else:
                    sort_key = key
                    sort_desc = default_desc
                direction = "desc" if sort_desc else "asc"
                if regs_only:
                    print(f"Sort set to {key} ({direction}) — applies to regs view")
                else:
                    df = df.sort(col, descending=sort_desc)
                    print(f"Sorted by {col} ({direction})")
                    print_table(df)

            elif cmd == "address":
                include_address = not include_address
                print(f"Address column: {'on' if include_address else 'off'}")

            elif cmd == "regs":
                col, _, _ = SORTABLE[sort_key]
                print_table(regs_view(df, include_address, col, sort_desc), full=True)

            elif cmd in FILTERS:
                if not arg:
                    print(f"Usage: {cmd} <text>")
                    continue
                df = df.filter(pl.col(FILTERS[cmd]).str.contains(f"(?i){arg}"))
                print_table(df)

            elif cmd == "date":
                date_parts = arg.split()
                if len(date_parts) != 2:
                    print("Usage: date <start> <end>  (e.g. date 2026-01-01 2026-04-01)")
                    continue
                # Order Date is stored as YYYY/MM/DD strings; normalize input.
                start, end = (s.replace("-", "/") for s in date_parts)
                df = df.filter(
                    (pl.col("Order Date") >= start) & (pl.col("Order Date") <= end)
                )
                print_table(df)

            elif cmd == "export":
                exp = arg.split(maxsplit=1)
                if not exp or (exp[0].lower() == "regs" and len(exp) < 2):
                    print("Usage: export <file.csv>  or  export regs <file.csv>")
                    continue
                if exp[0].lower() == "regs":
                    filename = exp[1]
                    out = regs_view(df, include_address, "Order Date", True)
                else:
                    filename = exp[0]
                    out = df.drop("Shirt Sort Key")
                if not filename.endswith(".csv"):
                    filename += ".csv"
                path = Path("data") / filename
                out.write_csv(path)
                print(f"Exported {out.shape[0]} rows to {path}")

            else:
                print(f'Unknown command: "{cmd}". Type "help" for available commands.')


if __name__ == "__main__":
    run()
