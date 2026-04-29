"""Interactive REPL for exploring and querying Square order data."""

import re
from pathlib import Path
import polars as pl

PARQUET_PATH = Path("data/orders.parquet")

CONFERENCES = {
    "hackboat": {
        "label": "Hackboat",
        "item_pattern": "Hackboat",
    },
    "bsides": {
        "label": "BSidesPDX",
        "item_pattern": "BSidesPDX",
    },
}

SHOW_COLUMNS = [
    "Recipient Name",
    "Recipient Email",
    "Item Name",
    "Item Variation",
    "Item Modifiers",
    "Order Date",
    "Item Total Price",
]

REG_COLUMNS = ["Name", "Email", "Shirt Size", "Registration Date"]

SHIRT_SIZES = {"S", "M", "L", "XL", "2XL", "3XL", "XXL", "XXXL", "XS"}
SHIRT_PATTERN = re.compile(r"(?:Standard|Fitted)\s+Cut\s+\w+", re.IGNORECASE)

ADDRESS_FIELDS = [
    "Recipient Address",
    "Recipient Address 2",
    "Recipient City",
    "Recipient Region",
    "Recipient Postal Code",
    "Recipient Country",
]

SORTABLE = {
    "date": ("Order Date", True),
    "name": ("Recipient Name", False),
    "email": ("Recipient Email", False),
}


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


def parse_shirt_size(modifiers: str | None) -> str:
    """Extract shirt size from Item Modifiers string."""
    if not modifiers or modifiers.strip() == "":
        return ""

    parts = [p.strip() for p in modifiers.split(",")]
    for part in parts:
        value = re.sub(r"^\d+\s*x\s*", "", part).strip()
        if value.upper() == "NONE":
            continue
        if value.upper() in SHIRT_SIZES:
            return value
        if SHIRT_PATTERN.match(value):
            return value
    return ""


def build_address(row: dict) -> str:
    """Join address fields into a single string, skipping blanks."""
    parts = []
    for field in ADDRESS_FIELDS:
        val = row.get(field)
        if val and str(val).strip() and str(val).strip() not in ("", "None"):
            parts.append(str(val).strip())
    return ", ".join(parts)


def build_registrations(df: pl.DataFrame, include_address: bool) -> pl.DataFrame:
    """Build a registration list from the current filtered data."""
    if df.shape[0] == 0:
        return pl.DataFrame()

    shirt_sizes = [
        parse_shirt_size(mod)
        for mod in df.get_column("Item Modifiers").to_list()
    ]

    result = pl.DataFrame({
        "Name": df.get_column("Recipient Name"),
        "Email": df.get_column("Recipient Email"),
        "Shirt Size": shirt_sizes,
    })

    if include_address:
        addresses = [
            build_address(row)
            for row in df.select(ADDRESS_FIELDS).to_dicts()
        ]
        result = result.with_columns(pl.Series("Address", addresses))

    result = result.with_columns(
        df.get_column("Order Date").alias("Registration Date")
    )
    return result


def print_summary(df: pl.DataFrame, columns: list[str] | None = None) -> None:
    cols = columns or [c for c in SHOW_COLUMNS if c in df.columns]
    print(f"\n[{df.shape[0]} rows]")
    if df.shape[0] > 0:
        with pl.Config(
            tbl_hide_column_data_types=True, tbl_hide_dtype_separator=True
        ):
            print(df.select(cols).head(20))


def print_full(df: pl.DataFrame, columns: list[str] | None = None) -> None:
    if df.shape[0] == 0:
        print("No rows to display.")
        return
    cols = columns or [c for c in SHOW_COLUMNS if c in df.columns]
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=len(cols),
        fmt_str_lengths=60,
        tbl_hide_column_data_types=True,
        tbl_hide_dtype_separator=True,
    ):
        print(df.select(cols))


def print_help() -> None:
    print(
        """
Commands:
  item <text>            Filter where Item Name contains text
  date <start> <end>     Filter by Order Date range (YYYY-MM-DD or YYYY/MM/DD)
  name <text>            Filter where Recipient Name contains text
  email <text>           Filter where Recipient Email contains text
  show                   Show all matching rows (key columns)
  sort <date|name|email> Sort results (toggles asc/desc)
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


def select_conference(all_data: pl.DataFrame) -> tuple[pl.DataFrame, str | None]:
    """Prompt for conference selection. Returns (filtered_df, conf_key or None)."""
    while True:
        print("Select conference:")
        conf_keys = list(CONFERENCES.keys())
        for i, key in enumerate(conf_keys, 1):
            print(f"  {i}. {CONFERENCES[key]['label']}")
        print("  a. All (no filter)")
        print("  q. Quit")

        choice = input("\n> ").strip().lower()

        if choice in ("q", "quit", "exit"):
            return pl.DataFrame(), None

        if choice == "a":
            print(f"\nAll orders: {all_data.shape[0]} rows")
            return all_data.clone(), "__all__"

        try:
            idx = int(choice) - 1
            if idx < 0 or idx >= len(conf_keys):
                raise ValueError
            conf_key = conf_keys[idx]
        except ValueError:
            print("Invalid selection.\n")
            continue

        conf = CONFERENCES[conf_key]
        filtered = all_data.filter(
            pl.col("Item Name").str.contains(f"(?i){conf['item_pattern']}")
        )
        print(f"\n{conf['label']}: {filtered.shape[0]} rows")
        return filtered, conf_key


def run() -> None:
    all_data = load_data()
    print(f"\n=== Square Order Explorer ===")
    print(f"Loaded {all_data.shape[0]} orders. Type \"help\" for commands.\n")

    include_address = False
    sort_col = "Order Date"
    sort_desc = True

    while True:
        base, conf_key = select_conference(all_data)
        if conf_key is None:
            break

        df = base.clone()
        print_summary(df)

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
                df = df.sort(sort_col, descending=sort_desc)
                print("Filters cleared.")
                print_summary(df)

            elif cmd == "count":
                print(f"{df.shape[0]} rows")

            elif cmd == "columns":
                for i, col in enumerate(df.columns, 1):
                    print(f"  {i:2}. {col}")

            elif cmd == "show":
                print_full(df)

            elif cmd == "sort":
                if not arg or arg.lower() not in SORTABLE:
                    print(f"Usage: sort <{'|'.join(SORTABLE.keys())}>")
                    continue
                new_col, default_desc = SORTABLE[arg.lower()]
                if sort_col == new_col:
                    sort_desc = not sort_desc
                else:
                    sort_col = new_col
                    sort_desc = default_desc
                df = df.sort(sort_col, descending=sort_desc)
                direction = "desc" if sort_desc else "asc"
                print(f"Sorted by {sort_col} ({direction})")
                print_summary(df)

            elif cmd == "address":
                include_address = not include_address
                state = "on" if include_address else "off"
                print(f"Address column: {state}")

            elif cmd == "regs":
                regs = build_registrations(df, include_address)
                if regs.shape[0] == 0:
                    print("No rows to display.")
                else:
                    # Map full-data sort columns to registration columns
                    reg_sort_map = {
                        "Order Date": "Registration Date",
                        "Recipient Name": "Name",
                        "Recipient Email": "Email",
                    }
                    reg_sort = reg_sort_map.get(sort_col, "Registration Date")
                    regs = regs.sort(reg_sort, descending=sort_desc)
                    print_full(regs, regs.columns)

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
                if arg.lower().startswith("regs"):
                    # export regs <file>
                    rest = arg[4:].strip()
                    if not rest:
                        print("Usage: export regs <filename.csv>")
                        continue
                    regs = build_registrations(df, include_address)
                    if regs.shape[0] == 0:
                        print("No rows to export.")
                        continue
                    regs = regs.sort("Registration Date", descending=True)
                    path = Path("data") / rest
                    regs.write_csv(path)
                    print(f"Exported {regs.shape[0]} registrations to {path}")
                else:
                    if not arg:
                        print("Usage: export <filename.csv>  or  export regs <filename.csv>")
                        continue
                    path = Path("data") / arg
                    df.write_csv(path)
                    print(f"Exported {df.shape[0]} rows to {path}")

            else:
                print(f'Unknown command: "{cmd}". Type "help" for available commands.')


if __name__ == "__main__":
    run()
