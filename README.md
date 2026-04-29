# registration-parsing

Python tool for parsing Square reports.

## Setup

```
uv sync
```

## Usage

Place Square order CSV exports in `data/reports/`, then ingest them into a single parquet file:

```
uv run python ingest.py
```

Output: `data/orders.parquet`
