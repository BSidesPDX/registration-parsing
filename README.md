# registration-parsing

Python tool for parsing Square reports.

Old scripts (`parse.py`, `emails.py`) have been moved to `archive/`.

## Exporting from Square

1. Go to https://app.squareup.com/dashboard/orders/overview
2. Select the date range you want
3. In the upper right, choose Export -> Standard Export CSV button -> Download
4. Unzip the downloaded CSV files into `data/reports/`

## Setup

```
uv sync
```

## Usage

Ingest all CSV exports into a single parquet file:

```
uv run python ingest.py
```

Output: `data/orders.parquet`

## Querying

Launch the interactive query REPL:

```
uv run python query.py
```

Select a conference (or all), then explore with filters. Example session:

```
> item Hackboat
> date 2026-03-01 2026-04-30
> show
> regs
> export regs hackboat-sailors.csv
> sort name
> reset
> back
```

Type `help` in the REPL for all commands.
