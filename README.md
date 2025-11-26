# Flowcase ETL

A production-style ETL pipeline for processing Flowcase (CV Partner) CSV reports.
The pipeline extracts the latest quarterly export (Q####), transforms the raw files into a clean relational model, loads them into PostgreSQL, refreshes a materialized search view, and outputs key operational KPIs.

## Required environment variables

The ETL relies on standard Postgres environment variables:

- PGHOST
- PGPORT
- PGDATABASE
- PGUSER
- PGPASSWORD

(Optional, for real Flowcase API mode)

- FLOWCASE_DATA_SOURCE = fake or real
- FLOWCASE_SUBDOMAIN
- LOWCASE_API_TOKEN
- FLOWCASE_OFFICE_IDS (comma-separated)
- FLOWCASE_LANG_PARAMS (comma-separated)

Use a .env file or export variables before running the pipeline.

## Running the Pipeline

From the repo root:

```bash
# Generate synthetic Flowcase reports and run the full ETL
PYTHONPATH=flowcase_etl/src python -m flowcase_etl_pipeline.cli --generate-fake

# Run ETL using existing reports under cv_reports/Q####
PYTHONPATH=flowcase_etl/src python -m flowcase_etl_pipeline.cli
```

When using real Flowcase API mode (FLOWCASE_DATA_SOURCE=real), the CLI automatically downloads all report types into a timestamped folder before extraction.

## CLI flags

| Flag              | Description                                                              |
| ----------------- | ------------------------------------------------------------------------ |
| `--generate-fake` | Generate synthetic Flowcase reports before running ETL (fake mode only). |
| `--data-folder`   | Override the reports directory (default: `repo_root/cv_reports`).        |
| `--sql-folder`    | Override the SQL schema directory (default: `flowcase_etl/src/sql`).     |
| `--skip-refresh`  | Skip refreshing the materialized search profile view.                    |

## Pipeline Stages

1 Acquire data

- Fake mode: generate cv_reports/Q#### synthetic exports
- Real mode: download CSVs using the Flowcase API client

2 Database setup

- Ensure database exists
- Apply schema from src/sql/*.sql (idempotent)

3 Extract

- Locate the latest Q#### folder
- Load all CSV files into DataFrames

4 Transform

- Clean and normalise user, CV, language, skills, experience and qualification data
- Parse multilingual fields, dates, and nested structures

5 Load

- Upsert into all relational tables
- Maintain dimensions, relationships and constraints

6 View refresh

- Refresh cv_search_profile_mv (best-effort)

7 KPI Logging

- Total users
- Total CVs
- Top 5 technologies
- Number of SC-cleared staff with ≥50% availability
- Average availability across all users

## Repository Structure

```bash
flowcase_etl/
├── src/
│   ├── flowcase_etl_pipeline/
│   │   ├── config.py        # Env handling, settings, data-source mode
│   │   ├── db.py            # Engine, schema application
│   │   ├── extract.py       # CSV discovery + loading
│   │   ├── transform.py     # Cleaning + shaping
│   │   ├── load.py          # Upserts into relational schema
│   │   ├── cli.py           # Orchestration entrypoint
│   │   ├── flowcase_client.py # Real API downloader
│   ├── sql/                 # Schema, indexes, materialized views
│   └── tests/               # Unit + integration tests
└── cv_reports/              # Fake or real Flowcase exports (Q####)
```

Additional:

`early_experimentation` — early notebooks and prototypes (not used in production)

## Scheduling & Automation

### Manual

- drop the latest `Q####` export into `cv_reports/` and run the CLI.

### Cron example

- `0 6 1 * * PYTHONPATH=/path/to/src /path/to/.venv/bin/python -m flowcase_etl_pipeline.cli >> etl.log 2>&1`

### Airflow

- call `python -m flowcase_etl.cli` from a BashOperator/PythonOperator; ensure env vars are set on the worker.

## Notes

- Real Flowcase exports have identical filenames/columns, so they can replace fake reports seamlessly.
- Keep .env files and secrets out of version control.
- The ETL is idempotent: schema application and upserts are safe to re-run.
- Designed for extension → additional report types and sources (e.g., BambooHR, Kantata) can be added by implementing new transform/load modules.
