# Flowcase ETL

Production-style ETL for Flowcase CV Partner reports: extract the latest `Q####` export, transform to a relational schema, load into Postgres, refresh the search view, and log KPIs.

## Required environment variables

- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

## How to run (from repo root)

```bash
# Generate fake reports then run ETL
PYTHONPATH=flowcase_etl/src python -m flowcase_etl.cli --generate-fake

# Use existing real reports under cv_reports/Q#### (no fake generation)
PYTHONPATH=flowcase_etl/src python -m flowcase_etl.cli
```

## CLI flags

- `--generate-fake` : run the synthetic report generator first
- `--data-folder`   : override reports folder (default: repo_root/cv_reports)
- `--sql-folder`    : override SQL folder (default: flowcase_etl/src/sql)
- `--skip-refresh`  : skip refreshing the materialized view

## What it does

1. (Optional) Generate fake Flowcase-style reports into `cv_reports/Q####`.
2. Apply schema from `src/sql/*.sql` (idempotent).
3. Extract latest quarterly folder, transform, load with upserts.
4. Refresh `cv_search_profile_mv` (best-effort).
5. Log KPIs: users, CVs, top skills, SC-cleared availability, average availability.

## Folder guide

- `flowcase_etl/src/flowcase_etl/` : package code (config, db, extract, transform, load, cli)
- `flowcase_etl/src/sql/`          : schema and materialized view
- `flowcase_etl/src/tests/`        : unit/integration tests (start here: extract/transform tests)
- `experiments/`                   : notebook + generator for exploration (not used by production package)

## Scheduling

- Manual: drop the latest `Q####` export into `cv_reports/` and run the CLI.
- Cron example:  
  `0 6 1 * * PYTHONPATH=/path/to/flowcase_etl/src /path/to/.venv/bin/python -m flowcase_etl.cli >> etl.log 2>&1`
- Airflow: call `python -m flowcase_etl.cli` from a BashOperator/PythonOperator; ensure env vars are set on the worker.

## Notes

- Real Flowcase exports can replace the fake data directly (same filenames/columns).
- Keep `.env`/secrets out of git; use env vars for Postgres credentials.
