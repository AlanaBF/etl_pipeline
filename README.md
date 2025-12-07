# Flowcase ETL

A production-style ETL pipeline for processing Flowcase (CV Partner) CSV reports.
The pipeline extracts the latest quarterly export (Q####), transforms the raw files into a clean relational model, loads them into PostgreSQL, refreshes a materialized search view, and outputs key operational KPIs.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Tech stack

Developed in [![Visual Studio Code](https://custom-icon-badges.demolab.com/badge/Visual%20Studio%20Code-0078d7.svg?logo=vsc&logoColor=white)](https://code.visualstudio.com/)

Backend: Python 3.10+, SQLAlchemy

[![ETL](https://custom-icon-badges.demolab.com/badge/ETL-9370DB?logo=etl-logo&logoColor=fff)](https://en.wikipedia.org/wiki/Extract,_transform,_load)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=fff)](https://www.python.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-ffffff?logo=Jupyter)](https://jupyter.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=fff)](https://pandas.pydata.org/)

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=fff)](https://airflow.apache.org/)
[![Pytest](https://img.shields.io/badge/Pytest-fff?logo=pytest&logoColor=000)](https://docs.pytest.org/)

Database: PostgreSQL (12+)

[![Postgres](https://img.shields.io/badge/Postgres-%23316192.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

Dev tooling: npm, node 18+, pip, virtualenv/venv

[![npm](https://img.shields.io/badge/npm-CB3837?logo=npm&logoColor=fff)](https://www.npmjs.com/)

## Prerequisites

- PostgreSQL (install via Homebrew or Postgres.app)
- pgAdmin (optional GUI for inspecting the database)
- Python 3.12 (for creating the virtual environment)
- Git

## Server setup (prepare database)

Install PostgreSQL and start the service.

In pgAdmin, right-click `Servers` → `Create` → `Server...`
On the `General` tab: set `Name` = `flowcase` (or any other name)
On the `Connection` tab:

- **Host:** `localhost`
- **Port:** `5432`
- **Maintenance DB:** `postgres` (or `flowcase`)
- **Username:** `myuser`
- **Password:** `mypassword`
  Click `Save`. You should now see the server and can expand databases/schemas.

## Required environment variables

The ETL relies on standard Postgres environment variables:

```txt
# .env (example)
PGHOST=localhost
PGPORT=5432
PGDATABASE=flowcase
PGUSER=myuser
PGPASSWORD=mypassword

FLOWCASE_DATA_SOURCE=fake 
# optional (real Flowcase API mode)
# FLOWCASE_DATA_SOURCE=real
FLOWCASE_SUBDOMAIN=your-subdomain
FLOWCASE_API_TOKEN=your_api_token
FLOWCASE_OFFICE_IDS=123,456
FLOWCASE_LANG_PARAMS=en,fr
```

Use a .env file or export variables before running the pipeline.

## Set-up and Running the Pipeline

Follow these steps from the repository root to create a virtual environment, install dependencies, and run the pipeline.

```bash
# from repo root
cd /path/to/repo_root

# create a virtual environment
python3 -m venv .venv

# or `python -m venv .venv` on windows

# activate (bash)
source .venv/bin/activate
# or `.venv\Scripts\activate` on windows

# upgrade pip and install project dependencies
pip install --upgrade pip # or `python -m pip install --upgrade pip` on windows
pip install -r etl_pipeline/flowcase_etl/requirements.txt #or pip install -r etl_pipeline/flowcase_etl/requirements.txt` on windows

# verify dependency integrity
pip check

# run the ETL in fake-data mode (creates Q#### CSVs and runs pipeline)
PYTHONPATH=flowcase_etl/src python -m flowcase_etl_pipeline.cli --generate-fake

# on windows
# `cd etl_pipeline`
# `set PYTHONPATH=flowcase_etl/src`
# `python -m flowcase_etl_pipeline.cli --generate-fake`

# when finished, deactivate the venv
deactivate
```

## Pipeline Stages

1 Acquire data

- Fake mode: generate cv_reports/Q#### synthetic exports
- Real mode: download CSVs using the Flowcase API client

2 Database setup

- Ensure database exists
- Apply schema from src/sql/\*.sql (idempotent)

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

- Refresh cv_search_profile_mv

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
│   │   ├── fake_data.py     # Wrapper for running the make_flowcase_reports.py
│   ├── sql/                 # Schema, indexes, materialized views
│   └── tests/               # Unit + integration tests
└── cv_reports/              # Fake or real Flowcase exports (Q####)
└── make_fake_flowcase_reports.py
└── requirements.txt
└── .env
```

Additional:

`early_experimentation` — early notebooks and prototypes (not used in production)

## Running Tests

To run all unit and integration tests with coverage and HTML reports, use the following commands from the repository root:

**For bash/Linux/macOS:**

```bash
# Load test environment variables
set -a && source etl_pipeline/flowcase_etl/.env.test && set +a

# Run tests with coverage and reports
PYTHONPATH=etl_pipeline/flowcase_etl/src .venv/bin/pytest etl_pipeline/flowcase_etl/src/tests \
  --junitxml=reports/junit.xml --html=reports/report.html \
  --cov=flowcase_etl_pipeline --cov-report=term-missing \
  --cov-report=html:reports/coverage_html \
  --cov-report=xml:reports/coverage.xml
```

**For Windows PowerShell:**

```powershell
# Set environment variable and run tests
$env:PYTHONPATH="etl_pipeline/flowcase_etl/src"
.venv\Scripts\python -m pytest etl_pipeline\flowcase_etl\src\tests --junitxml=reports\junit.xml --html=reports\report.html --cov=flowcase_etl_pipeline --cov-report=term-missing --cov-report=html:reports\coverage_html --cov-report=xml:reports\coverage.xml
```

- `.env.test` contains test-specific environment variables.
- Test results and coverage reports will be saved in the `reports/` directory.

## Scheduling & Automation

### Manual

- drop the latest `Q####` export into `cv_reports/` and run the CLI.

### Airflow automation (docker-compose)

#### Prereqs

- Docker Desktop installed.
- Repo checked out; working dir: `ETL_pipeline/airflow`.

#### 1 Environment

- Create an env file in `airflow/`:
  - Local Postgres (in containers): `cp .env.local .env`
  - Azure Postgres: `cp .env.azure .env`

```env
AIRFLOW_UID=501

# Database connection used by Airflow/ETL inside containers
PGHOST=postgres
PGPORT=5432
PGDATABASE=airflow
PGUSER=airflow
PGPASSWORD=airflow
FLOWCASE_DATA_SOURCE=fake

# Airflow admin user
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=password
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Security keys (generate once per machine; do NOT commit real values)
# Run this locally and copy the outputs below:
# python - <<'PY'
# from cryptography.fernet import Fernet
# import secrets
# print("AIRFLOW__CORE__FERNET_KEY=" + Fernet.generate_key().decode())
# print("AIRFLOW__WEBSERVER__SECRET_KEY=" + secrets.token_urlsafe(32))
# PY
AIRFLOW__CORE__FERNET_KEY=replace_me
AIRFLOW__WEBSERVER__SECRET_KEY=replace_me
```

##### How to switch Airflow between local and Azure

- In airflow/docker-compose.yaml, keep env_file: .env but swap the file:
  - For local DB: cp airflow/.env.local airflow/.env
  - For Azure: cp airflow/.env.azure airflow/.env

#### 2 Build custom Airflow image with ETL dependencies

```Dockerfile
airflow/Dockerfile:FROM apache/airflow:2.11.0
COPY ../flowcase_etl/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
--constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.0/constraints-3.11.txt
```

- `airflow/docker-compose.yaml` already set to build with:

```yaml
build:
  context: ..
  dockerfile: airflow/Dockerfile
```

and mounts `../flowcase_etl/src` and `../flowcase_etl/cv_reports`.

#### 3 Ports

- Postgres exposed on host port 5434 (`ports: ["5434:5432"]`).
- Airflow UI on 8080.

#### 4 Full flow

```bash
cd airflow
cp .env.local .env # or cp .env.azure .env
docker compose down -v       # wipe old DB/volumes
docker compose build         # build image with ETL dependencies
docker compose up airflow-init
docker compose up -d
```

#### 5 Airflow UI

- Open [http://localhost:8080](http://localhost:8080)
- Login: username/password set in .env
- Trigger DAG: `flowcase_etl_quarterly`

#### 6 pgAdmin connection (host)

- Host: `localhost`
- Port: `5434`
- DB: `airflow`
- User: `airflow`
- Password: `airflow`
- Refresh Schemas → public → Tables

## Notes

- Real Flowcase exports have identical filenames/columns, so they can replace fake reports seamlessly.
- Keep .env files and secrets out of version control.
- The ETL is idempotent: schema application and upserts are safe to re-run.
- Designed for extension → additional report types and sources (e.g., BambooHR, Kantata) can be added by implementing new transform/load modules.

========================================

## SMART-ASSIGN

Smart-Assign is a separate repo that can be run to work alongside the ETL_Pipeline and demonstrates and visualises the ETL database in an Angular UI with Python backend.

========================================

## Set up and run the application

Clone the repo
`https://github.com/AlanaBF/smart-assign.git`

### Set up and run the backend

1 Create and activate a virtual environment

- `python -m venv .venv`
- `source .venv/bin/activate`

2 Install dependencies

- `pip install -r requirements.txt`

3 Configure database access

- Copy the example config or create a config.ini file in the backend/ folder with a [postgres] section. Example:

  ```text
  [postgres]
  username = myuser
  password = mypassword
  host = localhost
  port = 5432
  database = flowcase
  ```

The backend code reads config.ini via services.db.\_read_config() so ensure the file is readable by the running process.

4 Run the app (development)

- `source .venv/bin/activate`
- `uvicorn main:app --reload`

### Set up and run the frontend

1 Install dependencies:

- `cd frontend/smart-assign`
- `npm install`

2 Run the development server (opens at [http://localhost:4200](http://localhost:4200) by default):

- `npm start`

## License & contact

- This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
- Questions or access issues: contact the repository owner.

## Questions

Have questions? Get in touch:

- [![Email](https://img.shields.io/badge/Email-alanabarrett--frew%40hotmail.com-0052CC?style=flat&logo=gmail&logoColor=white)](mailto:alanabarrett-frew@hotmail.com)

- [![GitHub](https://img.shields.io/badge/GitHub-@AlanaBF-181717?style=flat&logo=github&logoColor=white)](https://github.com/AlanaBF)

- [![LinkedIn](https://img.shields.io/badge/LinkedIn-Alana%20Barrett%20Frew-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/alanabarrettfrew/)
