# Smart-Assign

## Overview

Smart-Assign is a small ETL demo for the Smart Assign MVP. It:

1. Generates **synthetic CV Partner–style reports** (Flowcase exports)
2. **Extracts** raw CSV files from the latest export folder
3. **Transforms** them into a clean, relational schema
4. **Loads** them into PostgreSQL
5. Runs a few **sanity checks** to prove the pipeline works end-to-end

The whole flow is implemented in a single, step-by-step Jupyter notebook:

- `data_engineering_ETL_pipeline.ipynb`

and a set of SQL schema files:

- `sql/*.sql`

---

## Prerequisites

- **Python**: 3.11+ (virtual environment strongly recommended)
- **PostgreSQL**: 14+ running locally  
  - A user with permission to `CREATE DATABASE`
- **Client tools** (optional, but useful):
  - `psql` or **pgAdmin** for ad-hoc queries

---

## Setup Instructions

1. **Clone the repository**

   ```bash
   git clone <your-repo-url>
   cd smart-assign
   ```

2. **Create & activate a virtual environment**

   ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    # On Windows:
    # .venv\Scripts\activate
    ```

3. **Install dependencies**

    ```bash
    pip install -r requirements.txt
    ```

4. **Configure database**

    Create a db_config.txt file in the project root:

    ```text
    host=localhost
    port=5432
    database=cv_demo
    user=postgres
    password=postgres
    ```

## Running the ETL Notebook

The main end-to-end flow lives in:

- **`data_engineering_ETL_pipeline.ipynb`**

The notebook is structured as a clear, narrative walkthrough and includes small tests throughout.

---

### 1. Open the notebook

- Open the notebook in **VS Code**, **JupyterLab**, or your preferred environment.
- Select the **Python kernel** associated with your `.venv`.

---

### 2. Run the cells in order

The notebook is organised into the following sections:

---

### **Environment Setup**

Initial imports and utility functions.

---

### **Step 0 — Generate Synthetic Data**

- Calls `make_fake_flowcase_reports.main()`
- Generates timestamped folders under `cv_reports/`

---

### **Step 1 — Extract**

- Locates the **latest** report folder
- Loads all CSVs into pandas DataFrames
- Performs small sanity checks (e.g., ensure `user_report.csv` exists)

---

### **Step 2 — Transform**

- Builds clean tables:  
  `users_df`, `cvs_df`, skills, languages, experiences, etc.
- Normalises multilang fields and date formats
- Runs basic integrity checks:
  - `users_df` is not empty
  - `"CV Partner User ID"` exists and has non-null values
  - `len(users_df) == len(cvs_df)` (1 user ↔ 1 CV)

---

### **Step 3 — Load**

Upserts into PostgreSQL tables:

- `users`, `cvs`
- `dim_technology`, `cv_technology`
- `cv_language`, `project_experience`, `work_experience`
- `education`, `course`, `position`, `blog_publication`
- `cv_role`, `key_qualification`
- `dim_clearance`, `user_clearance`, `user_availability`

All load steps run inside a **single transaction** via:

```python
load(tr, engine)
```

### Step 4 — Database Setup

The notebook prepares the PostgreSQL environment before loading data:

- Builds database settings using **`compose_settings()`**
- Ensures the target database exists with **`ensure_database_exists()`**
- Applies all SQL schema files from `sql/*.sql` using  
  **`setup_database_schema_if_needed()`**

---

### Step 5 — Run Full ETL Pipeline

This final stage executes the complete workflow:

- Runs **Extract → Transform → Load** in sequence  
- Prints a success message when the ETL completes successfully  
- (Optional) You can manually verify key tables using SQL, for example:

  ```sql
  SELECT COUNT(*) FROM users;
  SELECT COUNT(*) FROM cvs;
  SELECT COUNT(*) FROM cv_technology;
  ```

### Re-running the Notebook

You can safely run the entire notebook multiple times:

- New synthetic data is generated on each run  
- The database is fully reloaded  
- All loads use **upserts**, ensuring idempotency (no duplication)

This makes the pipeline ideal for:

- Demonstrations  
- Debugging  
- Development iteration  
- Repeated testing with fresh datasets  

No manual cleanup is required between runs.
