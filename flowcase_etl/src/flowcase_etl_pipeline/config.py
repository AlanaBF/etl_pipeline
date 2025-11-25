"""
Configuration helpers.

Uses environment variables:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv() 

@dataclass
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DbConfig":
        required = {
            "host": os.getenv("PGHOST"),
            "port": os.getenv("PGPORT"),
            "database": os.getenv("PGDATABASE"),
            "user": os.getenv("PGUSER"),
            "password": os.getenv("PGPASSWORD"),
        }
        missing = [key for key, value in required.items() if value is None]
        if missing:
            raise ValueError(f"Missing required database environment variables: {', '.join(missing)}")

        try:
            required["port"] = int(required["port"])
        except ValueError:
            raise ValueError("PGPORT must be an integer")

        return cls(
            host=required["host"],  
            port=required["port"],
            database=required["database"],
            user=required["user"],
            password=required["password"],
        )


@dataclass
class Settings:
    base_folder: Path
    sql_folder: Path
    db: DbConfig
    data_source: str = "fake"  # or "real"

    @classmethod
    def load(cls, project_root: Optional[Path] = None) -> "Settings":
        root = project_root or Path(__file__).resolve().parents[2]
        base_folder = root / "cv_reports"
        sql_folder = root / "src" / "sql"
        db = DbConfig.from_env()
        return cls(base_folder=base_folder, sql_folder=sql_folder, db=db)

__all__ = ["DbConfig", "Settings"]
