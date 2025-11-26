"""
Configuration helpers.

Uses environment variables:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
load_dotenv() 

DataSource = Literal["fake", "real"]

@dataclass
class FlowcaseConfig:
    subdomain: str
    api_token: str
    office_ids: list[str] | None
    lang_params: list[str] | None
    
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
    root: Path
    db: DbConfig
    data_source: DataSource
    cv_reports_dir: Path
    sql_dir: Path
    flowcase: FlowcaseConfig | None

    @classmethod
    def load(cls) -> "Settings":
        root = Path(__file__).resolve().parents[2]

        data_source = os.getenv("FLOWCASE_DATA_SOURCE", "fake").lower()
        if data_source not in ("fake", "real"):
            raise ValueError(f"Invalid FLOWCASE_DATA_SOURCE: {data_source}")

        # If real: load Flowcase secrets from .env
        flowcase_cfg = None
        if data_source == "real":
            flowcase_cfg = FlowcaseConfig(
                subdomain=os.environ["FLOWCASE_SUBDOMAIN"],
                api_token=os.environ["FLOWCASE_API_TOKEN"],
                office_ids=(
                    os.getenv("FLOWCASE_OFFICE_IDS", "").split(",")
                    if os.getenv("FLOWCASE_OFFICE_IDS")
                    else None
                ),
                lang_params=(
                    os.getenv("FLOWCASE_LANG_PARAMS", "").split(",")
                    if os.getenv("FLOWCASE_LANG_PARAMS")
                    else None
                ),
            )

        return cls(
            root=root,
            db=DbConfig.from_env(),
            data_source=data_source,
            cv_reports_dir=root / "cv_reports",
            sql_dir=root / "src" / "sql",
            flowcase=flowcase_cfg,
        )
__all__ = ["DbConfig", "Settings"]
