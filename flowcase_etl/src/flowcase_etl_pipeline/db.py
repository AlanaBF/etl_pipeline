"""
Database helpers: create the engine, optionally create the database, and apply
all SQL files in a folder.
"""

import logging
from pathlib import Path

import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import DbConfig

logger = logging.getLogger(__name__)

def create_database_if_missing(cfg: DbConfig) -> None:
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port=cfg.port,
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg.database,))
        if not cur.fetchone():
            logger.info("Creating database %s", cfg.database)
            cur.execute(f'CREATE DATABASE "{cfg.database}"')
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning("Could not verify/create database: %s", exc)

      
def get_engine(cfg: DbConfig) -> Engine:
    url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.database}"
    return create_engine(url)

def apply_sql_folder(engine: Engine, sql_folder: Path) -> None:
    if not sql_folder.exists():
        logger.info("SQL folder %s not found; skipping schema setup.", sql_folder)
        return
    sql_files = sorted(sql_folder.glob("*.sql"))
    if not sql_files:
        logger.info("No .sql files in %s; nothing to apply.", sql_folder)
        return
    with engine.begin() as conn:
        for path in sql_files:
            logger.info("Applying %s", path.name)
            conn.execute(text(path.read_text()))

__all__ = ["create_database_if_missing", "get_engine", "apply_sql_folder"]
