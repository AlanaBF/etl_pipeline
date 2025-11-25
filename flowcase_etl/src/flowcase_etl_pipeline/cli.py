"""
Command-line entrypoint for the Flowcase ETL.

Steps:
1) Generate fake Flowcase-style reports
2) Ensure the database exists and schema is applied
3) Extract -> Transform -> Load
4) Refresh the materialized view

Required environment variables for the database:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import argparse
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import text

from .config import Settings
from .db import apply_sql_folder, create_database_if_missing, get_engine
from .extract import extract
from .load import load
from .transform import transform
from . import fake_data

logger = logging.getLogger(__name__)


def run_etl(generate_fake: bool, refresh_mv: bool, data_folder: Path | None, sql_folder: Path | None) -> None:
    settings = Settings.load()
    if data_folder:
        settings.base_folder = data_folder
    if sql_folder:
        settings.sql_folder = sql_folder

    if generate_fake:
        try:
            if generate_fake:
                fake_data.main()
            logger.info("Generated fake Flowcase reports.")
        except Exception as exc:
            logger.error("Failed to generate fake reports: %s", exc)
            raise

    create_database_if_missing(settings.db)
    engine = get_engine(settings.db)
    apply_sql_folder(engine, settings.sql_folder)

    # ETL
    extract_result = extract({"data_source": settings.data_source, "base_folder": settings.base_folder})
    transform_result = transform(extract_result.frames)
    load(transform_result, engine)

    if refresh_mv:
        logger.info("Refreshing materialized view cv_search_profile_mv")
        try:
            with engine.begin() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW cv_search_profile_mv;"))
        except Exception as exc:
            logger.warning("Could not refresh cv_search_profile_mv: %s", exc)

    try:
        with engine.connect() as conn:
            users_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
            cvs_count = conn.execute(text("SELECT COUNT(*) FROM cvs")).scalar()
            top_skills = conn.execute(
                text(
                    """
                    SELECT dt.name, COUNT(*) AS cnt
                    FROM cv_technology ct
                    JOIN dim_technology dt ON dt.technology_id = ct.technology_id
                    GROUP BY dt.name
                    ORDER BY cnt DESC
                    LIMIT 5;
                    """
                )
            ).fetchall()
            sc_available = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM cv_search_profile_mv
                    WHERE (clearance = 'SC') AND latest_percent_available >= 50;
                    """
                )
            ).scalar()
            avg_availability = conn.execute(
                text("SELECT AVG(percent_available) FROM user_availability")
            ).scalar()
        logger.info(
            "KPI: users=%s, cvs=%s, top skills=%s, SC>=50%%=%s, avg availability=%.2f",
            users_count,
            cvs_count,
            top_skills,
            sc_available,
            avg_availability or 0,
        )
    except Exception as exc:
        logger.warning("Post-load KPI query failed: %s", exc)

    logger.info("All done.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Flowcase ETL pipeline.")
    parser.add_argument(
        "--generate-fake",
        action="store_true",
        help="Generate fake Flowcase reports before running ETL.",
    )
    parser.add_argument(
        "--data-folder",
        default=None,
        help="Path to cv_reports folder (default: repo_root/cv_reports).",
    )
    parser.add_argument(
        "--sql-folder",
        default=None,
        help="Path to SQL folder (default: repo_root/src/sql).",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing the materialized view after load.",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    run_etl(
        generate_fake=args.generate_fake,
        refresh_mv=not args.skip_refresh,
        data_folder=Path(args.data_folder) if args.data_folder else None,
        sql_folder=Path(args.sql_folder) if args.sql_folder else None,
    )


if __name__ == "__main__":
    main()
