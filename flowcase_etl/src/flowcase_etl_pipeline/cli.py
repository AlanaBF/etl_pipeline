"""
Command-line entrypoint for the Flowcase ETL.

Steps:
1) (Optional) generate fake Flowcase-style reports (if data_source=fake)
2) If data_source=real, download CV reports from Flowcase API
3) Ensure the database exists and schema is applied
4) Extract -> Transform -> Load
5) (Optional) refresh the materialized view
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from .config import Settings
from .db import apply_sql_folder, create_database_if_missing, get_engine
from .extract import extract
from .load import load
from .transform import transform
from . import fake_data
from .flowcase_client import FlowcaseClient


logger = logging.getLogger(__name__)


def run_etl(
    generate_fake: bool,
    refresh_mv: bool,
    data_folder: Path | None,
    sql_folder: Path | None,
) -> None:
    settings = Settings.load()

    # Decide where the CSVs live and where SQL lives
    base_data_dir = data_folder if data_folder else settings.cv_reports_dir
    sql_dir = sql_folder if sql_folder else settings.sql_dir

    # ------------------------------------------------------------------
    # 1. Acquire data (fake OR real)
    # ------------------------------------------------------------------
    if settings.data_source == "real":
        if settings.flowcase is None:
            raise RuntimeError(
                "FLOWCASE_DATA_SOURCE=real but Flowcase config is missing. "
                "Check FLOWCASE_SUBDOMAIN and FLOWCASE_API_TOKEN in your .env."
            )

        logger.info("Using REAL Flowcase data source.")
        client = FlowcaseClient(settings.flowcase)

        # Put each real download in a timestamped subfolder
        ts = datetime.utcnow().strftime("QREAL_%Y%m%d_%H%M%S")
        data_dir = base_data_dir / ts
        logger.info("Downloading Flowcase reports into %s", data_dir)

        client.fetch_all_reports(output_dir=data_dir)
        extract_base_folder = data_dir

    else:
        logger.info("Using FAKE data source.")
        data_dir = base_data_dir

        if generate_fake:
            logger.info("Generating fake Flowcase reports using make_fake_flowcase_reports.py")
            fake_data.main()
        else:
            logger.info("Re-using existing fake reports in %s", data_dir)

        extract_base_folder = data_dir

    # ------------------------------------------------------------------
    # 2. DB setup
    # ------------------------------------------------------------------
    create_database_if_missing(settings.db)
    engine = get_engine(settings.db)
    apply_sql_folder(engine, sql_dir)

    # ------------------------------------------------------------------
    # 3. ETL: extract -> transform -> load
    # ------------------------------------------------------------------
    extract_result = extract(
        {
            "data_source": settings.data_source,
            "base_folder": extract_base_folder,
        }
    )
    transform_result = transform(extract_result.frames)
    load(transform_result, engine)

    # ------------------------------------------------------------------
    # 4. Optional: refresh MV
    # ------------------------------------------------------------------
    if refresh_mv:
        logger.info("Refreshing materialized view cv_search_profile_mv")
        try:
            with engine.begin() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW cv_search_profile_mv;"))
        except Exception as exc:
            logger.warning("Could not refresh cv_search_profile_mv: %s", exc)

    # ------------------------------------------------------------------
    # 5. Simple KPIs to prove business value
    # ------------------------------------------------------------------
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
            avg_availability or 0.0,
        )
    except Exception as exc:
        logger.warning("Post-load KPI query failed: %s", exc)

    logger.info("All done.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Flowcase ETL pipeline.")
    parser.add_argument(
        "--generate-fake",
        action="store_true",
        help="(FAKE mode only) Generate fake Flowcase reports before running ETL.",
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
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