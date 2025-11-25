"""
Transform step.

Clean the raw CSV DataFrames into shapes ready for loading:
- Parse multilang text fields into dicts
- Build CV rows from user data
- Normalise dates and numbers
- Pass through section tables untouched
- Run simple data-quality checks (row counts, required columns)
"""

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

@dataclass
class TransformResult:
    users_df: pd.DataFrame | None = None
    cvs_df: pd.DataFrame | None = None
    technologies_df: Optional[pd.DataFrame] = None
    languages_df: Optional[pd.DataFrame] = None
    project_experiences_df: Optional[pd.DataFrame] = None
    work_experiences_df: Optional[pd.DataFrame] = None
    certifications_df: Optional[pd.DataFrame] = None
    courses_df: Optional[pd.DataFrame] = None
    educations_df: Optional[pd.DataFrame] = None
    positions_df: Optional[pd.DataFrame] = None
    blogs_df: Optional[pd.DataFrame] = None
    cv_roles_df: Optional[pd.DataFrame] = None
    key_qualifications_df: Optional[pd.DataFrame] = None
    sc_clearance_df: Optional[pd.DataFrame] = None
    availability_df: Optional[pd.DataFrame] = None

def parse_multilang(pipe: object) -> dict:
    """
    Convert a single pipe string like 'int:Text|no:Tekst' into a dict.
    Anything non-string or blank -> {}.
    """
    if not isinstance(pipe, str) or not pipe.strip():
        return {}
    out = {}
    for part in pipe.split("|"):
        if ":" in part:
            k, v = part.split(":", 1)
            k, v = k.strip(), v.strip()
            if k and v:
                out[k] = v
    return out

def to_iso_date(s: object) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    if not s:
        return None
    # If it looks like ISO (yyyy-mm-dd), parse straight without dayfirst
    if "-" in s and len(s.split("-")[0]) == 4:
        dt = pd.to_datetime(s, errors="coerce")
    else:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return None if pd.isna(dt) else dt.date().isoformat()

def transform(raw: dict[str, pd.DataFrame]) -> TransformResult:
    logger.info("Starting transform step")
    users = raw.get("user_report.csv", pd.DataFrame()).copy()
    usage = raw.get("usage_report.csv", pd.DataFrame()).copy()

    if not users.empty and "Name (multilang)" in users.columns:
        users["Name (multilang)"] = users["Name (multilang)"].map(parse_multilang)
    else:
        users["Name (multilang)"] = [{}] * len(users)

    if not usage.empty and "Nationality (#{lang})" in usage.columns:
        nat_map = {
            str(r["CV Partner User ID"]): parse_multilang(r["Nationality (#{lang})"])
            for _, r in usage.iterrows()
            if "CV Partner User ID" in r and pd.notna(r["CV Partner User ID"])
        }
        users["nationality_multilang"] = users["CV Partner User ID"].map(
            lambda uid: nat_map.get(str(uid), {})
        )
    else:
        users["nationality_multilang"] = [{}] * len(users)

    cvs = users.copy()
    if "Title (#{lang})" in users.columns:
        cvs["title_multilang"] = users["Title (#{lang})"].map(parse_multilang)
    else:
        cvs["title_multilang"] = [{}] * len(cvs)

    def _num(x):
        try:
            return int(x)
        except Exception:
            return None

    cvs["sfia_level"] = users.get("SFIA Level", pd.Series([None]*len(users))).map(_num)
    cvs["cpd_level"]  = users.get("CPD Level",  pd.Series([None]*len(users))).map(_num)
    cvs["cpd_band"]   = users.get("CPD Band",   pd.Series([None]*len(users))).astype("string").where(lambda s: s.notna(), None)
    cvs["cpd_label"]  = users.get("CPD Label",  pd.Series([None]*len(users))).astype("string").where(lambda s: s.notna(), None)

    sc_clearance = raw.get("sc_clearance.csv", pd.DataFrame()).copy()
    if not sc_clearance.empty:
        for col in ("Valid From", "Valid To"):
            if col in sc_clearance.columns:
                sc_clearance[col] = sc_clearance[col].map(to_iso_date)
        logger.info("Processed sc_clearance.csv: %d rows", len(sc_clearance))

    availability = raw.get("availability_report.csv", pd.DataFrame()).copy()
    logger.info("users rows=%d, cvs rows=%d", len(users), len(cvs))
    assert not users.empty, "users_df is unexpectedly empty"
    assert "CV Partner User ID" in users.columns, "Missing CV Partner User ID"
    assert len(users) == len(cvs), "users_df and cvs_df row counts differ"

    if not availability.empty and "Date" in availability.columns:
        availability["Date"] = availability["Date"].map(to_iso_date)
        logger.info("Processed availability_report.csv: %d rows", len(availability))

    return TransformResult(
        users_df=users if not users.empty else pd.DataFrame(),
        cvs_df=cvs if not cvs.empty else pd.DataFrame(),
        technologies_df=raw.get("technologies.csv"),
        languages_df=raw.get("languages.csv"),
        project_experiences_df=raw.get("project_experiences.csv"),
        work_experiences_df=raw.get("work_experiences.csv"),
        certifications_df=raw.get("certifications.csv"),
        courses_df=raw.get("courses.csv"),
        educations_df=raw.get("educations.csv"),
        positions_df=raw.get("positions.csv"),
        blogs_df=raw.get("blogs.csv"),
        cv_roles_df=raw.get("cv_roles.csv"),
        key_qualifications_df=raw.get("key_qualifications.csv"),
        sc_clearance_df=sc_clearance if not sc_clearance.empty else pd.DataFrame(),
        availability_df=availability if not availability.empty else pd.DataFrame(),
    )
    
__all__ = [
    "TransformResult",
    "transform",
    'parse_multilang',
    'to_iso_date',
]
