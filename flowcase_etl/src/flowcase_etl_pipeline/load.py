"""
Load step.

Upsert cleaned DataFrames into PostgreSQL. This keeps the row-by-row logic from
the notebook so it is easy to follow. You can add logging later once this is
error-free.
"""

import json
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)

def _to_bool(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    text_value = str(value).strip().lower()
    return text_value in ("true", "1", "t", "yes", "y")


def _clean_str(value, default=""):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    text_value = str(value).strip()
    return text_value if text_value else default


def _to_date(value, default=None):
    if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == "":
        return default
    text_value = str(value).strip()
    # If looks like ISO yyyy-mm-dd, parse without dayfirst to avoid warnings
    dayfirst = False if len(text_value) == 10 and text_value[4] == "-" else True
    parsed = pd.to_datetime(text_value, dayfirst=dayfirst, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _resolve_user_id(conn, email=None, upn=None, external_id=None):
    if email:
        uid = conn.execute(text("SELECT user_id FROM users WHERE lower(email)=lower(:e)"), {"e": email}).scalar()
        if uid:
            return uid
    if upn:
        uid = conn.execute(text("SELECT user_id FROM users WHERE lower(upn)=lower(:u)"), {"u": upn}).scalar()
        if uid:
            return uid
    if external_id:
        uid = conn.execute(text("SELECT user_id FROM users WHERE external_user_id=:x"), {"x": external_id}).scalar()
        if uid:
            return uid
    return None


def _cv_id(conn, cv_partner_cv_id: str):
    return conn.execute(
        text("SELECT cv_id FROM cvs WHERE cv_partner_cv_id=:cid"),
        {"cid": str(cv_partner_cv_id)},
    ).scalar()


def _ensure_dim(conn, table: str, name: Optional[str], key: str = "name", id_col: str = None):
    if not name:
        return None
    if id_col is None:
        id_col = (table[4:] + "_id") if table.startswith("dim_") else (table.rstrip("s") + "_id")
    conn.execute(text(f"INSERT INTO {table} ({key}) VALUES (:n) ON CONFLICT ({key}) DO NOTHING"), {"n": name})
    return conn.execute(text(f"SELECT {id_col} FROM {table} WHERE {key}=:n"), {"n": name}).scalar()


# ---------- Upsert functions ----------

def upsert_users(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} users.")
    sql = text(
        """
        INSERT INTO users
          (cv_partner_user_id, name_multilang, email, upn, external_user_id,
           phone_number, landline, birth_year, department, country,
           user_created_at, nationality_multilang)
        VALUES
          (:cv_partner_user_id, CAST(:name_multilang AS JSONB), :email, :upn, :external_user_id,
           :phone_number, :landline, :birth_year, :department, :country,
           :user_created_at, CAST(:nationality_multilang AS JSONB))
        ON CONFLICT (cv_partner_user_id) DO UPDATE
        SET name_multilang = EXCLUDED.name_multilang,
            email = EXCLUDED.email,
            upn = EXCLUDED.upn,
            external_user_id = EXCLUDED.external_user_id,
            phone_number = EXCLUDED.phone_number,
            landline = EXCLUDED.landline,
            birth_year = EXCLUDED.birth_year,
            department = EXCLUDED.department,
            country = EXCLUDED.country,
            user_created_at = EXCLUDED.user_created_at,
            nationality_multilang = EXCLUDED.nationality_multilang
    """
    )
    for _, row in df.iterrows():
        conn.execute(
            sql,
            {
                "cv_partner_user_id": str(row["CV Partner User ID"]),
                "name_multilang": json.dumps(row["Name (multilang)"]),
                "email": row.get("Email"),
                "upn": row.get("UPN"),
                "external_user_id": row.get("External User ID"),
                "phone_number": row.get("Phone Number"),
                "landline": row.get("Landline"),
                "birth_year": int(row["Birth Year"]) if pd.notna(row.get("Birth Year")) else None,
                "department": row.get("Department"),
                "country": row.get("Country"),
                "user_created_at": row.get("User created at"),
                "nationality_multilang": json.dumps(row.get("nationality_multilang", {})),
            },
        )


def upsert_cvs(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} CVs...")
    sql = text(
        """
        INSERT INTO cvs
          (cv_partner_cv_id, user_id, title_multilang, years_of_education,
           years_since_first_work_experience, has_profile_image,
           owns_reference_project, read_privacy_notice,
           cv_last_updated_by_owner, cv_last_updated,
           sfia_level, cpd_level, cpd_band, cpd_label)
        VALUES
          (:cv_partner_cv_id, :user_id, CAST(:title_multilang AS JSONB), :yoe, :ysfwe,
           :has_img, :owns_ref, :read_priv, :lu_owner, :lu,
           :sfia_level, :cpd_level, :cpd_band, :cpd_label)
        ON CONFLICT (cv_partner_cv_id) DO UPDATE
        SET title_multilang = EXCLUDED.title_multilang,
            years_of_education = EXCLUDED.years_of_education,
            years_since_first_work_experience = EXCLUDED.years_since_first_work_experience,
            has_profile_image = EXCLUDED.has_profile_image,
            owns_reference_project = EXCLUDED.owns_reference_project,
            read_privacy_notice = EXCLUDED.read_privacy_notice,
            cv_last_updated_by_owner = EXCLUDED.cv_last_updated_by_owner,
            cv_last_updated = EXCLUDED.cv_last_updated,
            sfia_level = EXCLUDED.sfia_level,
            cpd_level  = EXCLUDED.cpd_level,
            cpd_band   = EXCLUDED.cpd_band,
            cpd_label  = EXCLUDED.cpd_label
    """
    )
    for _, row in df.iterrows():
        uid = conn.execute(
            text("SELECT user_id FROM users WHERE cv_partner_user_id=:uid"),
            {"uid": str(row["CV Partner User ID"])},
        ).scalar()
        if uid is None:
            logger.warning(f"Skipping CV {row['CV Partner CV ID']} (unknown user {row['CV Partner User ID']})")
            continue

        conn.execute(
            sql,
            {
                "cv_partner_cv_id": str(row["CV Partner CV ID"]),
                "user_id": uid,
                "title_multilang": json.dumps(row["title_multilang"]),
                "yoe": int(row["Years of education"]) if pd.notna(row["Years of education"]) else None,
                "ysfwe": int(row["Years since first work experience"]) if pd.notna(row["Years since first work experience"]) else None,
                "has_img": _to_bool(row["Has profile image"]),
                "owns_ref": _to_bool(row["Owns a reference project"]),
                "read_priv": _to_bool(row["Read and understood privacy notice"]),
                "lu_owner": row["CV Last updated by owner"],
                "lu": row["CV Last updated"],
                "sfia_level": row.get("sfia_level"),
                "cpd_level": row.get("cpd_level"),
                "cpd_band": None if pd.isna(row.get("cpd_band")) else str(row.get("cpd_band")),
                "cpd_label": None if pd.isna(row.get("cpd_label")) else str(row.get("cpd_label")),
            },
        )


def upsert_technologies(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} technologies...")
    for _, row in df.iterrows():
        tech_name = row["Skill name"]
        conn.execute(
            text(
                """
            INSERT INTO dim_technology (name)
            VALUES (:name)
            ON CONFLICT (name) DO NOTHING
        """
            ),
            {"name": tech_name},
        )

        tech_id = conn.execute(text("SELECT technology_id FROM dim_technology WHERE name=:n"), {"n": tech_name}).scalar()
        if tech_id is None:
            logger.warning(f"Skipping tech link; cannot resolve technology '{tech_name}'")
            continue

        cv_id = conn.execute(text("SELECT cv_id FROM cvs WHERE cv_partner_cv_id=:cid"), {"cid": str(row["CV Partner CV ID"])}).scalar()
        if cv_id is None:
            logger.warning(f"Skipping tech link; unknown CV {row['CV Partner CV ID']}")
            continue

        conn.execute(
            text(
                """
            INSERT INTO cv_technology (cv_id, technology_id, years_experience, proficiency, is_official_masterdata)
            VALUES (:cv, :tech, :yexp, :prof, CAST(:is_md AS JSONB))
            ON CONFLICT (cv_id, technology_id) DO UPDATE
            SET years_experience = EXCLUDED.years_experience,
                proficiency = EXCLUDED.proficiency,
                is_official_masterdata = EXCLUDED.is_official_masterdata
        """
            ),
            {
                "cv": cv_id,
                "tech": tech_id,
                "yexp": int(row["Year experience"]) if pd.notna(row["Year experience"]) else None,
                "prof": int(row["Proficiency (0-5)"]) if pd.notna(row["Proficiency (0-5)"]) else None,
                "is_md": json.dumps(row["Is official masterdata (in #{lang})"]),
            },
        )


def upsert_languages(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} languages...")
    sql = text(
        """
        INSERT INTO cv_language
          (cv_id, language_id, level, highlighted, is_official_masterdata, updated, updated_by_owner)
        VALUES
          (:cv_id, :lang_id, :level, :highlighted, CAST(:is_md AS JSONB), :updated, :updated_by_owner)
        ON CONFLICT (cv_id, language_id) DO UPDATE
        SET level = EXCLUDED.level,
            highlighted = EXCLUDED.highlighted,
            is_official_masterdata = EXCLUDED.is_official_masterdata,
            updated = EXCLUDED.updated,
            updated_by_owner = EXCLUDED.updated_by_owner
    """
    )
    for _, row in df.iterrows():
        cv_id = _cv_id(conn, row["CV Partner CV ID"])
        if not cv_id:
            continue
        lang_id = _ensure_dim(conn, "dim_language", row.get("Language"))
        conn.execute(
            sql,
            {
                "cv_id": cv_id,
                "lang_id": lang_id,
                "level": row.get("Level"),
                "highlighted": _to_bool(row.get("Highlighted")),
                "is_md": json.dumps(row.get("Is official masterdata (in #{lang})", {})),
                "updated": row.get("Updated"),
                "updated_by_owner": row.get("Updated by owner"),
            },
        )


def upsert_project_experiences(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} project experiences...")
    sql = text(
        """
      INSERT INTO project_experience
        (cv_id, cv_partner_section_id, external_unique_id,
         month_from, year_from, month_to, year_to,
         customer_int, customer_multilang,
         customer_anon_int, customer_anon_multilang,
         description_int, description_multilang,
         long_description_int, long_description_multilang,
         industry_id, project_type_id,
         percent_allocated, extent_individual_hours, extent_hours, extent_total_hours,
         extent_unit, extent_currency, extent_total, extent_total_currency,
         project_area, project_area_unit,
         highlighted, updated, updated_by_owner)
      VALUES
        (:cv_id, :sid, :ext_id,
         :m_from, :y_from, :m_to, :y_to,
         :cust_int, CAST(:cust_ml AS JSONB),
         :cust_anon_int, CAST(:cust_anon_ml AS JSONB),
         :desc_int, CAST(:desc_ml AS JSONB),
         :ldesc_int, CAST(:ldesc_ml AS JSONB),
         :industry_id, :project_type_id,
         :pct_alloc, :indiv_hours, :hours, :total_hours,
         :extent_unit, :extent_curr, :extent_total, :extent_total_curr,
         :proj_area, :proj_area_unit,
         :highlighted, :updated, :updated_by_owner)
      ON CONFLICT (cv_id, cv_partner_section_id) DO UPDATE
      SET external_unique_id = EXCLUDED.external_unique_id,
          month_from = EXCLUDED.month_from, year_from = EXCLUDED.year_from,
          month_to = EXCLUDED.month_to, year_to = EXCLUDED.year_to,
          customer_int = EXCLUDED.customer_int, customer_multilang = EXCLUDED.customer_multilang,
          customer_anon_int = EXCLUDED.customer_anon_int, customer_anon_multilang = EXCLUDED.customer_anon_multilang,
          description_int = EXCLUDED.description_int, description_multilang = EXCLUDED.description_multilang,
          long_description_int = EXCLUDED.long_description_int, long_description_multilang = EXCLUDED.long_description_multilang,
          industry_id = EXCLUDED.industry_id, project_type_id = EXCLUDED.project_type_id,
          percent_allocated = EXCLUDED.percent_allocated, extent_individual_hours = EXCLUDED.extent_individual_hours,
          extent_hours = EXCLUDED.extent_hours, extent_total_hours = EXCLUDED.extent_total_hours,
          extent_unit = EXCLUDED.extent_unit, extent_currency = EXCLUDED.extent_currency,
          extent_total = EXCLUDED.extent_total, extent_total_currency = EXCLUDED.extent_total_currency,
          project_area = EXCLUDED.project_area, project_area_unit = EXCLUDED.project_area_unit,
          highlighted = EXCLUDED.highlighted, updated = EXCLUDED.updated,
          updated_by_owner = EXCLUDED.updated_by_owner
    """
    )
    for _, row in df.iterrows():
        cv_id = _cv_id(conn, row["CV Partner CV ID"])
        if not cv_id:
            continue
        conn.execute(
            sql,
            {
                "cv_id": cv_id,
                "sid": row.get("CV Partner section ID"),
                "ext_id": row.get("External unique ID"),
                "m_from": row.get("Month from"),
                "y_from": row.get("Year from"),
                "m_to": row.get("Month to"),
                "y_to": row.get("Year to"),
                "cust_int": row.get("Customer (int)"),
                "cust_ml": json.dumps(row.get("Customer (#{lang})", {})),
                "cust_anon_int": row.get("Customer anonymous (int)"),
                "cust_anon_ml": json.dumps(row.get("Customer anonymous (#{lang})", {})),
                "desc_int": row.get("Description (int)"),
                "desc_ml": json.dumps(row.get("Description (#{lang})", {})),
                "ldesc_int": row.get("Long description (int)"),
                "ldesc_ml": json.dumps(row.get("Long description (#{lang})", {})),
                "industry_id": _ensure_dim(conn, "dim_industry", row.get("Industry")),
                "project_type_id": _ensure_dim(conn, "dim_project_type", row.get("Project type")),
                "pct_alloc": row.get("Percent allocated"),
                "indiv_hours": row.get("Extent individual hours"),
                "hours": row.get("Extent hours"),
                "total_hours": row.get("Extent total hours"),
                "extent_unit": row.get("Extent unit"),
                "extent_curr": row.get("Extent currency"),
                "extent_total": row.get("Extent total"),
                "extent_total_curr": row.get("Extent total currency"),
                "proj_area": row.get("Project area"),
                "proj_area_unit": row.get("Project area unit"),
                "highlighted": _to_bool(row.get("Highlighted")),
                "updated": row.get("Updated"),
                "updated_by_owner": row.get("Updated by owner"),
            },
        )


def upsert_section_table(conn, df: pd.DataFrame, table: str, fields: dict):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} rows into {table}...")
    # Always include cv_id as the first column
    column_names = ["cv_id"] + list(fields.keys())
    cols = ", ".join(column_names)
    col_params = ", ".join(f":{column}" for column in column_names)
    pk = "cv_partner_section_id" if "cv_partner_section_id" in fields.keys() else "name"
    sql = f"""
      INSERT INTO {table} ({cols})
      VALUES ({col_params})
      ON CONFLICT (cv_id, {pk}) DO UPDATE
      SET {", ".join(f"{col}=EXCLUDED.{col}" for col in fields.keys() if col != pk)}
    """
    payload = []
    for _, row in df.iterrows():
        cv_id = _cv_id(conn, row["CV Partner CV ID"])
        if cv_id is None:
            continue
        item = {db_col: row.get(csv_col) for db_col, csv_col in fields.items()}
        item["cv_id"] = cv_id
        payload.append(item)
    if payload:
        conn.execute(text(sql), payload)


def upsert_work_experiences(conn, df: pd.DataFrame):
    fields = {
        "cv_partner_section_id": "CV Partner section ID",
        "external_unique_id": "External unique ID",
        "month_from": "Month from",
        "year_from": "Year from",
        "month_to": "Month to",
        "year_to": "Year to",
        "highlighted": "Highlighted",
        "employer": "Employer",
        "description": "Description",
        "long_description": "Long description",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "work_experience", fields)


def upsert_certifications(conn, df: pd.DataFrame):
    fields = {
        "cv_partner_section_id": "CV Partner section ID",
        "external_unique_id": "External unique ID",
        "month": "Month",
        "year": "Year",
        "month_expire": "Month expire",
        "year_expire": "Year expire",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "certification", fields)


def upsert_courses(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} rows into course...")
    sql = text(
        """
        INSERT INTO course
          (cv_id, cv_partner_section_id, external_unique_id,
           month, year, name, organiser, long_description,
           highlighted, is_official_masterdata, attachments,
           updated, updated_by_owner)
        VALUES
          (:cv_id, :cv_partner_section_id, :external_unique_id,
           :month, :year, :name, :organiser, :long_description,
           :highlighted, CAST(:is_official_masterdata AS JSONB), :attachments,
           :updated, :updated_by_owner)
        ON CONFLICT (cv_id, cv_partner_section_id) DO UPDATE
        SET external_unique_id     = EXCLUDED.external_unique_id,
            month                  = EXCLUDED.month,
            year                   = EXCLUDED.year,
            name                   = EXCLUDED.name,
            organiser              = EXCLUDED.organiser,
            long_description       = EXCLUDED.long_description,
            highlighted            = EXCLUDED.highlighted,
            is_official_masterdata = EXCLUDED.is_official_masterdata,
            attachments            = EXCLUDED.attachments,
            updated                = EXCLUDED.updated,
            updated_by_owner       = EXCLUDED.updated_by_owner
        """
    )
    payload = []
    for _, row in df.iterrows():
        cv_id = _cv_id(conn, row["CV Partner CV ID"])
        if cv_id is None:
            continue
        payload.append(
            {
                "cv_id": cv_id,
                "cv_partner_section_id": row.get("CV Partner section ID"),
                "external_unique_id": row.get("External unique ID"),
                "month": row.get("Month"),
                "year": row.get("Year"),
                "name": row.get("Name"),
                "organiser": row.get("Organiser"),
                "long_description": row.get("Long description"),
                "highlighted": _to_bool(row.get("Highlighted")),
                "is_official_masterdata": json.dumps(row.get("Is official masterdata (in #{lang})", {})),
                "attachments": _clean_str(row.get("Attachments"), None),
                "updated": row.get("Updated"),
                "updated_by_owner": row.get("Updated by owner"),
            }
        )
    if payload:
        conn.execute(sql, payload)


def upsert_educations(conn, df: pd.DataFrame):
    fields = {
        "cv_partner_section_id": "CV Partner section ID",
        "external_unique_id": "External unique ID",
        "month_from": "Month from",
        "year_from": "Year from",
        "month_to": "Month to",
        "year_to": "Year to",
        "highlighted": "Highlighted",
        "attachments": "Attachments",
        "place_of_study": "Place of study",
        "degree": "Degree",
        "description": "Description",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "education", fields)


def upsert_positions(conn, df: pd.DataFrame):
    fields = {
        "cv_partner_section_id": "CV Partner section ID",
        "external_unique_id": "External unique ID",
        "year_from": "Year from",
        "year_to": "Year to",
        "highlighted": "Highlighted",
        "name": "Name",
        "description": "Description",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "position", fields)


def upsert_blogs(conn, df: pd.DataFrame):
    fields = {
        "cv_partner_section_id": "CV Partner section ID",
        "external_unique_id": "External unique ID",
        "name": "Name",
        "description": "Description",
        "highlighted": "Highlighted",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "blog_publication", fields)


def upsert_cv_roles(conn, df: pd.DataFrame):
    fields = {
        "name": "Name",
        "description": "Description",
        "highlighted": "Highlighted",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "cv_role", fields)


def upsert_key_qualifications(conn, df: pd.DataFrame):
    fields = {
        "cv_partner_section_id": "CV Partner section ID",
        "external_unique_id": "External unique ID",
        "label": "Label",
        "summary": "Summary of Qualifications",
        "short_description": "Short description",
        "updated": "Updated",
        "updated_by_owner": "Updated by owner",
    }
    upsert_section_table(conn, df, "key_qualification", fields)


def upsert_sc_clearance(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} security clearances...")
    sql = text(
        """
        INSERT INTO user_clearance(user_id, clearance_id, valid_from, valid_to, verified_by, notes)
        VALUES (:user_id, :clearance_id, :valid_from, :valid_to, :verified_by, :notes)
        ON CONFLICT (user_id, clearance_id, valid_from) DO UPDATE
        SET valid_to    = EXCLUDED.valid_to,
            verified_by = EXCLUDED.verified_by,
            notes       = EXCLUDED.notes
    """
    )
    for _, row in df.iterrows():
        uid = _resolve_user_id(conn, row.get("Email"), row.get("UPN"), row.get("External User ID"))
        if not uid:
            continue

        clearance_name = _clean_str(row.get("Clearance"), "None") or "None"
        conn.execute(text("INSERT INTO dim_clearance(name) VALUES (:n) ON CONFLICT(name) DO NOTHING"), {"n": clearance_name})
        clearance_id = conn.execute(
            text("SELECT clearance_id FROM dim_clearance WHERE name=:n"), {"n": clearance_name}
        ).scalar()

        valid_from_date = _to_date(row.get("Valid From"), default=_to_date("1900-01-01"))
        valid_to_date = _to_date(row.get("Valid To"))
        verified_by = _clean_str(row.get("Verified By"), None) or None
        notes = _clean_str(row.get("Notes"), None) or None

        if valid_to_date and valid_from_date and valid_to_date < valid_from_date:
            valid_to_date = None

        conn.execute(
            sql,
            {
                "user_id": uid,
                "clearance_id": clearance_id,
                "valid_from": valid_from_date,
                "valid_to": valid_to_date,
                "verified_by": verified_by,
                "notes": notes,
            },
        )


def upsert_availability(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    logger.info(f"Upserting {len(df)} availability rows...")
    sql = text(
        """
        INSERT INTO user_availability(user_id, date, percent_available, source)
        VALUES (:user_id, :date, :percent_available, :source)
        ON CONFLICT (user_id, date) DO UPDATE
        SET percent_available = EXCLUDED.percent_available,
            source            = EXCLUDED.source,
            updated_at        = NOW()
    """
    )
    for _, row in df.iterrows():
        uid = _resolve_user_id(conn, row.get("Email"), row.get("UPN"), row.get("External User ID"))
        if not uid:
            continue
        raw_percent = row.get("Percent Available")
        percent = 0 if raw_percent is None or (isinstance(raw_percent, float) and pd.isna(raw_percent)) else int(float(raw_percent))
        percent = max(0, min(100, percent))
        conn.execute(
            sql,
            {
                "user_id": uid,
                "date": _clean_str(row.get("Date"), None) or None,
                "percent_available": percent,
                "source": _clean_str(row.get("Source"), "Fake generator"),
            },
        )


# ---------- Orchestrator ----------

def load(clean_data, engine: Engine) -> None:
    """
    Run all upserts inside a single transaction.
    """
    with engine.begin() as conn:
        upsert_users(conn, getattr(clean_data, "users_df", None))
        upsert_cvs(conn, getattr(clean_data, "cvs_df", None))
        upsert_technologies(conn, getattr(clean_data, "technologies_df", None))
        upsert_languages(conn, getattr(clean_data, "languages_df", None))
        upsert_project_experiences(conn, getattr(clean_data, "project_experiences_df", None))
        upsert_work_experiences(conn, getattr(clean_data, "work_experiences_df", None))
        upsert_certifications(conn, getattr(clean_data, "certifications_df", None))
        upsert_courses(conn, getattr(clean_data, "courses_df", None))
        upsert_educations(conn, getattr(clean_data, "educations_df", None))
        upsert_positions(conn, getattr(clean_data, "positions_df", None))
        upsert_blogs(conn, getattr(clean_data, "blogs_df", None))
        upsert_cv_roles(conn, getattr(clean_data, "cv_roles_df", None))
        upsert_key_qualifications(conn, getattr(clean_data, "key_qualifications_df", None))
        upsert_sc_clearance(conn, getattr(clean_data, "sc_clearance_df", None))
        upsert_availability(conn, getattr(clean_data, "availability_df", None))
    logger.info("Load complete.")


__all__ = [
    "load",
    "upsert_users",
    "upsert_cvs",
    "upsert_technologies",
    "upsert_languages",
    "upsert_project_experiences",
    "upsert_work_experiences",
    "upsert_certifications",
    "upsert_courses",
    "upsert_educations",
    "upsert_positions",
    "upsert_blogs",
    "upsert_cv_roles",
    "upsert_key_qualifications",
    "upsert_sc_clearance",
    "upsert_availability",
]
