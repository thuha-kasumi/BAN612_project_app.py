import re
import uuid
from datetime import datetime

import pandas as pd
import streamlit as st
from jobspy import scrape_jobs
from streamlit_gsheets import GSheetsConnection


st.set_page_config(page_title="BAN 612 Collaborative Job Scraper", layout="wide")

MASTER_SHEET = "master_jobs"
RAW_SHEET = "raw_jobs_archive"
LOG_SHEET = "run_log"

ROLE_FOCUS_OPTIONS = ["Strategic", "Technical", "Hybrid"]

INDUSTRY_OPTIONS = [
    "High Tech",
    "Finance and Real Estate",
    "Healthcare / Medical",
    "Consulting / Professional Services",
    "Retail / Trade / Transportation / Utilities",
    "Public Sector",
    "Manufacturing",
    "Education",
    "Other"
]

STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
    "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
    "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"
}

STATE_NAME_TO_ABBR = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA","colorado":"CO",
    "connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID",
    "illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY","louisiana":"LA",
    "maine":"ME","maryland":"MD","massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
    "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ",
    "new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK",
    "oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC","south dakota":"SD",
    "tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA",
    "west virginia":"WV","wisconsin":"WI","wyoming":"WY","district of columbia":"DC"
}

SKILL_PATTERNS = {
    "python_required": [r"\bpython\b"],
    "sql_required": [r"\bsql\b"],
    "excel_required": [r"\bexcel\b"],
    "tableau_required": [r"\btableau\b"],
    "power_bi_required": [r"\bpower\s?bi\b"],
    "ml_required": [r"\bmachine learning\b", r"\bml\b"],
    "ai_required": [r"\bartificial intelligence\b", r"\bgenerative ai\b", r"\bllm\b", r"\bai\b"],
    "communication_required": [r"\bcommunication\b", r"\bstakeholder\b", r"\bpresentation\b"],
    "strategy_required": [r"\bstrategy\b", r"\bstrategic\b"],
    "consulting_required": [r"\bconsulting\b", r"\bconsultant\b"],
}

EXP_PATTERNS = {
    "Entry": [r"\bentry\b", r"\bjunior\b", r"\bnew grad\b", r"\bintern\b", r"\b0[- ]?2 years\b", r"\b1[- ]?2 years\b"],
    "Mid": [r"\bmid[- ]?level\b", r"\b3[- ]?5 years\b", r"\b4[- ]?6 years\b"],
    "Senior": [r"\bsenior\b", r"\blead\b", r"\bprincipal\b", r"\bdirector\b", r"\b7\+ years\b", r"\b8\+ years\b"],
}

DEFAULT_COLUMNS = [
    "job_uid",
    "job_title",
    "company",
    "location_raw",
    "city",
    "state",
    "remote_status",
    "job_url",
    "site",
    "date_posted",
    "date_scraped",
    "employment_type",
    "industry_tag",
    "role_focus_tag",
    "search_term",
    "team_member",
    "salary_min",
    "salary_max",
    "salary_currency",
    "salary_interval",
    "salary_source",
    "salary_annual_min",
    "salary_annual_max",
    "experience_level",
    "experience_years_min",
    "python_required",
    "sql_required",
    "excel_required",
    "tableau_required",
    "power_bi_required",
    "ml_required",
    "ai_required",
    "communication_required",
    "strategy_required",
    "consulting_required",
    "missing_salary_flag",
    "missing_description_flag",
    "description",
]


def normalize_text(value):
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip().lower()


def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def safe_get(row, candidates, default=None):
    for col in candidates:
        if col in row and pd.notna(row[col]):
            return row[col]
    return default


def build_job_uid(row):
    job_url = normalize_text(safe_get(row, ["job_url", "url", "link"], ""))
    if job_url:
        return job_url

    title = normalize_text(safe_get(row, ["job_title", "title"], ""))
    company = normalize_text(safe_get(row, ["company"], ""))
    location = normalize_text(safe_get(row, ["location_raw", "location"], ""))
    return f"{title}|{company}|{location}"


def parse_city_state(location_raw):
    if pd.isna(location_raw) or not str(location_raw).strip():
        return "", ""

    text = str(location_raw).strip()

    # Remove common noise
    text = re.sub(r"\s*\(.*?\)\s*", " ", text).strip()
    text = re.sub(r"\s+", " ", text)

    # Remote / hybrid only
    lowered = text.lower()
    if lowered in ["remote", "hybrid", "united states", "usa"]:
        return "", ""

    parts = [p.strip() for p in text.split(",") if p.strip()]

    # Case 1: "San Francisco, CA"
    if len(parts) >= 2:
        city = parts[0]
        state_part = parts[1].split()[0].strip()

        if state_part.upper() in STATE_ABBR:
            return city, state_part.upper()

        state_name = parts[1].lower()
        if state_name in STATE_NAME_TO_ABBR:
            return city, STATE_NAME_TO_ABBR[state_name]

    # Case 2: "San Francisco CA"
    tokens = text.split()
    if len(tokens) >= 2 and tokens[-1].upper() in STATE_ABBR:
        state = tokens[-1].upper()
        city = " ".join(tokens[:-1]).strip()
        return city, state

    # Case 3: "San Francisco California"
    if len(tokens) >= 2:
        maybe_state = " ".join(tokens[-2:]).lower()
        if maybe_state in STATE_NAME_TO_ABBR:
            state = STATE_NAME_TO_ABBR[maybe_state]
            city = " ".join(tokens[:-2]).strip()
            return city, state

        maybe_state = tokens[-1].lower()
        if maybe_state in STATE_NAME_TO_ABBR:
            state = STATE_NAME_TO_ABBR[maybe_state]
            city = " ".join(tokens[:-1]).strip()
            return city, state

    return text, ""


def detect_remote_status(location_text, description_text):
    combined = f"{normalize_text(location_text)} {normalize_text(description_text)}"
    if "hybrid" in combined:
        return "Hybrid"
    if "remote" in combined:
        return "Remote"
    if "on-site" in combined or "onsite" in combined or "on site" in combined:
        return "On-site"
    return "Unknown"


def text_contains_any(text, patterns):
    t = normalize_text(text)
    return int(any(re.search(p, t) for p in patterns))


def infer_experience_level(description, title=""):
    combined = f"{normalize_text(title)} {normalize_text(description)}"
    for level, patterns in EXP_PATTERNS.items():
        if any(re.search(p, combined) for p in patterns):
            return level
    return "Unknown"


def infer_experience_years_min(description):
    if pd.isna(description):
        return None

    text = normalize_text(description)
    patterns = [
        r"(\d+)\+?\s+years",
        r"minimum of (\d+)\s+years",
        r"at least (\d+)\s+years",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def annualize_salary(min_val, max_val, interval):
    if pd.isna(interval) or interval is None:
        return min_val, max_val

    interval = str(interval).lower().strip()
    factor_map = {
        "yearly": 1,
        "annually": 1,
        "monthly": 12,
        "weekly": 52,
        "daily": 260,
        "hourly": 2080,
    }
    factor = factor_map.get(interval)
    if factor is None:
        return min_val, max_val

    annual_min = min_val * factor if pd.notna(min_val) else None
    annual_max = max_val * factor if pd.notna(max_val) else None
    return annual_min, annual_max


def parse_salary_from_description(description):
    """
    Returns:
    salary_min, salary_max, salary_currency, salary_interval, salary_source
    """
    if pd.isna(description) or not str(description).strip():
        return None, None, None, None, None

    text = str(description)

    # Normalize commas
    text_clean = text.replace(",", "")

    # Range with interval, e.g. "$120000 - $150000 a year"
    patterns = [
        r"\$ ?(\d+(?:\.\d+)?)\s*[-to]+\s*\$ ?(\d+(?:\.\d+)?)\s*(?:per|a)?\s*(hour|year|month|week|day)",
        r"\$ ?(\d+(?:\.\d+)?)\s*[-–]\s*\$ ?(\d+(?:\.\d+)?)\s*(hour|year|month|week|day)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text_clean, flags=re.IGNORECASE)
        if match:
            g = match.groups()
            min_val = float(g[0])
            max_val = float(g[1])
            interval = g[2].lower()
            interval = {"hour": "hourly", "year": "yearly", "month": "monthly", "week": "weekly", "day": "daily"}[interval]
            return min_val, max_val, "USD", interval, "description"

    # Single amount with interval, e.g. "$65 an hour" or "$130000 a year"
    pattern_single = r"\$ ?(\d+(?:\.\d+)?)\s*(?:per|a)?\s*(hour|year|month|week|day)"
    match = re.search(pattern_single, text_clean, flags=re.IGNORECASE)
    if match:
        amount = float(match.group(1))
        interval = match.group(2).lower()
        interval = {"hour": "hourly", "year": "yearly", "month": "monthly", "week": "weekly", "day": "daily"}[interval]
        return amount, amount, "USD", interval, "description"

    return None, None, None, None, None


def standardize_jobs(raw_df, team_member, search_term, industry_tag, role_focus_tag):
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

    df = raw_df.copy()

    rename_map = {
        "title": "job_title",
        "location": "location_raw",
        "min_amount": "salary_min",
        "max_amount": "salary_max",
        "currency": "salary_currency",
        "interval": "salary_interval",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df = ensure_columns(df, [
        "job_title", "company", "location_raw", "description", "date_posted",
        "job_url", "site", "employment_type", "salary_min", "salary_max",
        "salary_currency", "salary_interval"
    ])

    df["job_uid"] = df.apply(build_job_uid, axis=1)

    city_state = df["location_raw"].apply(parse_city_state)
    df["city"] = city_state.apply(lambda x: x[0])
    df["state"] = city_state.apply(lambda x: x[1])

    df["remote_status"] = df.apply(
        lambda row: detect_remote_status(row["location_raw"], row["description"]),
        axis=1
    )

    df["date_scraped"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["team_member"] = team_member
    df["search_term"] = search_term
    df["industry_tag"] = industry_tag
    df["role_focus_tag"] = role_focus_tag
    df["salary_source"] = "structured"

    # Fallback salary parsing from description where structured salary is missing
    desc_salary = df["description"].apply(parse_salary_from_description)
    df["desc_salary_min"] = desc_salary.apply(lambda x: x[0])
    df["desc_salary_max"] = desc_salary.apply(lambda x: x[1])
    df["desc_salary_currency"] = desc_salary.apply(lambda x: x[2])
    df["desc_salary_interval"] = desc_salary.apply(lambda x: x[3])
    df["desc_salary_source"] = desc_salary.apply(lambda x: x[4])

    missing_structured = df["salary_min"].isna() & df["salary_max"].isna()

    df.loc[missing_structured, "salary_min"] = df.loc[missing_structured, "desc_salary_min"]
    df.loc[missing_structured, "salary_max"] = df.loc[missing_structured, "desc_salary_max"]
    df.loc[missing_structured, "salary_currency"] = df.loc[missing_structured, "desc_salary_currency"]
    df.loc[missing_structured, "salary_interval"] = df.loc[missing_structured, "desc_salary_interval"]
    df.loc[missing_structured & df["desc_salary_source"].notna(), "salary_source"] = "description"

    df["experience_level"] = df.apply(
        lambda row: infer_experience_level(row["description"], row["job_title"]),
        axis=1
    )
    df["experience_years_min"] = df["description"].apply(infer_experience_years_min)

    for skill_col, patterns in SKILL_PATTERNS.items():
        df[skill_col] = df["description"].fillna("").apply(lambda x: text_contains_any(x, patterns))

    df["salary_annual_min"], df["salary_annual_max"] = zip(*df.apply(
        lambda row: annualize_salary(row["salary_min"], row["salary_max"], row["salary_interval"]),
        axis=1
    ))

    df["missing_salary_flag"] = ((df["salary_min"].isna()) & (df["salary_max"].isna())).astype(int)
    df["missing_description_flag"] = (
        df["description"].isna() | (df["description"].astype(str).str.strip() == "")
    ).astype(int)

    drop_helper_cols = [
        "desc_salary_min", "desc_salary_max", "desc_salary_currency",
        "desc_salary_interval", "desc_salary_source"
    ]
    df = df.drop(columns=[c for c in drop_helper_cols if c in df.columns], errors="ignore")

    df = ensure_columns(df, DEFAULT_COLUMNS)
    df = df[DEFAULT_COLUMNS].copy()
    df = df.drop_duplicates(subset=["job_uid"], keep="first")

    return df


def read_sheet_safe(conn, worksheet):
    try:
        df = conn.read(worksheet=worksheet, ttl=0)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


def append_unique_rows(conn, worksheet, new_df, key_col="job_uid"):
    existing = read_sheet_safe(conn, worksheet)

    if existing.empty:
        conn.update(worksheet=worksheet, data=new_df)
        return len(new_df), 0

    existing = ensure_columns(existing, [key_col])
    existing_keys = set(existing[key_col].astype(str).fillna("").tolist())

    to_save = new_df[~new_df[key_col].astype(str).isin(existing_keys)].copy()
    duplicates_skipped = len(new_df) - len(to_save)

    updated = pd.concat([existing, to_save], ignore_index=True)
    conn.update(worksheet=worksheet, data=updated)

    return len(to_save), duplicates_skipped


def log_run(conn, payload):
    old_log = read_sheet_safe(conn, LOG_SHEET)
    new_log = pd.DataFrame([payload])
    updated = pd.concat([old_log, new_log], ignore_index=True)
    conn.update(worksheet=LOG_SHEET, data=updated)


def initialize_sheet_tabs(conn):
    empty_master = pd.DataFrame(columns=DEFAULT_COLUMNS)
    empty_raw = pd.DataFrame(columns=["date_scraped", "team_member", "search_term"])
    empty_log = pd.DataFrame(columns=[
        "run_id", "run_timestamp", "team_member", "search_term", "location", "sites",
        "results_requested", "raw_scraped", "unique_in_run", "already_in_master",
        "saved_to_master", "duplicates_skipped", "missing_salary", "missing_description",
        "industry_tag", "role_focus_tag"
    ])

    conn.update(worksheet=MASTER_SHEET, data=empty_master)
    conn.update(worksheet=RAW_SHEET, data=empty_raw)
    conn.update(worksheet=LOG_SHEET, data=empty_log)


st.title("BAN 612 Collaborative Job Scraper")
st.caption("Scrape -> standardize -> review -> save only unique rows")

conn = st.connection("gsheets", type=GSheetsConnection)

with st.sidebar:
    st.header("Search setup")
    team_member = st.text_input("Teammate Name")
    search_term = st.text_input("Search Term", value="Data Analyst")
    location = st.text_input("Location", value="United States")
    site_names = st.multiselect(
        "Job Sites",
        ["linkedin", "indeed", "glassdoor"],
        default=["linkedin", "indeed"]
    )
    results_wanted = st.number_input("Results Wanted", min_value=10, max_value=200, value=50, step=10)
    industry_tag = st.selectbox("Industry Tag", INDUSTRY_OPTIONS)
    role_focus_tag = st.selectbox("Role Focus Tag", ROLE_FOCUS_OPTIONS)

    st.divider()
    if st.button("Initialize / Reset 3 tabs"):
        initialize_sheet_tabs(conn)
        st.success("Tabs initialized: master_jobs, raw_jobs_archive, run_log")

run_search = st.button("Run Search", type="primary")

if run_search:
    if not team_member.strip():
        st.error("Please enter your teammate name.")
        st.stop()

    if not search_term.strip():
        st.error("Please enter a search term.")
        st.stop()

    if not site_names:
        st.error("Please choose at least one job site.")
        st.stop()

    with st.spinner("Scraping jobs..."):
        raw_jobs = scrape_jobs(
            site_name=site_names,
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
        )

    if raw_jobs is None or raw_jobs.empty:
        st.warning("No jobs found.")
        st.stop()

    clean_jobs = standardize_jobs(
        raw_df=raw_jobs,
        team_member=team_member,
        search_term=search_term,
        industry_tag=industry_tag,
        role_focus_tag=role_focus_tag
    )

    existing_master = read_sheet_safe(conn, MASTER_SHEET)
    existing_keys = set(existing_master["job_uid"].astype(str).tolist()) if not existing_master.empty and "job_uid" in existing_master.columns else set()

    clean_jobs["already_in_master"] = clean_jobs["job_uid"].astype(str).isin(existing_keys).astype(int)

    total_scraped = len(raw_jobs)
    unique_in_run = len(clean_jobs)
    already_in_master = int(clean_jobs["already_in_master"].sum())
    new_rows = unique_in_run - already_in_master
    missing_salary = int(clean_jobs["missing_salary_flag"].sum())
    missing_description = int(clean_jobs["missing_description_flag"].sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Raw scraped", total_scraped)
    c2.metric("Unique in run", unique_in_run)
    c3.metric("Already in master", already_in_master)
    c4.metric("New rows to save", new_rows)
    c5.metric("Missing salary", missing_salary)

    st.subheader("Preview cleaned records")
    st.dataframe(
        clean_jobs[[
            "job_title", "company", "city", "state", "remote_status",
            "salary_min", "salary_max", "salary_interval", "salary_source",
            "salary_annual_min", "salary_annual_max", "experience_level",
            "python_required", "sql_required", "ml_required", "already_in_master"
        ]],
        use_container_width=True
    )

    save_raw = st.checkbox("Also save raw scrape to raw_jobs_archive", value=False)

    if st.button("Save unique cleaned rows to master_jobs"):
        saved_count, duplicates_skipped = append_unique_rows(
            conn=conn,
            worksheet=MASTER_SHEET,
            new_df=clean_jobs.drop(columns=["already_in_master"]),
            key_col="job_uid"
        )

        if save_raw:
            raw_copy = raw_jobs.copy()
            raw_copy["team_member"] = team_member
            raw_copy["search_term"] = search_term
            raw_copy["date_scraped"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            existing_raw = read_sheet_safe(conn, RAW_SHEET)
            updated_raw = pd.concat([existing_raw, raw_copy], ignore_index=True)
            conn.update(worksheet=RAW_SHEET, data=updated_raw)

        log_run(conn, {
            "run_id": str(uuid.uuid4())[:8],
            "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "team_member": team_member,
            "search_term": search_term,
            "location": location,
            "sites": ", ".join(site_names),
            "results_requested": results_wanted,
            "raw_scraped": total_scraped,
            "unique_in_run": unique_in_run,
            "already_in_master": already_in_master,
            "saved_to_master": saved_count,
            "duplicates_skipped": duplicates_skipped,
            "missing_salary": missing_salary,
            "missing_description": missing_description,
            "industry_tag": industry_tag,
            "role_focus_tag": role_focus_tag,
        })

        st.success(f"Saved {saved_count} new unique rows to {MASTER_SHEET}. Skipped {duplicates_skipped} duplicates.")
