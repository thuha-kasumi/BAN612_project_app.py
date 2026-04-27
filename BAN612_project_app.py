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

ROLE_FOCUS_OPTIONS = ["Hybrid", "Strategic", "Technical"]

INDUSTRY_OPTIONS = [
    "High Tech",
    "Finance and Real Estate",
    "Healthcare / Medical",
    "Consulting / Professional Services",
    "Retail / Trade / Transportation / Utilities",
    "Public Sector",
    "Manufacturing",
    "Education",
    "Other",
]

SKILL_OPTIONS = [
    "python", "sql", "excel", "tableau", "power bi", "dashboard", "r", "sas",
    "machine learning", "analytics", "data analysis", "data science", "etl",
    "stakeholder management", "communication", "financial modeling", "strategy",
    "consulting", "project management", "powerpoint", "stakeholder", "client",
    "business case", "operating model", "transformation", "automation",
    "advisory", "management consulting", "roadmap",
]

DEFAULT_SITE_OPTIONS = ["linkedin", "indeed", "glassdoor"]

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
    "consulting_required": [r"\bconsulting\b", r"\bconsultant\b", r"\bmanagement consulting\b"],
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
    "is_linkedin",
    "date_posted",
    "date_scraped",
    "employment_type",
    "industry_tag",
    "role_focus_tag",
    "search_term",
    "search_skills",
    "search_companies",
    "search_locations",
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


def init_state():
    if "raw_jobs_df" not in st.session_state:
        st.session_state["raw_jobs_df"] = None
    if "clean_jobs_df" not in st.session_state:
        st.session_state["clean_jobs_df"] = None
    if "search_summary" not in st.session_state:
        st.session_state["search_summary"] = None


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
    text = re.sub(r"\s*\(.*?\)\s*", " ", text).strip()
    text = re.sub(r"\s+", " ", text)

    lowered = text.lower()

    broad_locations = {
        "remote",
        "hybrid",
        "us",
        "u.s.",
        "usa",
        "united states",
        "california, united states",
        "new york, united states",
        "texas, united states",
        "ca, us",
        "ny, us",
        "tx, us",
    }

    if lowered in broad_locations:
        return "", ""

    parts = [p.strip() for p in text.split(",") if p.strip()]

    if len(parts) >= 2:
        city = parts[0]
        state_part = parts[1].split()[0].strip()

        if state_part.upper() in STATE_ABBR:
            return city, state_part.upper()

        state_name = parts[1].lower()
        if state_name in STATE_NAME_TO_ABBR:
            return city, STATE_NAME_TO_ABBR[state_name]

        if parts[0].lower() in STATE_NAME_TO_ABBR and "united states" in lowered:
            return "", STATE_NAME_TO_ABBR[parts[0].lower()]

    tokens = text.split()

    if len(tokens) >= 2 and tokens[-1].upper() in STATE_ABBR:
        return " ".join(tokens[:-1]).strip(), tokens[-1].upper()

    if len(tokens) >= 2:
        maybe_state_two = " ".join(tokens[-2:]).lower()
        if maybe_state_two in STATE_NAME_TO_ABBR:
            return " ".join(tokens[:-2]).strip(), STATE_NAME_TO_ABBR[maybe_state_two]

        maybe_state_one = tokens[-1].lower()
        if maybe_state_one in STATE_NAME_TO_ABBR:
            return " ".join(tokens[:-1]).strip(), STATE_NAME_TO_ABBR[maybe_state_one]

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


def infer_experience_years_min(description):
    if pd.isna(description) or not str(description).strip():
        return None

    text = normalize_text(description)
    patterns = [
        r"minimum of (\d+)\s+years",
        r"at least (\d+)\s+years",
        r"(\d+)\+?\s+years",
        r"(\d+)\s*-\s*(\d+)\s+years",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            if len(match.groups()) == 2:
                return int(match.group(1))
            return int(match.group(1))

    return None


def infer_experience_level(description, title="", years_min=None):
    if years_min is not None and pd.notna(years_min):
        try:
            years = float(years_min)
            if years >= 5:
                return "Senior"
            elif years >= 3:
                return "Mid"
            elif years >= 0:
                return "Entry"
        except Exception:
            pass

    combined = f"{normalize_text(title)} {normalize_text(description)}"
    for level, patterns in EXP_PATTERNS.items():
        if any(re.search(p, combined) for p in patterns):
            return level

    return "Unknown"


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
    if pd.isna(description) or not str(description).strip():
        return None, None, None, None, None

    text = str(description).replace(",", "")

    patterns = [
        r"\$ ?(\d+(?:\.\d+)?)\s*[-–to]+\s*\$ ?(\d+(?:\.\d+)?)\s*(?:per|a)?\s*(hour|year|month|week|day)",
        r"\$ ?(\d+(?:\.\d+)?)\s*[-–]\s*\$ ?(\d+(?:\.\d+)?)\s*(hour|year|month|week|day)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            min_val = float(match.group(1))
            max_val = float(match.group(2))
            interval = match.group(3).lower()
            interval = {
                "hour": "hourly",
                "year": "yearly",
                "month": "monthly",
                "week": "weekly",
                "day": "daily",
            }[interval]
            return min_val, max_val, "USD", interval, "description"

    pattern_single = r"\$ ?(\d+(?:\.\d+)?)\s*(?:per|a)?\s*(hour|year|month|week|day)"
    match = re.search(pattern_single, text, flags=re.IGNORECASE)
    if match:
        amount = float(match.group(1))
        interval = match.group(2).lower()
        interval = {
            "hour": "hourly",
            "year": "yearly",
            "month": "monthly",
            "week": "weekly",
            "day": "daily",
        }[interval]
        return amount, amount, "USD", interval, "description"

    return None, None, None, None, None


def build_search_term(target_roles, companies, locations, selected_skills):
    parts = []

    if target_roles.strip():
        parts.append(target_roles.strip())

    if companies.strip():
        company_terms = [x.strip() for x in companies.split(",") if x.strip()]
        if company_terms:
            parts.append(" ".join(company_terms))

    if locations.strip():
        location_terms = [x.strip() for x in locations.split(",") if x.strip()]
        if location_terms:
            parts.append(" ".join(location_terms))

    if selected_skills:
        parts.append(" ".join(selected_skills))

    return " ".join(parts).strip()


def standardize_jobs(
    raw_df,
    team_member,
    search_term,
    industry_tag,
    role_focus_tag,
    search_skills="",
    search_companies="",
    search_locations=""
):
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
    df["search_skills"] = search_skills
    df["search_companies"] = search_companies
    df["search_locations"] = search_locations
    df["is_linkedin"] = (df["site"].astype(str).str.lower() == "linkedin").astype(int)

    df["salary_source"] = "missing"

    desc_salary = df["description"].apply(parse_salary_from_description)
    df["desc_salary_min"] = desc_salary.apply(lambda x: x[0])
    df["desc_salary_max"] = desc_salary.apply(lambda x: x[1])
    df["desc_salary_currency"] = desc_salary.apply(lambda x: x[2])
    df["desc_salary_interval"] = desc_salary.apply(lambda x: x[3])
    df["desc_salary_source"] = desc_salary.apply(lambda x: x[4])

    has_structured_salary = df["salary_min"].notna() | df["salary_max"].notna()
    df.loc[has_structured_salary, "salary_source"] = "structured"

    missing_structured = ~has_structured_salary
    df.loc[missing_structured, "salary_min"] = df.loc[missing_structured, "desc_salary_min"]
    df.loc[missing_structured, "salary_max"] = df.loc[missing_structured, "desc_salary_max"]
    df.loc[missing_structured, "salary_currency"] = df.loc[missing_structured, "desc_salary_currency"]
    df.loc[missing_structured, "salary_interval"] = df.loc[missing_structured, "desc_salary_interval"]

    has_desc_salary = df["desc_salary_source"].notna()
    df.loc[missing_structured & has_desc_salary, "salary_source"] = "description"

    df["experience_years_min"] = df["description"].apply(infer_experience_years_min)
    df["experience_level"] = df.apply(
        lambda row: infer_experience_level(
            row["description"],
            row["job_title"],
            row["experience_years_min"]
        ),
        axis=1
    )

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

    df = df.drop(columns=[
        c for c in [
            "desc_salary_min", "desc_salary_max", "desc_salary_currency",
            "desc_salary_interval", "desc_salary_source"
        ] if c in df.columns
    ], errors="ignore")

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
    existing = read_sheet_safe(conn, LOG_SHEET)
    updated = pd.concat([existing, pd.DataFrame([payload])], ignore_index=True)
    conn.update(worksheet=LOG_SHEET, data=updated)


init_state()

st.markdown("""
<style>
div[data-testid="stButton"] > button[kind="primary"] {
    background-color: #d32f2f;
    color: white;
    border: none;
}
div[data-testid="stButton"] > button[kind="primary"]:hover {
    background-color: #b71c1c;
    color: white;
}
.small-grey {
    color: #808080;
    font-size: 0.9rem;
    margin-top: -10px;
    margin-bottom: 8px;
}
</style>
""", unsafe_allow_html=True)

st.title("BAN 612 Collaborative Job Scraper")
st.caption("Scrape -> standardize -> review -> save only unique rows")

conn = st.connection("gsheets", type=GSheetsConnection)

with st.sidebar:
    st.header("Search setup")

    team_member = st.text_input("Name *", placeholder="please input your name")
    st.markdown('<div class="small-grey">Please input your name</div>', unsafe_allow_html=True)

    selected_industries = st.multiselect("Industry", INDUSTRY_OPTIONS)
    other_industry = ""
    if "Other" in selected_industries:
        other_industry = st.text_input("Other Industry", placeholder="please input your industry")

    selected_role_focus = st.multiselect("Role Focus", ROLE_FOCUS_OPTIONS)

    target_roles = st.text_input(
        "Target roles",
        placeholder="please input your search key-words, separated by comma. For example: Financial consultant"
    )

    selected_skills = st.multiselect("Skills (Optional)", SKILL_OPTIONS)

    preferred_companies = st.text_input(
        "Companies (Optional)",
        placeholder="please input your preferred companies, separated by comma"
    )

    preferred_locations = st.text_input(
        "Location (Optional)",
        placeholder="please input your preferred cities / states, separated by comma"
    )

    site_names = st.multiselect(
        "Job Sites (Optional)",
        DEFAULT_SITE_OPTIONS,
        default=DEFAULT_SITE_OPTIONS
    )

    results_wanted = st.number_input(
        "Number of results wanted",
        min_value=10,
        max_value=200,
        value=20,
        step=10
    )
    st.markdown('<div class="small-grey">Please input number only</div>', unsafe_allow_html=True)


industry_tag = ", ".join(
    [x for x in selected_industries if x != "Other"] +
    ([other_industry.strip()] if other_industry.strip() else [])
)
role_focus_tag = ", ".join(selected_role_focus)
search_skills_str = ", ".join(selected_skills)
search_term = build_search_term(
    target_roles,
    preferred_companies,
    preferred_locations,
    selected_skills
)

run_search = st.button("Run Search")

if run_search:
    if not team_member.strip():
        st.error("Name is required.")
        st.stop()

    if not target_roles.strip():
        st.error("Target roles is required.")
        st.stop()

    if not selected_industries and not other_industry.strip():
        st.error("Please select at least one industry.")
        st.stop()

    if not selected_role_focus:
        st.error("Please select at least one role focus.")
        st.stop()

    if not site_names:
        st.error("Please choose at least one job site.")
        st.stop()

    with st.spinner("Scraping jobs..."):
        raw_jobs = scrape_jobs(
            site_name=site_names,
            search_term=search_term,
            location="United States",
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
        role_focus_tag=role_focus_tag,
        search_skills=search_skills_str,
        search_companies=preferred_companies,
        search_locations=preferred_locations
    )

    existing_master = read_sheet_safe(conn, MASTER_SHEET)
    existing_keys = (
        set(existing_master["job_uid"].astype(str).tolist())
        if not existing_master.empty and "job_uid" in existing_master.columns
        else set()
    )

    clean_jobs["already_in_master"] = clean_jobs["job_uid"].astype(str).isin(existing_keys).astype(int)

    st.session_state["raw_jobs_df"] = raw_jobs.copy()
    st.session_state["clean_jobs_df"] = clean_jobs.copy()
    st.session_state["search_summary"] = {
        "team_member": team_member,
        "search_term": search_term,
        "results_requested": results_wanted,
        "industry_tag": industry_tag,
        "role_focus_tag": role_focus_tag,
        "site_names": site_names,
        "preferred_companies": preferred_companies,
        "preferred_locations": preferred_locations,
        "search_skills_str": search_skills_str,
    }

if st.session_state["clean_jobs_df"] is not None:
    clean_jobs = st.session_state["clean_jobs_df"].copy()
    raw_jobs = st.session_state["raw_jobs_df"].copy()
    summary = st.session_state["search_summary"]

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
            "experience_years_min", "is_linkedin",
            "python_required", "sql_required", "ml_required", "already_in_master"
        ]],
        use_container_width=True
    )

    if st.button("SAVE", type="primary"):
        master_to_save = clean_jobs.drop(columns=["already_in_master"]).copy()

        saved_count, duplicates_skipped = append_unique_rows(
            conn=conn,
            worksheet=MASTER_SHEET,
            new_df=master_to_save,
            key_col="job_uid"
        )

        raw_copy = raw_jobs.copy()
        raw_copy["team_member"] = summary["team_member"]
        raw_copy["search_term"] = summary["search_term"]
        raw_copy["date_scraped"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        existing_raw = read_sheet_safe(conn, RAW_SHEET)
        updated_raw = pd.concat([existing_raw, raw_copy], ignore_index=True)
        conn.update(worksheet=RAW_SHEET, data=updated_raw)

        log_run(conn, {
            "run_id": str(uuid.uuid4())[:8],
            "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "team_member": summary["team_member"],
            "search_term": summary["search_term"],
            "results_requested": summary["results_requested"],
            "raw_scraped": total_scraped,
            "unique_in_run": unique_in_run,
            "already_in_master": already_in_master,
            "saved_to_master": saved_count,
            "duplicates_skipped": duplicates_skipped,
            "missing_salary": missing_salary,
            "missing_description": missing_description,
            "industry_tag": summary["industry_tag"],
            "role_focus_tag": summary["role_focus_tag"],
        })

        st.success(
            f"Saved {saved_count} new unique rows to {MASTER_SHEET} "
            f"and archived raw rows to {RAW_SHEET}."
        )

        st.session_state["raw_jobs_df"] = None
        st.session_state["clean_jobs_df"] = None
        st.session_state["search_summary"] = None
