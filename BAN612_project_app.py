import streamlit as st
import pandas as pd
from jobspy import scrape_jobs
from streamlit_gsheets import GSheetsConnection
from datetime import datetime
import uuid

# --- CATEGORY DEFINITIONS ---
REMOTE_OPTIONS = ["Hybrid", "Remote", "On-site", "Unknown"]
EXP_MAP = {
    "Entry": ["entry", "junior", "0-2 years", "1-2 years", "new grad", "intern"],
    "Mid": ["3-5 years", "4-6 years", "mid-level", "mid level"],
    "Senior": ["senior", "lead", "principal", "director", "7+ years", "8+ years"]
}
SKILL_OPTIONS = ["python", "sql", "excel", "tableau", "power bi", "machine learning", "strategy", "consulting"]

st.title("🚀 BAN 612 Collaborative Job Scraper")
conn = st.connection("gsheets", type=GSheetsConnection)

# --- SIDEBAR INPUTS ---
with st.sidebar:
    user_name = st.text_input("Teammate Name")
    # Allow manual typing within the selection boxes
    remote_choice = st.multiselect("Remote Status", REMOTE_OPTIONS, default=["Remote"], accept_new_options=True)
    role_focus = st.multiselect("Role Focus", ["Strategic", "Technical", "Hybrid"], accept_new_options=True)
    target_skills = st.multiselect("Skills to Extract", SKILL_OPTIONS, accept_new_options=True)
    limit = st.number_input("Entries to Search", 10, 200, 50)

if st.button("Run Search"):
    # 1. Scrape raw data
    jobs = scrape_jobs(site_name=["linkedin", "indeed"], search_term="Strategy Consultant", results_wanted=limit)
    
    if not jobs.empty:
        # 2. Map & Standardize Columns
        run_id = str(uuid.uuid4())[:8]
        jobs['team_member'] = user_name
        jobs['search_run_id'] = run_id
        jobs['date_scraped'] = datetime.now().strftime("%Y-%m-%d")
        jobs['remote_status'] = ", ".join(remote_choice)
        jobs['role_focus'] = ", ".join(role_focus)
        
        # 3. Create Dedupe Key (Combination of unique variables)
        jobs['dedupe_key'] = jobs.apply(lambda x: f"{x['job_url']}_{x['team_member']}_{run_id}", axis=1)
        
        # 4. Write to GSheets
        existing = conn.read()
        updated_df = pd.concat([existing, jobs], ignore_index=True).drop_duplicates(subset=['dedupe_key'])
        conn.update(data=updated_df)
        st.success(f"Added {len(jobs)} entries to the master sheet!")
