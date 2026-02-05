import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()
session.sql("USE DATABASE UNDERSTOOD_DEMO").collect()
session.sql("USE SCHEMA STREAMLIT_APP").collect()
TABLE_NAME = "STUDENT_SUPPORT_TRACKER"

def table_exists():
    result = session.sql(f"SHOW TABLES LIKE '{TABLE_NAME}'").collect()
    return len(result) > 0

def create_table():
    seed_df = pd.DataFrame({
        "STUDENT_ID": ["S001", "S002", "S003"],
        "NAME": ["Alex Johnson", "Sam Rivera", "Jordan Lee"],
        "SUPPORT_TYPE": ["Reading Support", "Focus Strategies", "Math Support"],
        "STATUS": ["In Progress", "Completed", "In Progress"],
        "NOTES": ["Working on phonics", "Using timer techniques", "Visual aids helpful"],
        "LAST_UPDATED": [datetime.now().date()] * 3
    })
    session.write_pandas(seed_df, TABLE_NAME, auto_create_table=True, overwrite=True)

def load_data():
    return session.table(TABLE_NAME).to_pandas()

def save_data(df):
    session.write_pandas(df, TABLE_NAME, overwrite=True)

st.title("Learning Support Tracker")
st.caption("Track student progress and interventions - Data saves directly to Snowflake!")

exists = table_exists()
status_col1, status_col2 = st.columns([3, 1])
with status_col1:
    if exists:
        st.success(f"Connected to table: {TABLE_NAME}")
    else:
        st.warning(f"Table {TABLE_NAME} does not exist yet")
with status_col2:
    if not exists:
        if st.button("Create Table", type="primary"):
            create_table()
            st.rerun()

if exists:
    if "students" not in st.session_state or st.session_state.get("needs_reload", False):
        st.session_state.students = load_data()
        st.session_state.needs_reload = False

    st.subheader("Current Students")
    st.dataframe(st.session_state.students, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Add or Edit Student")
    
    col1, col2 = st.columns(2)
    
    with col1:
        student_ids = ["-- New Student --"] + st.session_state.students["STUDENT_ID"].tolist()
        selected = st.selectbox("Select student to edit or add new", student_ids)
        
        if selected == "-- New Student --":
            default_id = ""
            default_name = ""
            default_support = "Reading Support"
            default_status = "Not Started"
            default_notes = ""
        else:
            row = st.session_state.students[st.session_state.students["STUDENT_ID"] == selected].iloc[0]
            default_id = row["STUDENT_ID"]
            default_name = row["NAME"]
            default_support = row["SUPPORT_TYPE"]
            default_status = row["STATUS"]
            default_notes = row["NOTES"] if pd.notna(row["NOTES"]) else ""
    
    support_types = ["Reading Support", "Focus Strategies", "Math Support", "Writing Support", "Social Skills"]
    status_options = ["Not Started", "In Progress", "Completed", "On Hold"]
    
    with st.form("student_form"):
        form_col1, form_col2 = st.columns(2)
        with form_col1:
            new_id = st.text_input("Student ID", value=default_id, placeholder="e.g., S004")
            new_name = st.text_input("Name", value=default_name, placeholder="e.g., Taylor Smith")
            new_support = st.selectbox(
                "Support Type", 
                support_types,
                index=support_types.index(default_support) if default_support in support_types else 0
            )
        with form_col2:
            new_status = st.selectbox(
                "Status",
                status_options,
                index=status_options.index(default_status) if default_status in status_options else 0
            )
            new_notes = st.text_area("Notes", value=default_notes, height=100, placeholder="Progress notes...")
        
        submitted = st.form_submit_button("Save to Snowflake", type="primary", use_container_width=True)
        if submitted:
            if not new_id or not new_name:
                st.error("Student ID and Name are required!")
            else:
                new_row = pd.DataFrame({
                    "STUDENT_ID": [new_id],
                    "NAME": [new_name],
                    "SUPPORT_TYPE": [new_support],
                    "STATUS": [new_status],
                    "NOTES": [new_notes],
                    "LAST_UPDATED": [datetime.now().date()]
                })
                if selected == "-- New Student --":
                    updated_df = pd.concat([st.session_state.students, new_row], ignore_index=True)
                else:
                    updated_df = st.session_state.students[st.session_state.students["STUDENT_ID"] != selected]
                    updated_df = pd.concat([updated_df, new_row], ignore_index=True)
                save_data(updated_df)
                st.session_state.students = load_data()
                st.success(f"Saved {new_name} to Snowflake!")
                st.rerun()

    st.markdown("---")
    st.subheader("Summary")
    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("Total Students", len(st.session_state.students))
    col_b.metric("In Progress", len(st.session_state.students[st.session_state.students["STATUS"] == "In Progress"]))
    col_c.metric("Completed", len(st.session_state.students[st.session_state.students["STATUS"] == "Completed"]))
    col_d.metric("Not Started", len(st.session_state.students[st.session_state.students["STATUS"] == "Not Started"]))
