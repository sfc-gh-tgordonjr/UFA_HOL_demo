import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Dynamic Tables Demo")
st.caption("See how Snowflake automatically keeps data in sync!")

WAREHOUSE = "UNDERSTOOD_DEMO_WH"

def setup_exists():
    result = session.sql("SHOW TABLES LIKE 'DONATIONS' IN UNDERSTOOD_DEMO.DYNAMIC_TABLES").collect()
    return len(result) > 0

def create_setup():
    session.sql("CREATE DATABASE IF NOT EXISTS UNDERSTOOD_DEMO").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS UNDERSTOOD_DEMO.DYNAMIC_TABLES").collect()
    session.sql("USE SCHEMA UNDERSTOOD_DEMO.DYNAMIC_TABLES").collect()
    
    session.sql("""
        CREATE OR REPLACE TABLE donations (
            donor_name VARCHAR,
            amount NUMBER(10,2),
            donation_date DATE
        )
    """).collect()
    
    seed_data = pd.DataFrame({
        "DONOR_NAME": ["Alice Smith", "Bob Jones", "Carol White"],
        "AMOUNT": [100.00, 250.00, 50.00],
        "DONATION_DATE": [datetime.now().date()] * 3
    })
    session.write_pandas(seed_data, "DONATIONS", overwrite=True)
    
    session.sql(f"""
        CREATE OR REPLACE DYNAMIC TABLE donation_summary
            LAG = '1 MINUTE'
            WAREHOUSE = {WAREHOUSE}
        AS
        SELECT 
            COUNT(*) as total_donors,
            SUM(amount) as total_raised,
            ROUND(AVG(amount), 2) as avg_donation,
            MAX(donation_date) as last_donation_date
        FROM donations
    """).collect()

def get_donations():
    return session.sql("SELECT * FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.DONATIONS ORDER BY DONATION_DATE DESC").to_pandas()

def get_summary():
    return session.sql("SELECT * FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.DONATION_SUMMARY").to_pandas()

def add_donation(name, amount):
    session.sql(f"""
        INSERT INTO UNDERSTOOD_DEMO.DYNAMIC_TABLES.DONATIONS 
        VALUES ('{name}', {amount}, CURRENT_DATE())
    """).collect()

if not setup_exists():
    st.warning("Demo tables not set up yet")
    if st.button("Create Demo Tables", type="primary"):
        with st.spinner("Creating tables..."):
            create_setup()
        st.success("Setup complete!")
        st.rerun()
else:
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Source Table: DONATIONS")
        st.caption("This is the raw data - you can add to it")
        donations_df = get_donations()
        st.dataframe(donations_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("Dynamic Table: DONATION_SUMMARY")
        st.caption("This auto-updates within 1 minute of changes")
        summary_df = get_summary()
        if not summary_df.empty:
            st.metric("Total Donors", int(summary_df["TOTAL_DONORS"].iloc[0]))
            st.metric("Total Raised", f"${summary_df['TOTAL_RAISED'].iloc[0]:,.2f}")
            st.metric("Avg Donation", f"${summary_df['AVG_DONATION'].iloc[0]:,.2f}")
    
    st.markdown("---")
    st.subheader("Add a New Donation")
    
    with st.form("donation_form"):
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            donor_name = st.text_input("Donor Name", placeholder="e.g., David Lee")
        with fcol2:
            amount = st.number_input("Amount ($)", min_value=1.0, value=100.0, step=10.0)
        
        submitted = st.form_submit_button("Add Donation", type="primary", use_container_width=True)
        if submitted:
            if donor_name:
                add_donation(donor_name, amount)
                st.success(f"Added ${amount:.2f} donation from {donor_name}!")
                st.rerun()
            else:
                st.error("Please enter a donor name")
    
    st.markdown("---")
    st.info("""
    **How it works:**
    1. Add a donation above - it goes into the SOURCE table immediately
    2. The DYNAMIC TABLE (summary) refreshes automatically within 1 minute
    3. Click the refresh button below to see updated summary
    
    **No code needed** - Snowflake handles the pipeline automatically!
    """)
    
    if st.button("Refresh to see updated summary"):
        st.rerun()
