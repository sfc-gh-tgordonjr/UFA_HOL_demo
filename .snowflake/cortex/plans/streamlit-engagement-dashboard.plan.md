# Plan: Streamlit Executive Dashboard

## Overview
Replace [3_streamlit_app.py](demos/3_streamlit_app.py) with an Executive Dashboard displaying real-time KPIs from the Dynamic Tables pipeline.

---

## Data Sources

Query these tables from `UNDERSTOOD_DEMO.DYNAMIC_TABLES`:

```sql
-- Primary: Org-wide KPIs (single row)
SELECT * FROM ENGAGEMENT_DASHBOARD;

-- Secondary: Top engaged members
SELECT * FROM MEMBER_ENGAGEMENT_SUMMARY 
ORDER BY lifetime_engagement_seconds DESC LIMIT 10;

-- For charts: Member breakdown by type/region
SELECT member_type, COUNT(*) as count FROM MEMBER_ENGAGEMENT_SUMMARY GROUP BY member_type;
SELECT region, COUNT(*) as count FROM MEMBER_ENGAGEMENT_SUMMARY GROUP BY region;
```

---

## Layout

```
+----------------------------------------------------------+
|  User Engagement Dashboard                               |
|  Last Updated: {report_generated_at}    [Refresh Button] |
+----------------------------------------------------------+
|  [Active Members]  [Sessions]  [Engagement Hrs]  [Views] |
|       847            12,453        1,234.5        45,678 |
+----------------------------------------------------------+
|  Members by Type (bar)    |   Content Preferences (pie)  |
|  ████ Free: 512           |   [article] 35%              |
|  ███ Registered: 284      |   [video] 28%                |
|  █ Premium: 51            |   [tool] 22%                 |
+----------------------------------------------------------+
|  Top Engaged Members                                     |
|  | Name | Type | Region | Sessions | Engagement | Topic  |
|  |------|------|--------|----------|------------|--------|
|  | Alex | premium | West | 45 | 12,450s | focus        |
+----------------------------------------------------------+
|  Members by Region (bar chart)                           |
+----------------------------------------------------------+
```

---

## Implementation

### Task 1: Update imports and connection
```python
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()
session.sql("USE DATABASE UNDERSTOOD_DEMO").collect()
session.sql("USE SCHEMA DYNAMIC_TABLES").collect()
```

### Task 2: KPI Metrics Row
Query `ENGAGEMENT_DASHBOARD` and display with `st.metric()`:
- Active Members
- Total Sessions  
- Total Engagement Hours
- Total Resource Views
- Premium/Registered/Free counts

### Task 3: Charts
Use `st.bar_chart()` or `st.plotly_chart()` for:
- Members by Type (horizontal bar)
- Members by Region (bar)
- Content Type Preferences (from `most_popular_content_type` or aggregate query)

### Task 4: Top Engaged Members Table
Query `MEMBER_ENGAGEMENT_SUMMARY` top 10, display with `st.dataframe()`:
- member_name, member_type, region, total_sessions, lifetime_engagement_seconds, favorite_topic

### Task 5: Auto-refresh
- Display `report_generated_at` timestamp prominently
- Add `st.button("Refresh")` that triggers `st.rerun()`
- Optional: `st.autorefresh()` from streamlit-autorefresh for live updates

---

## File Changes

**Replace entire contents of:** [demos/3_streamlit_app.py](demos/3_streamlit_app.py)

No new files needed. Single ~80 line file.