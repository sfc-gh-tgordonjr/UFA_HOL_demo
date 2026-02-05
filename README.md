# Snowflake Hands-On Demo Lab

A collection of interactive demos showcasing key Snowflake capabilities. Each demo is self-contained and designed to run in approximately 15-20 minutes.

---

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** role (or equivalent privileges)
- Access to Snowsight (Snowflake's web UI)

---

## Getting Started

### Step 1: Upload the Demo Files

1. Log into Snowsight
2. Navigate to **Projects > Notebooks**
3. Click **+ Notebook** > **Import .ipynb file**
4. Upload all `.ipynb` files from this folder
5. For Streamlit apps (`.py` files), navigate to **Projects > Streamlit** and create new apps

### Step 2: Run the Setup

Open and run **`0_setup.ipynb`** first. This creates:
- A demo warehouse (`UNDERSTOOD_DEMO_WH`)
- A demo database with schemas (`UNDERSTOOD_DEMO`)
- An event table for observability logging

### Step 3: Run the Demos

Run the demos in any order:

| Demo | File | Description | Duration |
|------|------|-------------|----------|
| 1 | `1_observability_demo.ipynb` | Log events from UDFs using Event Tables | 15 min |
| 2 | `2_streamlit_app.py` | Build interactive apps with database writeback | 15 min |
| 3 | `3_dynamic_tables_demo.ipynb` | Create auto-refreshing data pipelines | 15 min |
| 3b | `3_dynamic_tables_app.py` | Visual demo of Dynamic Tables | 10 min |
| 4 | `4_ai_cortex_demo.ipynb` | Use built-in AI: sentiment, summarization, LLMs | 20 min |
| 5 | `5_horizon_catalog_walkthrough.ipynb` | Explore data discovery and lineage in Snowsight | 15 min |

### Step 4: Cleanup

When finished, run **`99_cleanup.ipynb`** to remove all demo objects and restore your account.

---

## Demo Descriptions

### 1. Observability with Event Tables
Learn how to capture structured logs from Python UDFs. See how events flow into queryable tables for debugging and monitoring.

### 2. Streamlit in Snowflake
Build a simple data entry application that writes directly back to Snowflake tables. No external infrastructure required.

### 3. Dynamic Tables
Create declarative data pipelines that automatically refresh when source data changes. Compare source tables with their transformed summaries in real-time.

### 4. Cortex AI Functions
Use Snowflake's built-in AI capabilities:
- **SENTIMENT**: Analyze text sentiment (-1 to +1)
- **SUMMARIZE**: Generate concise summaries
- **COMPLETE**: Generate text with large language models

All processing happens inside Snowflake—your data never leaves your account.

### 5. Horizon Catalog
A guided tour of Snowflake's data discovery features:
- Browse databases and schemas
- View column-level lineage
- Search across your data estate

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Object does not exist" | Run `0_setup.ipynb` first |
| "Insufficient privileges" | Switch to ACCOUNTADMIN role |
| Notebook shows old content | Delete and re-import the notebook |
| Warehouse suspended | It auto-resumes; re-run the cell |

---

## Resources

- [Snowflake Documentation](https://docs.snowflake.com)
- [Snowflake Cortex AI](https://docs.snowflake.com/en/guides-overview-ai-features)
- [Dynamic Tables Guide](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)

---

## Cleanup Reminder

Always run `99_cleanup.ipynb` when done to:
- Drop the demo database and all objects
- Drop the demo warehouse
- Avoid unnecessary storage and compute costs
