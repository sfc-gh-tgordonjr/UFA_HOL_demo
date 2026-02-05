# Plan: Dynamic Tables Notebook Refactor

## Overview
Transform the existing generic sales demo into a **User Engagement Analytics** pipeline tailored for content-focused nonprofit presentation. Creates a 5-layer Dynamic Table DAG that tells a compelling data transformation story.

---

## Architecture: Before vs After

### Current (2 DTs)
```
cust_info + salesdata → customer_sales_data_history → salesreport
```

### New (5 DTs)
```
MEMBERS    RESOURCES    RAW_INTERACTIONS
   │           │              │
   │           │              ▼
   │           │      ┌─────────────────┐
   │           │      │ MEMBER_SESSIONS │ (DT1 - Sessionize)
   │           │      │ LAG=DOWNSTREAM  │
   │           │      └────────┬────────┘
   │           │               │
   │           ▼               │
   │    ┌──────────────┐       │
   │    │RESOURCE_CATALOG│     │
   │    │(DT2 - Enrich) │      │
   │    │LAG=DOWNSTREAM │      │
   │    └──────┬───────┘       │
   │           │               │
   ▼           ▼               ▼
┌───────────────────────────────────────┐
│     SESSION_ENGAGEMENT_DETAIL         │
│        (DT3 - Join All)               │
│        LAG=DOWNSTREAM                 │
└───────────────────┬───────────────────┘
                    │
                    ▼
       ┌────────────────────────┐
       │MEMBER_ENGAGEMENT_SUMMARY│
       │    (DT4 - Aggregate)    │
       │    LAG=DOWNSTREAM       │
       └────────────┬───────────┘
                    │
                    ▼
       ┌────────────────────────┐
       │  ENGAGEMENT_DASHBOARD  │
       │   (DT5 - Final KPIs)   │
       │    LAG='1 MINUTE'      │
       └────────────────────────┘
```

---

## Task 1: Update Markdown Header and Narrative

### Changes:
- **Title**: "User Engagement Analytics Pipeline"
- **Demo Flow**: Update steps to reflect engagement analytics narrative
- **Business Context**: "For content-focused organizations, tracking user engagement with educational resources is critical for measuring impact"

### New Demo Flow:
1. Setup database/schema
2. Create engagement data generators
3. Generate base tables (Members, Resources, Interactions)
4. Create 5-layer Dynamic Tables pipeline
5. View DAG in Snowsight (5 nodes!)
6. Insert data and watch cascade refresh

---

## Task 2: Create 3 New Data Generators (UDTFs)

### Generator 1: `gen_members(num_records)`
**Output columns:**
| Column | Type | Description |
|--------|------|-------------|
| member_id | NUMBER | Unique member identifier (10001+) |
| email | VARCHAR | Fake email address |
| name | VARCHAR | Full name |
| member_type | VARCHAR | 'free', 'registered', 'premium' |
| signup_date | DATE | Registration date (past 2 years) |
| region | VARCHAR | US region |

### Generator 2: `gen_resources(num_records)`
**Output columns:**
| Column | Type | Description |
|--------|------|-------------|
| resource_id | NUMBER | Unique resource ID (100+) |
| title | VARCHAR | Resource title (Faker sentence) |
| content_type | VARCHAR | 'article', 'video', 'tool', 'webinar', 'guide' |
| topic | VARCHAR | 'focus', 'reading', 'learning', 'parenting', 'school' |
| difficulty_level | VARCHAR | 'beginner', 'intermediate', 'advanced' |
| publish_date | DATE | When published |
| estimated_minutes | NUMBER | Time to consume |

### Generator 3: `gen_interactions(num_records, ndays)`
**Output columns:**
| Column | Type | Description |
|--------|------|-------------|
| member_id | NUMBER | FK to members |
| event_data | VARIANT | JSON with: resource_id, event_type, session_id, timestamp, engagement_seconds, device_type |

**Event types:** 'page_view', 'video_start', 'video_complete', 'download', 'share', 'bookmark'

---

## Task 3: Create Base Tables

```sql
-- 1,000 members
CREATE OR REPLACE TABLE MEMBERS AS 
SELECT * FROM TABLE(gen_members(1000)) ORDER BY 1;

-- 100 resources  
CREATE OR REPLACE TABLE RESOURCES AS 
SELECT * FROM TABLE(gen_resources(100)) ORDER BY 1;

-- 10,000 interactions
CREATE OR REPLACE TABLE RAW_INTERACTIONS AS 
SELECT * FROM TABLE(gen_interactions(10000, 30));
```

Preview cells for each table.

---

## Task 4: Build 5-Layer Dynamic Table Pipeline

### DT1: MEMBER_SESSIONS (Sessionize raw events)
```sql
CREATE OR REPLACE DYNAMIC TABLE MEMBER_SESSIONS
    LAG = 'DOWNSTREAM'
    WAREHOUSE = UNDERSTOOD_DEMO_WH
AS
SELECT 
    event_data:session_id::VARCHAR as session_id,
    member_id,
    MIN(event_data:timestamp::TIMESTAMP) as session_start,
    MAX(event_data:timestamp::TIMESTAMP) as session_end,
    COUNT(*) as event_count,
    SUM(event_data:engagement_seconds::NUMBER) as total_engagement_seconds,
    ARRAY_AGG(DISTINCT event_data:resource_id::NUMBER) as resources_viewed,
    MAX(event_data:device_type::VARCHAR) as device_type
FROM RAW_INTERACTIONS
GROUP BY event_data:session_id, member_id;
```

### DT2: RESOURCE_CATALOG (Enrich resources)
```sql
CREATE OR REPLACE DYNAMIC TABLE RESOURCE_CATALOG
    LAG = 'DOWNSTREAM'
    WAREHOUSE = UNDERSTOOD_DEMO_WH
AS
SELECT 
    resource_id,
    title,
    content_type,
    topic,
    difficulty_level,
    publish_date,
    estimated_minutes,
    CASE 
        WHEN content_type IN ('video', 'webinar') THEN 'multimedia'
        WHEN content_type IN ('article', 'guide') THEN 'reading'
        ELSE 'interactive'
    END as content_category,
    DATEDIFF('day', publish_date, CURRENT_DATE()) as days_since_publish
FROM RESOURCES;
```

### DT3: SESSION_ENGAGEMENT_DETAIL (Join all sources)
```sql
CREATE OR REPLACE DYNAMIC TABLE SESSION_ENGAGEMENT_DETAIL
    LAG = 'DOWNSTREAM'
    WAREHOUSE = UNDERSTOOD_DEMO_WH
AS
SELECT 
    s.session_id,
    s.member_id,
    m.name as member_name,
    m.member_type,
    m.region,
    s.session_start,
    s.session_end,
    s.event_count,
    s.total_engagement_seconds,
    s.device_type,
    r.value::NUMBER as resource_id,
    rc.title as resource_title,
    rc.content_type,
    rc.topic,
    rc.content_category
FROM MEMBER_SESSIONS s
JOIN MEMBERS m ON s.member_id = m.member_id,
LATERAL FLATTEN(input => s.resources_viewed) r
LEFT JOIN RESOURCE_CATALOG rc ON r.value::NUMBER = rc.resource_id;
```

### DT4: MEMBER_ENGAGEMENT_SUMMARY (Aggregate per member)
```sql
CREATE OR REPLACE DYNAMIC TABLE MEMBER_ENGAGEMENT_SUMMARY
    LAG = 'DOWNSTREAM'
    WAREHOUSE = UNDERSTOOD_DEMO_WH
AS
SELECT 
    member_id,
    member_name,
    member_type,
    region,
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(DISTINCT resource_id) as unique_resources,
    SUM(total_engagement_seconds) as lifetime_engagement_seconds,
    ROUND(AVG(event_count), 1) as avg_events_per_session,
    MODE(topic) as favorite_topic,
    MODE(content_type) as preferred_content_type,
    MAX(session_start) as last_activity
FROM SESSION_ENGAGEMENT_DETAIL
GROUP BY member_id, member_name, member_type, region;
```

### DT5: ENGAGEMENT_DASHBOARD (Executive KPIs - 1 minute freshness)
```sql
CREATE OR REPLACE DYNAMIC TABLE ENGAGEMENT_DASHBOARD
    LAG = '1 MINUTE'
    WAREHOUSE = UNDERSTOOD_DEMO_WH
AS
SELECT 
    CURRENT_TIMESTAMP() as report_generated_at,
    COUNT(DISTINCT member_id) as active_members,
    SUM(total_sessions) as total_sessions,
    SUM(unique_resources) as total_resource_views,
    ROUND(SUM(lifetime_engagement_seconds) / 3600, 1) as total_engagement_hours,
    ROUND(AVG(lifetime_engagement_seconds) / 60, 1) as avg_engagement_minutes_per_member,
    MODE(favorite_topic) as most_popular_topic,
    MODE(preferred_content_type) as most_popular_content_type,
    -- Breakdown by member type
    COUNT_IF(member_type = 'premium') as premium_members,
    COUNT_IF(member_type = 'registered') as registered_members,
    COUNT_IF(member_type = 'free') as free_members,
    -- Breakdown by region  
    OBJECT_AGG(region, member_count) as members_by_region
FROM MEMBER_ENGAGEMENT_SUMMARY,
(SELECT region, COUNT(*) as member_count FROM MEMBER_ENGAGEMENT_SUMMARY GROUP BY region);
```
*Note: Final DT5 SQL may need adjustment for proper aggregation syntax*

---

## Task 5: Update DAG Visualization Section

### New ASCII Art:
```
        MEMBERS          RESOURCES       RAW_INTERACTIONS
           │                 │                  │
           │                 ▼                  ▼
           │         RESOURCE_CATALOG    MEMBER_SESSIONS
           │            (DT2)               (DT1)
           │                 │                  │
           └────────────────┼──────────────────┘
                            ▼
               SESSION_ENGAGEMENT_DETAIL
                       (DT3)
                            │
                            ▼
              MEMBER_ENGAGEMENT_SUMMARY
                       (DT4)
                            │
                            ▼
                ENGAGEMENT_DASHBOARD
                   (DT5 - 1 MIN)
```

### Updated Navigation Instructions:
1. Navigate to **Data** → **Databases** → **UNDERSTOOD_DEMO** → **DYNAMIC_TABLES**
2. Click on **ENGAGEMENT_DASHBOARD**
3. Click the **Graph** tab
4. Marvel at the 5-node DAG!

---

## Task 6: Enhance Auto-Refresh Demo Section

### Streaming Data Insert Function
Add a new cell with a loop that inserts batches of data to simulate real-time event stream:

```python
import time

def stream_interactions(batches=5, records_per_batch=100, delay_seconds=10):
    """Simulate real-time event streaming"""
    for i in range(batches):
        session.sql(f"INSERT INTO RAW_INTERACTIONS SELECT * FROM TABLE(gen_interactions({records_per_batch}, 1))").collect()
        count = session.sql("SELECT COUNT(*) as cnt FROM RAW_INTERACTIONS").collect()[0]['CNT']
        print(f"Batch {i+1}/{batches}: Inserted {records_per_batch} events. Total: {count}")
        if i < batches - 1:
            print(f"Waiting {delay_seconds} seconds...")
            time.sleep(delay_seconds)
    print("Streaming complete! Check ENGAGEMENT_DASHBOARD for updated metrics.")
```

### Updated Monitoring Queries:
```sql
-- Check all DT refresh history
SELECT 
    NAME,
    STATE,
    REFRESH_START_TIME,
    REFRESH_END_TIME,
    DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME) as duration_seconds
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME IN ('MEMBER_SESSIONS', 'RESOURCE_CATALOG', 'SESSION_ENGAGEMENT_DETAIL', 
               'MEMBER_ENGAGEMENT_SUMMARY', 'ENGAGEMENT_DASHBOARD')
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;
```

---

## Task 7: Update Summary and Cleanup

### New Summary Bullets:
1. **5-layer declarative pipeline** - SELECT statements define the entire DAG
2. **Smart refresh strategy** - DOWNSTREAM saves compute, 1-MINUTE only where freshness matters
3. **Automatic dependency tracking** - Snowflake knows the order
4. **Incremental refresh** - Only processes changed data
5. **Visual lineage** - Beautiful DAG in Snowsight
6. **Built-in monitoring** - Refresh history via INFORMATION_SCHEMA

### New Cleanup Commands:
```python
# Dynamic Tables (must drop in reverse dependency order)
session.sql("DROP DYNAMIC TABLE IF EXISTS ENGAGEMENT_DASHBOARD").collect()
session.sql("DROP DYNAMIC TABLE IF EXISTS MEMBER_ENGAGEMENT_SUMMARY").collect()
session.sql("DROP DYNAMIC TABLE IF EXISTS SESSION_ENGAGEMENT_DETAIL").collect()
session.sql("DROP DYNAMIC TABLE IF EXISTS RESOURCE_CATALOG").collect()
session.sql("DROP DYNAMIC TABLE IF EXISTS MEMBER_SESSIONS").collect()

# Base tables
session.sql("DROP TABLE IF EXISTS RAW_INTERACTIONS").collect()
session.sql("DROP TABLE IF EXISTS RESOURCES").collect()
session.sql("DROP TABLE IF EXISTS MEMBERS").collect()

# Functions
session.sql("DROP FUNCTION IF EXISTS gen_interactions(NUMBER, NUMBER)").collect()
session.sql("DROP FUNCTION IF EXISTS gen_resources(NUMBER)").collect()
session.sql("DROP FUNCTION IF EXISTS gen_members(NUMBER)").collect()
```

---

## Execution Notes

- Keep cell structure similar to original for familiarity
- Test each generator before moving to next
- Verify DAG appears correctly in Snowsight after DT creation
- Test streaming function with small batches first
- Total notebook should still run in ~15 minutes