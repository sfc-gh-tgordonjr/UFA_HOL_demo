# Demo Materials - Ready-to-Run Code

## DEMO 1: Observability (Logging, Tracing, Metrics)

### Setup: Create Event Table
```sql
-- Create event table for capturing telemetry
CREATE OR REPLACE EVENT TABLE demo_event_table;

-- Associate with account (requires ACCOUNTADMIN)
ALTER ACCOUNT SET EVENT_TABLE = DEMO_DB.PUBLIC.DEMO_EVENT_TABLE;

-- Set log level
ALTER SESSION SET LOG_LEVEL = INFO;
```

### Demo UDF with Logging
```sql
CREATE OR REPLACE FUNCTION process_student_with_logging(student_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'process'
AS
$$
import _snowflake
import json

def process(student_name):
    # Log start
    _snowflake.log("info", json.dumps({
        "action": "process_start",
        "student": student_name
    }))
    
    # Simulate processing
    if not student_name or len(student_name.strip()) == 0:
        _snowflake.log("warn", "Empty student name received")
        return "ERROR: Empty name"
    
    result = f"Processed: {student_name.upper()}"
    
    # Log success
    _snowflake.log("info", json.dumps({
        "action": "process_complete",
        "student": student_name,
        "result": result
    }))
    
    return result
$$;
```

### Test the UDF
```sql
-- Run some test calls
SELECT process_student_with_logging('Alex Johnson');
SELECT process_student_with_logging('Sam Rivera');
SELECT process_student_with_logging('');  -- Will log warning
SELECT process_student_with_logging('Jordan Lee');
```

### Query the Event Table
```sql
-- View recent logs
SELECT 
    TIMESTAMP,
    RESOURCE_ATTRIBUTES['snow.executable.name']::STRING as FUNCTION_NAME,
    RECORD['severity_text']::STRING as LOG_LEVEL,
    VALUE::STRING as MESSAGE
FROM demo_event_table
WHERE RECORD_TYPE = 'LOG'
  AND TIMESTAMP > DATEADD(minute, -10, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
LIMIT 20;
```

---

## DEMO 2: Streamlit Writeback App

### The Code (already in your notes)
Deploy this to Snowflake Streamlit. Key talking points:
1. `get_active_session()` - no credentials needed
2. `session.write_pandas()` - the writeback magic
3. `st.session_state` - managing app state
4. `st.form` - batch user input

### Quick Deployment Steps
1. Go to Snowsight → Streamlit
2. Create new app
3. Paste the code from your notes (lines 49-182)
4. Run and demo

---

## DEMO 3: Dynamic Tables

### Complete Setup Script
```sql
-- Set parameters
SET DB_NAME = 'UNDERSTOOD_DEMO';
SET SCHEMA_NAME = 'DT_DEMO';
SET WAREHOUSE_NAME = 'COMPUTE_WH';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME);
USE DATABASE IDENTIFIER($DB_NAME);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SCHEMA_NAME);
USE SCHEMA IDENTIFIER($SCHEMA_NAME);
USE WAREHOUSE IDENTIFIER($WAREHOUSE_NAME);

-- Create customer data generator
CREATE OR REPLACE FUNCTION gen_cust_info(num_records NUMBER)
RETURNS TABLE (custid NUMBER(10), cname VARCHAR(100), spendlimit NUMBER(10,2))
LANGUAGE PYTHON
RUNTIME_VERSION=3.10
HANDLER='CustTab'
PACKAGES = ('Faker')
AS $$
from faker import Faker
import random

fake = Faker()

class CustTab:
    def process(self, num_records):
        customer_id = 1000
        for _ in range(num_records):
            custid = customer_id + 1
            cname = fake.name()
            spendlimit = round(random.uniform(1000, 10000), 2)
            customer_id += 1
            yield (custid, cname, spendlimit)
$$;

-- Create product data generator
CREATE OR REPLACE FUNCTION gen_prod_inv(num_records NUMBER)
RETURNS TABLE (pid NUMBER(10), pname VARCHAR(100), stock NUMBER(10,2), stockdate DATE)
LANGUAGE PYTHON
RUNTIME_VERSION=3.10
HANDLER='ProdTab'
PACKAGES = ('Faker')
AS $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class ProdTab:
    def process(self, num_records):
        product_id = 100
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000), 0)
            current_date = datetime.now()
            min_date = current_date - timedelta(days=90)
            stockdate = fake.date_between_dates(min_date, current_date)
            product_id += 1
            yield (pid, pname, stock, stockdate)
$$;

-- Create sales data generator
CREATE OR REPLACE FUNCTION gen_cust_purchase(num_records NUMBER, ndays NUMBER)
RETURNS TABLE (custid NUMBER(10), purchase VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION=3.10
HANDLER='genCustPurchase'
PACKAGES = ('Faker')
AS $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    def process(self, num_records, ndays):
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            current_date = datetime.now()
            min_date = current_date - timedelta(days=ndays)
            pdate = fake.date_between_dates(min_date, current_date)
            purchase = {
                'prodid': fake.random_int(min=101, max=199),
                'quantity': fake.random_int(min=1, max=5),
                'purchase_amount': round(random.uniform(10, 1000), 2),
                'purchase_date': pdate
            }
            yield (c_id, purchase)
$$;

-- Generate base tables
CREATE OR REPLACE TABLE cust_info AS SELECT * FROM TABLE(gen_cust_info(1000)) ORDER BY 1;
CREATE OR REPLACE TABLE prod_stock_inv AS SELECT * FROM TABLE(gen_prod_inv(100)) ORDER BY 1;
CREATE OR REPLACE TABLE salesdata AS SELECT * FROM TABLE(gen_cust_purchase(10000, 10));

-- Verify base tables
SELECT 'cust_info' as table_name, COUNT(*) as row_count FROM cust_info
UNION ALL
SELECT 'prod_stock_inv', COUNT(*) FROM prod_stock_inv
UNION ALL
SELECT 'salesdata', COUNT(*) FROM salesdata;
```

### Create Dynamic Tables
```sql
-- First DT: Join customers and sales (DOWNSTREAM = refresh when needed)
CREATE OR REPLACE DYNAMIC TABLE customer_sales_data_history
    LAG = 'DOWNSTREAM'
    WAREHOUSE = $WAREHOUSE_NAME
AS
SELECT 
    s.custid as customer_id,
    c.cname as customer_name,
    s.purchase:"prodid"::NUMBER(5) as product_id,
    s.purchase:"purchase_amount"::NUMBER(10) as saleprice,
    s.purchase:"quantity"::NUMBER(5) as quantity,
    s.purchase:"purchase_date"::DATE as salesdate
FROM cust_info c 
INNER JOIN salesdata s ON c.custid = s.custid;

-- Second DT: Sales report with product info (1 MINUTE refresh)
CREATE OR REPLACE DYNAMIC TABLE salesreport
    LAG = '1 MINUTE'
    WAREHOUSE = $WAREHOUSE_NAME
AS
SELECT
    t1.customer_id,
    t1.customer_name,
    t1.product_id,
    p.pname as product_name,
    t1.saleprice,
    t1.quantity,
    (t1.saleprice / t1.quantity) as unitsalesprice,
    t1.salesdate as creation_time
FROM customer_sales_data_history t1 
INNER JOIN prod_stock_inv p ON t1.product_id = p.pid;

-- Verify DTs created
SELECT * FROM salesreport LIMIT 10;
```

### Demo the Auto-Refresh
```sql
-- Check current counts
SELECT 'Before Insert' as status, COUNT(*) as count FROM salesdata
UNION ALL
SELECT 'Before Insert', COUNT(*) FROM salesreport;

-- Insert new data
INSERT INTO salesdata SELECT * FROM TABLE(gen_cust_purchase(500, 2));

-- Check base table immediately
SELECT 'After Insert - Base', COUNT(*) FROM salesdata;

-- Wait ~1 minute, then check DT
-- (Show the automatic refresh happening)
SELECT 'After Refresh - DT', COUNT(*) FROM salesreport;
```

---

## DEMO 4: PII Detection

### Quick PII Detection Demo
```sql
-- Create sample table with PII
CREATE OR REPLACE TABLE pii_test_data (
    id INT,
    full_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    ssn VARCHAR,
    notes VARCHAR
);

INSERT INTO pii_test_data VALUES
(1, 'John Smith', 'john.smith@email.com', '555-123-4567', '123-45-6789', 'Customer since 2020'),
(2, 'Jane Doe', 'jane.doe@company.org', '(555) 987-6543', '987-65-4321', 'VIP customer'),
(3, 'Bob Wilson', 'bob@test.net', '555.111.2222', '111-22-3333', 'No special notes');

-- Detect PII in each column
SELECT 
    'email' as column_name,
    email as sample_value,
    SNOWFLAKE.CORTEX.DETECT_PII(email, ['EMAIL_ADDRESS']) as pii_result
FROM pii_test_data
UNION ALL
SELECT 
    'phone',
    phone,
    SNOWFLAKE.CORTEX.DETECT_PII(phone, ['PHONE_NUMBER'])
FROM pii_test_data
UNION ALL
SELECT 
    'ssn',
    ssn,
    SNOWFLAKE.CORTEX.DETECT_PII(ssn, ['US_SSN'])
FROM pii_test_data;
```

### Sentiment Analysis Demo
```sql
-- Sample reviews
CREATE OR REPLACE TABLE sample_reviews (
    branch VARCHAR,
    review_text VARCHAR
);

INSERT INTO sample_reviews VALUES
('Downtown', 'Amazing service! The staff was so helpful and friendly.'),
('Downtown', 'Food was excellent but a bit pricey.'),
('Airport', 'Long wait times and rude staff. Very disappointed.'),
('Airport', 'Food was cold and service was slow.'),
('Mall', 'Great location, good food, fair prices.'),
('Mall', 'Nice atmosphere but nothing special.');

-- Analyze sentiment by branch
SELECT 
    branch,
    COUNT(*) as review_count,
    ROUND(AVG(SNOWFLAKE.CORTEX.SENTIMENT(review_text)), 3) as avg_sentiment,
    CASE 
        WHEN AVG(SNOWFLAKE.CORTEX.SENTIMENT(review_text)) > 0.3 THEN '😊 Positive'
        WHEN AVG(SNOWFLAKE.CORTEX.SENTIMENT(review_text)) < -0.3 THEN '😟 Negative'
        ELSE '😐 Neutral'
    END as sentiment_category
FROM sample_reviews
GROUP BY branch
ORDER BY avg_sentiment DESC;
```

---

## DEMO 5: Horizon Catalog

### UI-Based Demo (Snowsight)
1. **Navigate to:** Data → Databases → [Your Database]
2. **Show:** 
   - Object descriptions and tags
   - Click on a table → Lineage tab
   - Show upstream/downstream dependencies
3. **Highlight:** 
   - Auto-captured lineage from DT demo
   - Usage statistics (if available)

### SQL-Based Lineage Query
```sql
-- Show object dependencies
SELECT 
    REFERENCING_OBJECT_NAME,
    REFERENCING_OBJECT_DOMAIN,
    REFERENCED_OBJECT_NAME,
    REFERENCED_OBJECT_DOMAIN
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCED_DATABASE_NAME = 'UNDERSTOOD_DEMO'
ORDER BY REFERENCING_OBJECT_NAME;
```

---

## Pre-Demo Checklist

### Before the Session
- [ ] Create UNDERSTOOD_DEMO database
- [ ] Create event table (if using observability demo)
- [ ] Deploy Streamlit app
- [ ] Run Dynamic Tables setup (takes ~2 min)
- [ ] Create PII test data
- [ ] Test all queries run without errors
- [ ] Have Snowsight open and logged in
- [ ] Have this document open for quick reference

### Browser Tabs to Have Ready
1. Snowsight - Worksheets (for SQL demos)
2. Snowsight - Streamlit (for writeback demo)
3. Snowsight - Data (for Horizon demo)
4. This document (for code snippets)
