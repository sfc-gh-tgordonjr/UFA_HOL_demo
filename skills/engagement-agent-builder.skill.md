# Skill: Engagement Analytics Agent Builder

Build a Snowflake Cortex Agent for querying Dynamic Tables via natural language. This skill instructs you to examine existing tables and dynamically generate the appropriate semantic view and agent.

---

## Phase 0: Research Syntax Requirements

Before creating any objects, use the `snowflake_product_docs` tool to look up the correct syntax and requirements.

### Step 0.1: Semantic View Syntax

Search for documentation on `CREATE SEMANTIC VIEW` to understand:
- Required clauses (TABLES, COLUMNS, COMMENT)
- How to define primary keys
- How to add verified queries
- Supported data types and constraints

### Step 0.2: Cortex Agent Syntax

Search for documentation on `CREATE AGENT` and Cortex Agent specifications to understand:
- The JSON specification format
- Required fields (type, models, tools, tool_resources)
- Optional fields (description, sample_questions, instructions)
- How to configure the `cortex_analyst_text_to_sql` tool type

---

## Prerequisites

Verify these Dynamic Tables exist in `UNDERSTOOD_DEMO.DYNAMIC_TABLES`:
- `MEMBER_SESSIONS`
- `RESOURCE_CATALOG`
- `SESSION_ENGAGEMENT_DETAIL`
- `MEMBER_ENGAGEMENT_SUMMARY`
- `ENGAGEMENT_DASHBOARD`

---

## Phase 1: Examine Tables and Create Semantic View

### Step 1.1: Discover Table Schemas

Run `DESCRIBE TABLE` on each Dynamic Table to understand the columns, data types, and structure:

```sql
DESCRIBE TABLE UNDERSTOOD_DEMO.DYNAMIC_TABLES.MEMBER_ENGAGEMENT_SUMMARY;
DESCRIBE TABLE UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_DASHBOARD;
```

Also examine sample data to understand the content:

```sql
SELECT * FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.MEMBER_ENGAGEMENT_SUMMARY LIMIT 5;
SELECT * FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_DASHBOARD;
```

### Step 1.2: Generate Semantic View SQL

Based on your examination, create a `CREATE SEMANTIC VIEW` statement that:

1. **Includes relevant tables** - Focus on `MEMBER_ENGAGEMENT_SUMMARY` (member-level metrics) and `ENGAGEMENT_DASHBOARD` (org-level KPIs)

2. **Adds descriptive comments** - Each table and column should have a `COMMENT` explaining what it represents in business terms

3. **Defines primary keys** - Identify unique identifiers for each table

4. **Creates verified queries** - Add 4-6 verified queries for common questions based on the data you discovered

Use this SQL structure:

```sql
CREATE OR REPLACE SEMANTIC VIEW UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_SEMANTIC_VIEW
  COMMENT = '<description of the semantic model>'
  TABLES (
    <fully_qualified_table_name>
      COMMENT = '<table description>'
      PRIMARY KEY (<key_column>)
      COLUMNS (
        <column_name> COMMENT = '<column description>',
        ...
      ),
    ...
  )
  VERIFIED QUERIES (
    '<query_name>' AS '<natural language question>'
      VERIFIED BY '<SQL query>',
    ...
  );

GRANT SELECT ON SEMANTIC VIEW UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_SEMANTIC_VIEW TO ROLE PUBLIC;
```

---

## Phase 2: Create Cortex Agent

After the semantic view is created, generate a `CREATE AGENT` statement based on what you learned about the data.

### Step 2.1: Design Agent Configuration

Based on the semantic view you created, determine appropriate values for:

1. **description** - A user-facing description (1-2 sentences) explaining what the agent does, shown in Snowflake Intelligence

2. **sample_questions** - Array of exactly 3 starter questions that showcase the agent's capabilities. Derive these from your verified queries or the data patterns you discovered.

3. **instructions** - Context-aware prompts:
   - `orchestration`: When to use the analyst tool
   - `response`: How to format answers (numbers, rankings, tone)
   - `system`: The agent's persona and domain context

### Step 2.2: Generate Agent SQL

Create the agent using this structure (fill in values based on your analysis):

```sql
CREATE OR REPLACE AGENT UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_AGENT
  COMMENT = '<brief agent description>'
  FROM SPECIFICATION $$
  {
    "type": "cortex_agents_core",
    "description": "<user-facing description for Snowflake Intelligence UI>",
    "sample_questions": [
      "<question 1 based on your data discovery>",
      "<question 2 based on your data discovery>",
      "<question 3 based on your data discovery>"
    ],
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "<when and how to use the analyst tool>",
      "response": "<formatting and presentation guidelines>",
      "system": "<agent persona and domain context>"
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "engagement_analyst",
          "description": "<describe what data the tool can query>"
        }
      }
    ],
    "tool_resources": {
      "engagement_analyst": {
        "semantic_view": "UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "UNDERSTOOD_DEMO_WH"
        },
        "query_timeout": 60
      }
    }
  }
  $$;

GRANT USAGE ON AGENT UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_AGENT TO ROLE PUBLIC;
```

---
