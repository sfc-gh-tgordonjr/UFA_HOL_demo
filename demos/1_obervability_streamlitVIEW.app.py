"""
Observability Dashboard for Understood
=======================================
Demonstrates Snowflake observability (Logs, Traces, Spans, Metrics)
while showcasing Streamlit features and components.

For the February 2026 Hands-On Session
"""

import streamlit as st
import pandas as pd
import datetime
import random
import time

# ============================================================================
# PAGE CONFIG - Must be first Streamlit command
# ============================================================================
st.set_page_config(
    page_title="Observability Dashboard | Understood",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# MOCK DATA GENERATORS
# ============================================================================

def generate_mock_families():
    """Generate sample family intake data"""
    families = [
        {"family_id": 1, "family_name": "Johnson Family", "child_age": 8, "reading_score": 65, "math_score": 80, "focus_score": 55},
        {"family_id": 2, "family_name": "Chen Family", "child_age": 10, "reading_score": 45, "math_score": 50, "focus_score": 60},
        {"family_id": 3, "family_name": "Garcia Family", "child_age": 7, "reading_score": 85, "math_score": 90, "focus_score": 88},
        {"family_id": 4, "family_name": "Wilson Family", "child_age": 9, "reading_score": 40, "math_score": 35, "focus_score": 45},
        {"family_id": 5, "family_name": "Brown Family", "child_age": 11, "reading_score": 70, "math_score": 65, "focus_score": 72},
    ]
    return pd.DataFrame(families)

def categorize_support_level(reading_score, math_score, focus_score):
    """Simulate the UDF logic"""
    areas = []
    if reading_score < 70:
        areas.append("Reading")
    if math_score < 70:
        areas.append("Math")
    if focus_score < 70:
        areas.append("Focus")
    
    avg = (reading_score + math_score + focus_score) / 3
    if avg < 50:
        level = "Intensive"
    elif avg < 70:
        level = "Moderate"
    else:
        level = "Light Touch"
    
    return f"{level}: {', '.join(areas) if areas else 'General Support'}"

def generate_mock_logs(family_data):
    """Generate mock log entries"""
    logs = []
    base_time = datetime.datetime.now() - datetime.timedelta(minutes=5)
    
    logs.append({
        "timestamp": base_time,
        "logger_name": "intake_processor",
        "log_level": "INFO",
        "message": '{"event": "batch_started"}'
    })
    
    logs.append({
        "timestamp": base_time + datetime.timedelta(seconds=1),
        "logger_name": "intake_processor", 
        "log_level": "INFO",
        "message": f'{{"event": "records_found", "count": {len(family_data)}}}'
    })
    
    for idx, row in family_data.iterrows():
        t = base_time + datetime.timedelta(seconds=2 + idx*2)
        logs.append({
            "timestamp": t,
            "logger_name": "support_categorizer",
            "log_level": "INFO",
            "message": f'{{"event": "categorization_started", "reading": {row["reading_score"]}, "math": {row["math_score"]}, "focus": {row["focus_score"]}}}'
        })
        logs.append({
            "timestamp": t + datetime.timedelta(milliseconds=500),
            "logger_name": "support_categorizer",
            "log_level": "INFO", 
            "message": f'{{"event": "categorization_complete", "result": "{categorize_support_level(row["reading_score"], row["math_score"], row["focus_score"])}"}}'
        })
        logs.append({
            "timestamp": t + datetime.timedelta(seconds=1),
            "logger_name": "intake_processor",
            "log_level": "INFO",
            "message": f'{{"event": "family_processed", "family_id": {row["family_id"]}, "family_name": "{row["family_name"]}"}}'
        })
    
    logs.append({
        "timestamp": base_time + datetime.timedelta(seconds=15),
        "logger_name": "intake_processor",
        "log_level": "INFO",
        "message": f'{{"event": "batch_complete", "total": {len(family_data)}}}'
    })
    
    return pd.DataFrame(logs)

def generate_mock_traces(family_data):
    """Generate mock trace events"""
    traces = []
    base_time = datetime.datetime.now() - datetime.timedelta(minutes=5)
    
    traces.append({
        "timestamp": base_time,
        "event_name": "batch_processing_started",
        "event_details": "{}"
    })
    
    for idx, row in family_data.iterrows():
        t = base_time + datetime.timedelta(seconds=2 + idx*2)
        avg = (row["reading_score"] + row["math_score"] + row["focus_score"]) / 3
        areas = sum([1 for s in [row["reading_score"], row["math_score"], row["focus_score"]] if s < 70])
        
        traces.append({
            "timestamp": t + datetime.timedelta(milliseconds=500),
            "event_name": "categorization_complete",
            "event_details": f'{{"level": "{categorize_support_level(row["reading_score"], row["math_score"], row["focus_score"]).split(":")[0]}", "areas_count": {areas}, "avg_score": {avg:.1f}}}'
        })
        traces.append({
            "timestamp": t + datetime.timedelta(seconds=1),
            "event_name": "family_processed",
            "event_details": f'{{"family_id": {row["family_id"]}, "category": "{categorize_support_level(row["reading_score"], row["math_score"], row["focus_score"])}"}}'
        })
    
    traces.append({
        "timestamp": base_time + datetime.timedelta(seconds=15),
        "event_name": "batch_processing_complete",
        "event_details": f'{{"total_processed": {len(family_data)}}}'
    })
    
    return pd.DataFrame(traces)

def generate_mock_spans(family_data):
    """Generate mock span attributes"""
    spans = []
    base_time = datetime.datetime.now() - datetime.timedelta(minutes=5)
    
    spans.append({
        "timestamp": base_time,
        "function_name": "PROCESS_FAMILY_INTAKES",
        "custom_attributes": f'{{"batch.status": "started", "batch.record_count": {len(family_data)}}}'
    })
    
    for idx, row in family_data.iterrows():
        t = base_time + datetime.timedelta(seconds=2 + idx*2)
        result = categorize_support_level(row["reading_score"], row["math_score"], row["focus_score"])
        spans.append({
            "timestamp": t,
            "function_name": "CATEGORIZE_SUPPORT_LEVEL",
            "custom_attributes": f'{{"input.reading_score": {row["reading_score"]}, "input.math_score": {row["math_score"]}, "input.focus_score": {row["focus_score"]}, "output.result": "{result}"}}'
        })
    
    spans.append({
        "timestamp": base_time + datetime.timedelta(seconds=15),
        "function_name": "PROCESS_FAMILY_INTAKES",
        "custom_attributes": f'{{"batch.status": "completed", "batch.processed_count": {len(family_data)}}}'
    })
    
    return pd.DataFrame(spans)

def generate_mock_metrics():
    """Generate mock CPU/memory metrics"""
    metrics = []
    base_time = datetime.datetime.now() - datetime.timedelta(minutes=5)
    
    for i in range(10):
        t = base_time + datetime.timedelta(seconds=i*1.5)
        metrics.append({
            "timestamp": t,
            "function_name": "CATEGORIZE_SUPPORT_LEVEL",
            "metric_name": "process.cpu.utilization",
            "metric_value": round(random.uniform(0.05, 0.25), 3)
        })
        metrics.append({
            "timestamp": t,
            "function_name": "CATEGORIZE_SUPPORT_LEVEL",
            "metric_name": "process.memory.usage",
            "metric_value": random.randint(40000000, 80000000)
        })
    
    for i in range(5):
        t = base_time + datetime.timedelta(seconds=i*3)
        metrics.append({
            "timestamp": t,
            "function_name": "PROCESS_FAMILY_INTAKES",
            "metric_name": "process.cpu.utilization",
            "metric_value": round(random.uniform(0.10, 0.35), 3)
        })
        metrics.append({
            "timestamp": t,
            "function_name": "PROCESS_FAMILY_INTAKES",
            "metric_name": "process.memory.usage",
            "metric_value": random.randint(60000000, 120000000)
        })
    
    return pd.DataFrame(metrics)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================
if "families_processed" not in st.session_state:
    st.session_state.families_processed = False
if "processing_time" not in st.session_state:
    st.session_state.processing_time = None

# ============================================================================
# SIDEBAR
# ============================================================================
with st.sidebar:
    st.image("https://www.understood.org/images/understood-logo.svg", width=200)
    st.title("Observability Demo")
    
    st.divider()
    
    st.subheader("About This Demo")
    st.markdown("""
    This dashboard demonstrates:
    - **UDFs** - User-Defined Functions
    - **Stored Procedures** - Batch processing
    - **Observability** - Logs, Traces, Metrics
    
    All running in **Snowflake**!
    """)
    
    st.divider()
    
    st.subheader("Streamlit Features Shown")
    with st.expander("See the list", expanded=False):
        st.markdown("""
        - `st.tabs()` - Tabbed interface
        - `st.metric()` - KPI display
        - `st.dataframe()` - Interactive tables
        - `st.bar_chart()` - Visualizations
        - `st.form()` - User input
        - `st.session_state` - State management
        - `st.columns()` - Layout
        - `st.expander()` - Collapsible sections
        - `st.progress()` - Progress bars
        - `st.toast()` - Notifications
        """)
    
    st.divider()
    
    if st.button("Reset Demo", type="secondary", use_container_width=True):
        st.session_state.families_processed = False
        st.session_state.processing_time = None
        st.rerun()

# ============================================================================
# MAIN CONTENT
# ============================================================================
st.title("Observability Dashboard")
st.caption("Monitoring UDFs and Stored Procedures with Logs, Traces, and Metrics")

with st.expander("What is this app doing?", expanded=True):
    st.markdown("""
    ### Understanding Snowflake Observability
    
    This dashboard demonstrates how to monitor custom code running in Snowflake. When you write **User-Defined Functions (UDFs)** 
    and **Stored Procedures**, you need visibility into what your code is doing, how long it takes, and whether it succeeded or failed.
    
    Snowflake provides an **Event Table** that automatically captures telemetry data from your code. This app visualizes four types of telemetry:
    
    | Telemetry Type | What It Captures | How You Create It | Use Case |
    |----------------|------------------|-------------------|----------|
    | **Logs** | Human-readable messages | `logging.info("message")` | Debugging, status updates, error messages |
    | **Traces** | Structured events at key moments | `telemetry.add_event("name", {data})` | Marking milestones, tracking workflow steps |
    | **Spans** | Attributes attached to function execution | `telemetry.set_span_attribute("key", value)` | Recording inputs, outputs, context |
    | **Metrics** | CPU and memory usage | Automatic (no code needed) | Performance monitoring, capacity planning |
    
    ---
    
    **Demo Scenario:** Understood processes family intake assessments. Each child receives scores in Reading, Math, and Focus. 
    A **UDF** categorizes the support level needed, and a **Stored Procedure** processes families in batch. 
    All telemetry flows into the Event Table where we can query and visualize it.
    
    **How to use this app:**
    1. Click "Run Procedure" on the Family Intakes tab to simulate processing
    2. Explore the Logs, Traces, Spans, and Metrics tabs to see the generated telemetry
    3. Use the UDF Tester to see how the categorization function works
    """)

# Load mock data
families_df = generate_mock_families()

# ============================================================================
# TOP METRICS ROW
# ============================================================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Families in Queue",
        value=len(families_df),
        delta="+2 today" if not st.session_state.families_processed else "0 pending"
    )

with col2:
    st.metric(
        label="Processing Status",
        value="Complete" if st.session_state.families_processed else "Pending",
        delta="All done!" if st.session_state.families_processed else None
    )

with col3:
    if st.session_state.processing_time:
        st.metric(
            label="Processing Time",
            value=f"{st.session_state.processing_time:.1f}s",
            delta="-0.3s vs avg"
        )
    else:
        st.metric(label="Processing Time", value="--")

with col4:
    st.metric(
        label="Telemetry Events",
        value="47" if st.session_state.families_processed else "0",
        delta="+47" if st.session_state.families_processed else None
    )

st.divider()

# ============================================================================
# MAIN TABS
# ============================================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Family Intakes", 
    "Logs", 
    "Traces", 
    "Spans", 
    "Metrics"
])

# ----------------------------------------------------------------------------
# TAB 1: Family Intakes
# ----------------------------------------------------------------------------
with tab1:
    st.subheader("Family Intake Processing")
    
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.markdown("#### Families Awaiting Processing")
        
        display_df = families_df.copy()
        if st.session_state.families_processed:
            display_df["support_category"] = display_df.apply(
                lambda row: categorize_support_level(row["reading_score"], row["math_score"], row["focus_score"]),
                axis=1
            )
            display_df["status"] = "Processed"
        else:
            display_df["support_category"] = "Pending..."
            display_df["status"] = "Waiting"
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "family_id": st.column_config.NumberColumn("ID", width="small"),
                "family_name": st.column_config.TextColumn("Family", width="medium"),
                "child_age": st.column_config.NumberColumn("Age", width="small"),
                "reading_score": st.column_config.ProgressColumn("Reading", min_value=0, max_value=100, format="%d"),
                "math_score": st.column_config.ProgressColumn("Math", min_value=0, max_value=100, format="%d"),
                "focus_score": st.column_config.ProgressColumn("Focus", min_value=0, max_value=100, format="%d"),
                "support_category": st.column_config.TextColumn("Support Category", width="large"),
                "status": st.column_config.TextColumn("Status", width="small"),
            }
        )
    
    with col_right:
        st.markdown("#### Run Processing")
        
        with st.container(border=True):
            st.markdown("**Stored Procedure:**")
            st.code("CALL process_family_intakes()", language="sql")
            
            if not st.session_state.families_processed:
                if st.button("Run Procedure", type="primary", use_container_width=True):
                    progress_bar = st.progress(0, text="Starting batch...")
                    start_time = time.time()
                    
                    for i, row in families_df.iterrows():
                        progress_bar.progress(
                            (i + 1) / len(families_df),
                            text=f"Processing {row['family_name']}..."
                        )
                        time.sleep(0.3)
                    
                    st.session_state.processing_time = time.time() - start_time
                    st.session_state.families_processed = True
                    progress_bar.progress(100, text="Complete!")
                    st.toast("All families processed!")
                    time.sleep(0.5)
                    st.rerun()
            else:
                st.success("Processing complete!")
                st.info(f"Processed {len(families_df)} families in {st.session_state.processing_time:.1f}s")
        
        st.markdown("#### Test UDF Directly")
        
        with st.form("udf_tester"):
            st.markdown("**categorize_support_level()**")
            reading = st.slider("Reading Score", 0, 100, 65)
            math = st.slider("Math Score", 0, 100, 75)
            focus = st.slider("Focus Score", 0, 100, 55)
            
            submitted = st.form_submit_button("Test UDF", use_container_width=True)
            
            if submitted:
                result = categorize_support_level(reading, math, focus)
                st.success(f"**Result:** {result}")

# ----------------------------------------------------------------------------
# TAB 2: Logs
# ----------------------------------------------------------------------------
with tab2:
    st.subheader("Log Entries")
    
    st.info("""
    **Logs** are messages written with `logging.info()`. They capture what happened in human-readable form.
    
    **Event Table Filter:** `RECORD_TYPE = 'LOG'`
    """)
    
    if st.session_state.families_processed:
        logs_df = generate_mock_logs(families_df)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            logger_filter = st.multiselect(
                "Filter by Logger",
                options=logs_df["logger_name"].unique(),
                default=logs_df["logger_name"].unique()
            )
        
        filtered_logs = logs_df[logs_df["logger_name"].isin(logger_filter)]
        
        st.dataframe(
            filtered_logs,
            use_container_width=True,
            hide_index=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Timestamp", format="HH:mm:ss.SSS"),
                "logger_name": st.column_config.TextColumn("Logger"),
                "log_level": st.column_config.TextColumn("Level"),
                "message": st.column_config.TextColumn("Message", width="large"),
            }
        )
        
        st.caption(f"Showing {len(filtered_logs)} of {len(logs_df)} log entries")
    else:
        st.warning("Run the stored procedure first to generate log data!")

# ----------------------------------------------------------------------------
# TAB 3: Traces
# ----------------------------------------------------------------------------
with tab3:
    st.subheader("Trace Events")
    
    st.info("""
    **Traces** are structured events recorded with `telemetry.add_event()`. They mark milestones with structured data.
    
    **Event Table Filter:** `RECORD_TYPE = 'SPAN_EVENT'`
    """)
    
    if st.session_state.families_processed:
        traces_df = generate_mock_traces(families_df)
        
        st.dataframe(
            traces_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Timestamp", format="HH:mm:ss.SSS"),
                "event_name": st.column_config.TextColumn("Event Name"),
                "event_details": st.column_config.TextColumn("Event Details", width="large"),
            }
        )
        
        with st.expander("Event Distribution"):
            event_counts = traces_df["event_name"].value_counts()
            st.bar_chart(event_counts)
    else:
        st.warning("Run the stored procedure first to generate trace data!")

# ----------------------------------------------------------------------------
# TAB 4: Spans
# ----------------------------------------------------------------------------
with tab4:
    st.subheader("Span Attributes")
    
    st.info("""
    **Spans** represent function executions. Attributes added with `telemetry.set_span_attribute()` appear here.
    
    **Event Table Filter:** `RECORD_TYPE = 'SPAN'`
    """)
    
    if st.session_state.families_processed:
        spans_df = generate_mock_spans(families_df)
        
        function_filter = st.selectbox(
            "Filter by Function",
            options=["All"] + list(spans_df["function_name"].unique())
        )
        
        if function_filter != "All":
            spans_df = spans_df[spans_df["function_name"] == function_filter]
        
        st.dataframe(
            spans_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Timestamp", format="HH:mm:ss.SSS"),
                "function_name": st.column_config.TextColumn("Function"),
                "custom_attributes": st.column_config.TextColumn("Custom Attributes", width="large"),
            }
        )
    else:
        st.warning("Run the stored procedure first to generate span data!")

# ----------------------------------------------------------------------------
# TAB 5: Metrics
# ----------------------------------------------------------------------------
with tab5:
    st.subheader("Resource Metrics")
    
    st.info("""
    **Metrics** capture CPU and memory usage automatically. No code required - just enable `METRIC_LEVEL = ALL`.
    
    **Event Table Filter:** `RECORD_TYPE = 'METRIC'`
    """)
    
    if st.session_state.families_processed:
        metrics_df = generate_mock_metrics()
        
        metric_col1, metric_col2 = st.columns(2)
        
        with metric_col1:
            st.markdown("#### CPU Utilization")
            cpu_df = metrics_df[metrics_df["metric_name"] == "process.cpu.utilization"].copy()
            cpu_df = cpu_df.set_index("timestamp")
            st.line_chart(cpu_df["metric_value"], use_container_width=True)
        
        with metric_col2:
            st.markdown("#### Memory Usage")
            mem_df = metrics_df[metrics_df["metric_name"] == "process.memory.usage"].copy()
            mem_df["metric_value_mb"] = mem_df["metric_value"] / 1_000_000
            mem_df = mem_df.set_index("timestamp")
            st.line_chart(mem_df["metric_value_mb"], use_container_width=True)
        
        with st.expander("Raw Metrics Data"):
            st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    else:
        st.warning("Run the stored procedure first to generate metrics data!")

# ============================================================================
# FOOTER
# ============================================================================
st.divider()

col_footer1, col_footer2, col_footer3 = st.columns(3)

with col_footer1:
    st.markdown("**Telemetry Types**")
    st.markdown("""
    | Type | How to Emit |
    |------|-------------|
    | Logs | `logging.info()` |
    | Traces | `telemetry.add_event()` |
    | Spans | `telemetry.set_span_attribute()` |
    | Metrics | Automatic |
    """)

with col_footer2:
    st.markdown("**Event Table Filters**")
    st.markdown("""
    ```sql
    -- Logs
    WHERE RECORD_TYPE = 'LOG'
    
    -- Trace Events
    WHERE RECORD_TYPE = 'SPAN_EVENT'
    
    -- Span Attributes
    WHERE RECORD_TYPE = 'SPAN'
    
    -- Metrics
    WHERE RECORD_TYPE = 'METRIC'
    ```
    """)

with col_footer3:
    st.markdown("**Understood for All**")
    st.markdown("""
    Supporting families with learning differences through:
    - Automated intake processing
    - Support level categorization
    - Observable, debuggable code
    """)
