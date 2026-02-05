"""
================================================================================
USER ENGAGEMENT DASHBOARD - Streamlit Application
================================================================================
This dashboard displays real-time KPIs from the Dynamic Tables pipeline.
Data flows: RAW_INTERACTIONS -> 5 Dynamic Tables -> ENGAGEMENT_DASHBOARD

Key Features:
- Dual-mode session (Snowsight or local execution)
- Custom CSS styling for elevated KPI tiles
- Auto-refresh with 30-second cache
- Responsive layout with modular components
================================================================================
"""

import streamlit as st
import pandas as pd
import altair as alt
import os

# ==============================================================================
# STYLES MODULE
# ==============================================================================
# Custom CSS for enhanced visual presentation. Uses Streamlit's markdown 
# injection to apply styles. The KPI tiles (st.metric components) get:
# - Elevated card appearance with box-shadow
# - Rounded corners for modern look
# - Subtle background tint
# - Smooth hover transition for interactivity
# ==============================================================================

def load_styles():
    """
    Inject custom CSS into the Streamlit app.
    Targets the [data-testid="stMetric"] elements which wrap st.metric() calls.
    """
    st.markdown("""
        <style>
        /* KPI Tile Styling - Creates floating card effect */
        [data-testid="stMetric"] {
            background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
            border: 1px solid #e9ecef;
            border-radius: 12px;
            padding: 20px 15px;
            box-shadow: 
                0 4px 6px rgba(0, 0, 0, 0.07),
                0 10px 20px rgba(0, 0, 0, 0.04);
            transition: all 0.3s ease;
        }
        
        /* Hover state - tiles lift up slightly */
        [data-testid="stMetric"]:hover {
            transform: translateY(-4px);
            box-shadow: 
                0 8px 15px rgba(0, 0, 0, 0.1),
                0 20px 30px rgba(0, 0, 0, 0.06);
        }
        
        /* Metric label styling */
        [data-testid="stMetric"] label {
            font-weight: 600;
            color: #495057;
            font-size: 0.9rem;
        }
        
        /* Metric value styling */
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 1.8rem;
            font-weight: 700;
            color: #212529;
        }
        </style>
    """, unsafe_allow_html=True)


# ==============================================================================
# SESSION MODULE
# ==============================================================================
# Handles Snowflake connection for both Snowsight and local execution.
# In Snowsight: Uses get_active_session() from the notebook context
# Locally: Creates session using connection_name from environment variable
# ==============================================================================

def get_session():
    """
    Create a Snowpark session that works in both Snowsight and local environments.
    Falls back to connection_name if get_active_session() is not available.
    """
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except:
        from snowflake.snowpark import Session
        return Session.builder.config(
            "connection_name", 
            os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        ).create()


# ==============================================================================
# DATA MODULE  
# ==============================================================================
# Cached data loading functions. Each function queries the Dynamic Tables
# and caches results for 30 seconds to balance freshness with performance.
# TTL ensures data stays relatively current without hammering the database.
# ==============================================================================

def init_session():
    """Initialize session. Uses fully qualified names for Snowsight compatibility."""
    return get_session()

@st.cache_data(ttl=30)
def load_dashboard(_session):
    """
    Load org-wide KPIs from the final Dynamic Table.
    Returns a single row with all executive metrics.
    """
    return _session.sql("SELECT * FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.ENGAGEMENT_DASHBOARD").to_pandas()

@st.cache_data(ttl=30)
def load_top_members(_session, limit=10):
    """
    Load the most engaged members ranked by lifetime engagement.
    Used for the leaderboard table display.
    """
    return _session.sql(f"""
        SELECT member_name, member_type, region, total_sessions, 
               lifetime_engagement_seconds, favorite_topic, last_activity
        FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.MEMBER_ENGAGEMENT_SUMMARY 
        ORDER BY lifetime_engagement_seconds DESC 
        LIMIT {limit}
    """).to_pandas()

@st.cache_data(ttl=30)
def load_all_members(_session):
    """
    Load all members with full engagement details for exploration views.
    Used in the tabbed member exploration section.
    """
    return _session.sql("""
        SELECT 
            member_name, 
            member_type, 
            region, 
            total_sessions,
            lifetime_engagement_seconds,
            ROUND(lifetime_engagement_seconds / 60, 1) as engagement_minutes,
            favorite_topic, 
            last_activity,
            unique_resources
        FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.MEMBER_ENGAGEMENT_SUMMARY 
        ORDER BY lifetime_engagement_seconds DESC
    """).to_pandas()

@st.cache_data(ttl=30)
def load_member_breakdown(_session):
    """
    Load aggregated member counts by type and region.
    Used for the bar chart visualizations.
    """
    types = _session.sql("""
        SELECT member_type, COUNT(*) as count 
        FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.MEMBER_ENGAGEMENT_SUMMARY GROUP BY member_type
    """).to_pandas()
    regions = _session.sql("""
        SELECT region, COUNT(*) as count 
        FROM UNDERSTOOD_DEMO.DYNAMIC_TABLES.MEMBER_ENGAGEMENT_SUMMARY GROUP BY region
    """).to_pandas()
    return types, regions


# ==============================================================================
# UI COMPONENTS MODULE
# ==============================================================================
# Modular UI building blocks. Each function renders a specific section
# of the dashboard. This separation makes the code easier to maintain
# and allows for easy reordering or modification of dashboard sections.
# ==============================================================================

def render_header():
    """Render the dashboard header with title and description."""
    st.title("Member Engagement Dashboard")
    st.markdown("""
        Real-time visibility into member engagement powered by Dynamic Tables. 
        Track member interactions with resources, monitor session activity, and 
        identify high-value users to optimize content strategy.
    """)

def render_sidebar():
    """Render sidebar with refresh controls and app info."""
    with st.sidebar:
        st.header("Controls")
        if st.button("Refresh Data", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.caption("Data refreshes automatically via Snowflake Dynamic Tables.")

def render_kpi_tiles(row):
    """
    Render the main KPI metric tiles in two rows.
    Row 1: Core engagement metrics (members, sessions, hours, views)
    Row 2: Derived metrics (averages, member type breakdown)
    """
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Members", f"{row['ACTIVE_MEMBERS']:,}")
    col2.metric("Total Sessions", f"{row['TOTAL_SESSIONS']:,}")
    col3.metric("Engagement Hours", f"{row['TOTAL_ENGAGEMENT_HOURS']:,.1f}")
    col4.metric("Resource Views", f"{row['TOTAL_RESOURCE_VIEWS']:,}")
    
    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Avg Engagement (min)", f"{row['AVG_ENGAGEMENT_MINUTES_PER_MEMBER']:,.1f}")
    col6.metric("Premium Members", row['PREMIUM_MEMBERS'])
    col7.metric("Registered Members", row['REGISTERED_MEMBERS'])
    col8.metric("Free Members", row['FREE_MEMBERS'])

def render_charts(session):
    """
    Render the member breakdown charts side by side using Altair.
    Left: Members by membership type (free/registered/premium)
    Right: Members by geographic region
    """
    types_df, regions_df = load_member_breakdown(session)
    
    type_order = ['free', 'registered', 'premium']
    type_colors = ['#6366f1', '#8b5cf6', '#a855f7']
    region_colors = ['#06b6d4', '#14b8a6', '#10b981', '#22c55e', '#84cc16']
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("Members by Type")
        
        type_chart = alt.Chart(types_df).mark_bar(
            cornerRadiusTopLeft=8,
            cornerRadiusTopRight=8
        ).encode(
            x=alt.X('MEMBER_TYPE:N', 
                    sort=type_order,
                    axis=alt.Axis(labelAngle=0, title=None, labelFontSize=12)),
            y=alt.Y('COUNT:Q', 
                    axis=alt.Axis(title='Members', grid=True, labelFontSize=11)),
            color=alt.Color('MEMBER_TYPE:N',
                           scale=alt.Scale(domain=type_order, range=type_colors),
                           legend=None),
            tooltip=[
                alt.Tooltip('MEMBER_TYPE:N', title='Type'),
                alt.Tooltip('COUNT:Q', title='Members', format=',')
            ]
        ).properties(
            height=300
        ).configure_view(
            strokeWidth=0
        ).configure_axis(
            gridColor='#e5e7eb'
        )
        
        st.altair_chart(type_chart, use_container_width=True)
    
    with chart_col2:
        st.subheader("Members by Region")
        
        region_chart = alt.Chart(regions_df).mark_bar(
            cornerRadiusTopLeft=8,
            cornerRadiusTopRight=8
        ).encode(
            x=alt.X('REGION:N', 
                    sort='-y',
                    axis=alt.Axis(labelAngle=-45, title=None, labelFontSize=11)),
            y=alt.Y('COUNT:Q', 
                    axis=alt.Axis(title='Members', grid=True, labelFontSize=11)),
            color=alt.Color('REGION:N',
                           scale=alt.Scale(range=region_colors),
                           legend=None),
            tooltip=[
                alt.Tooltip('REGION:N', title='Region'),
                alt.Tooltip('COUNT:Q', title='Members', format=',')
            ]
        ).properties(
            height=300
        ).configure_view(
            strokeWidth=0
        ).configure_axis(
            gridColor='#e5e7eb'
        )
        
        st.altair_chart(region_chart, use_container_width=True)

# ==============================================================================
# MEMBER EXPLORATION MODULE - Tabbed Visualizations
# ==============================================================================
# Four interactive views for exploring member engagement data:
# 1. Ranked Bar Chart - Horizontal bars showing top members by engagement
# 2. Scatter Plot - Sessions vs engagement with type coloring
# 3. Filterable Table - Dynamic filtering by type and region
# 4. Card Layout - Visual member cards with key stats
# ==============================================================================

def render_member_exploration(session):
    """
    Render the tabbed member exploration section with 4 visualization options.
    Each tab provides a different way to explore and analyze member data.
    """
    st.subheader("Member Exploration")
    
    members_df = load_all_members(session)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Ranked Bar Chart", 
        "🔵 Scatter Plot", 
        "🔍 Filterable Table", 
        "🃏 Card Layout"
    ])
    
    with tab1:
        render_ranked_bar_chart(members_df)
    
    with tab2:
        render_scatter_plot(members_df)
    
    with tab3:
        render_filterable_table(members_df)
    
    with tab4:
        render_card_layout(members_df)

def render_ranked_bar_chart(df):
    """
    Horizontal bar chart ranking members by total engagement.
    Color-coded by member type, with detailed tooltips on hover.
    Shows top 15 members to avoid chart clutter.
    """
    top_df = df.head(15).copy()
    
    type_colors = {'free': '#6366f1', 'registered': '#8b5cf6', 'premium': '#a855f7'}
    
    chart = alt.Chart(top_df).mark_bar(
        cornerRadiusTopRight=6,
        cornerRadiusBottomRight=6
    ).encode(
        y=alt.Y('MEMBER_NAME:N', 
                sort='-x',
                axis=alt.Axis(title=None, labelFontSize=11)),
        x=alt.X('ENGAGEMENT_MINUTES:Q',
                axis=alt.Axis(title='Engagement (minutes)', grid=True)),
        color=alt.Color('MEMBER_TYPE:N',
                       scale=alt.Scale(domain=['free', 'registered', 'premium'], 
                                      range=['#6366f1', '#8b5cf6', '#a855f7']),
                       legend=alt.Legend(title='Member Type', orient='bottom')),
        tooltip=[
            alt.Tooltip('MEMBER_NAME:N', title='Name'),
            alt.Tooltip('MEMBER_TYPE:N', title='Type'),
            alt.Tooltip('REGION:N', title='Region'),
            alt.Tooltip('TOTAL_SESSIONS:Q', title='Sessions', format=','),
            alt.Tooltip('ENGAGEMENT_MINUTES:Q', title='Engagement (min)', format=',.1f'),
            alt.Tooltip('FAVORITE_TOPIC:N', title='Favorite Topic')
        ]
    ).properties(
        height=400
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        gridColor='#e5e7eb'
    )
    
    st.altair_chart(chart, use_container_width=True)

def render_scatter_plot(df):
    """
    Scatter plot showing relationship between sessions and engagement time.
    Point size represents unique resources viewed.
    Color indicates member type. Great for spotting patterns and outliers.
    """
    chart = alt.Chart(df).mark_circle(
        opacity=0.7,
        stroke='white',
        strokeWidth=1
    ).encode(
        x=alt.X('TOTAL_SESSIONS:Q',
                axis=alt.Axis(title='Total Sessions', grid=True)),
        y=alt.Y('ENGAGEMENT_MINUTES:Q',
                axis=alt.Axis(title='Engagement (minutes)', grid=True)),
        size=alt.Size('UNIQUE_RESOURCES:Q',
                     scale=alt.Scale(range=[50, 400]),
                     legend=alt.Legend(title='Resources Viewed')),
        color=alt.Color('MEMBER_TYPE:N',
                       scale=alt.Scale(domain=['free', 'registered', 'premium'],
                                      range=['#6366f1', '#8b5cf6', '#a855f7']),
                       legend=alt.Legend(title='Type', orient='right')),
        tooltip=[
            alt.Tooltip('MEMBER_NAME:N', title='Name'),
            alt.Tooltip('MEMBER_TYPE:N', title='Type'),
            alt.Tooltip('REGION:N', title='Region'),
            alt.Tooltip('TOTAL_SESSIONS:Q', title='Sessions'),
            alt.Tooltip('ENGAGEMENT_MINUTES:Q', title='Engagement (min)', format=',.1f'),
            alt.Tooltip('UNIQUE_RESOURCES:Q', title='Resources Viewed')
        ]
    ).properties(
        height=400
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        gridColor='#e5e7eb'
    ).interactive()
    
    st.altair_chart(chart, use_container_width=True)

def render_filterable_table(df):
    """
    Interactive table with filter controls for type and region.
    Allows drilling down into specific member segments.
    """
    filter_col1, filter_col2, filter_col3 = st.columns([2, 2, 1])
    
    with filter_col1:
        types = ['All'] + sorted(df['MEMBER_TYPE'].unique().tolist())
        selected_type = st.selectbox("Filter by Type", types, key="table_type")
    
    with filter_col2:
        regions = ['All'] + sorted(df['REGION'].unique().tolist())
        selected_region = st.selectbox("Filter by Region", regions, key="table_region")
    
    with filter_col3:
        top_n = st.selectbox("Show Top", [10, 25, 50, 100], key="table_limit")
    
    filtered_df = df.copy()
    if selected_type != 'All':
        filtered_df = filtered_df[filtered_df['MEMBER_TYPE'] == selected_type]
    if selected_region != 'All':
        filtered_df = filtered_df[filtered_df['REGION'] == selected_region]
    
    filtered_df = filtered_df.head(top_n)
    
    display_df = filtered_df[['MEMBER_NAME', 'MEMBER_TYPE', 'REGION', 
                               'TOTAL_SESSIONS', 'ENGAGEMENT_MINUTES', 
                               'FAVORITE_TOPIC']].copy()
    display_df.columns = ['Name', 'Type', 'Region', 'Sessions', 'Engagement (min)', 'Favorite Topic']
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(display_df)} of {len(df)} members")

def render_card_layout(df):
    """
    Visual card layout displaying member stats in a grid format.
    Each card shows avatar placeholder, name, type badge, and key metrics.
    Uses custom HTML/CSS for card styling.
    """
    top_members = df.head(12)
    
    type_colors = {
        'free': '#6366f1',
        'registered': '#8b5cf6', 
        'premium': '#a855f7'
    }
    
    cols = st.columns(4)
    
    for idx, (_, member) in enumerate(top_members.iterrows()):
        col = cols[idx % 4]
        badge_color = type_colors.get(member['MEMBER_TYPE'], '#6b7280')
        
        with col:
            st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
                    border: 1px solid #e5e7eb;
                    border-radius: 12px;
                    padding: 16px;
                    margin-bottom: 16px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                ">
                    <div style="display: flex; align-items: center; margin-bottom: 12px;">
                        <div style="
                            width: 40px; height: 40px;
                            background: linear-gradient(135deg, {badge_color} 0%, {badge_color}dd 100%);
                            border-radius: 50%;
                            display: flex; align-items: center; justify-content: center;
                            color: white; font-weight: bold; font-size: 16px;
                            margin-right: 12px;
                        ">{member['MEMBER_NAME'][0]}</div>
                        <div>
                            <div style="font-weight: 600; color: #1f2937; font-size: 14px;">
                                {member['MEMBER_NAME'][:18]}
                            </div>
                            <span style="
                                background: {badge_color}22;
                                color: {badge_color};
                                padding: 2px 8px;
                                border-radius: 12px;
                                font-size: 11px;
                                font-weight: 500;
                            ">{member['MEMBER_TYPE']}</span>
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 12px;">
                        <div>
                            <div style="color: #6b7280;">Sessions</div>
                            <div style="font-weight: 600; color: #1f2937;">{member['TOTAL_SESSIONS']}</div>
                        </div>
                        <div>
                            <div style="color: #6b7280;">Minutes</div>
                            <div style="font-weight: 600; color: #1f2937;">{member['ENGAGEMENT_MINUTES']:.0f}</div>
                        </div>
                        <div style="grid-column: span 2;">
                            <div style="color: #6b7280;">Favorite</div>
                            <div style="font-weight: 500; color: #374151; font-size: 11px;">
                                {member['FAVORITE_TOPIC'][:20]}
                            </div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

# ==============================================================================
# MAIN APPLICATION
# ==============================================================================
# Entry point that orchestrates all modules. Sets up the page config,
# loads styles, initializes the session, and renders all UI components.
# Uses top-level tabs to separate Overview from Member Exploration.
# ==============================================================================

def main():
    """Main application entry point."""
    st.set_page_config(page_title="Engagement Dashboard", layout="wide")
    load_styles()
    
    session = init_session()
    
    render_sidebar()
    render_header()
    
    dashboard = load_dashboard(session)
    
    if len(dashboard) > 0:
        row = dashboard.iloc[0]
        
        st.caption(f"Last Updated: {row['REPORT_GENERATED_AT']}")
        
        overview_tab, explore_tab = st.tabs(["📈 Overview", "🔎 Member Exploration"])
        
        with overview_tab:
            st.markdown("---")
            render_kpi_tiles(row)
            st.markdown("---")
            render_charts(session)
        
        with explore_tab:
            render_member_exploration(session)
    
    else:
        st.error("No data found in ENGAGEMENT_DASHBOARD. Run the Dynamic Tables notebook first!")
        st.info("Go to the notebook and execute all cells to create the pipeline.")


if __name__ == "__main__":
    main()
