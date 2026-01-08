import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- PRE-CONFIG ---
st.set_page_config(
    page_title="Big Data Pipelines Dashboard",
    page_icon="🚀",
    layout="wide",
)

# --- HELPER: ROBUST COLUMN ACCESS ---
def get_col(df, target_name):
    """Cari kolom dengan normalisasi nama (case-insensitive, ignore spaces/underscores)"""
    if df is None or df.empty:
        return None
    target_clean = target_name.lower().replace(" ", "").replace("_", "")
    for col in df.columns:
        col_clean = str(col).lower().replace(" ", "").replace("_", "")
        if col_clean == target_clean:
            return col
    return None


# --- DATABASE CONNECTION ---
@st.cache_resource
def get_engine(db_name='elt_sales_db'):
    """Create database engine (only works locally)"""
    return create_engine(f'mysql+pymysql://root:@localhost/{db_name}')


@st.cache_data
def load_elt_data():
    """Load ELT data from database or CSV fallback"""
    try:
        # Try local database connection
        engine = get_engine('elt_sales_db')
        df = pd.read_sql("SELECT * FROM sales_processed", engine)
        d_col = get_col(df, 'Order Date')
        if d_col:
            df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        return df
    except Exception as db_error:
        # Fallback to CSV (for GitHub deployment)
        try:
            csv_path = os.path.join(BASE_DIR, "data", "sales_processed.csv")
            df = pd.read_csv(csv_path)
            d_col = get_col(df, 'Order Date')
            if d_col:
                df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
            return df
        except Exception as csv_error:
            st.warning(f"ELT data not found. DB Error: {db_error}. CSV Error: {csv_error}")
            return pd.DataFrame()


@st.cache_data
def load_etl_data():
    """Load ETL data from database or CSV fallback"""
    try:
        # Try database with JOIN query
        engine = get_engine('dw_sales')
        query = """
        SELECT f.*, d.order_date, c.region, c.country, i.item_type, ch.sales_channel 
        FROM fact_sales f 
        LEFT JOIN dim_date d ON f.date_id = d.date_id 
        LEFT JOIN dim_country c ON f.country_id = c.country_id 
        LEFT JOIN dim_item i ON f.item_id = i.item_id 
        LEFT JOIN dim_channel ch ON f.channel_id = ch.channel_id
        """
        df = pd.read_sql(query, engine)
        d_col = get_col(df, 'Order Date')
        if d_col:
            df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        return df
    except Exception as db_error:
        # Fallback to CSV
        try:
            csv_path = os.path.join(BASE_DIR, "data", "fact_sales.csv")
            df = pd.read_csv(csv_path)
            d_col = get_col(df, 'Order Date')
            if d_col:
                df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
            return df
        except Exception as csv_error:
            st.warning(f"ETL data not found. DB Error: {db_error}. CSV Error: {csv_error}")
            return pd.DataFrame()


# --- LOAD DATA ---
df_elt_raw = load_elt_data()
df_fact_raw = load_etl_data()

# Check if any data loaded
if df_elt_raw.empty and df_fact_raw.empty:
    st.error("❌ No data found. Please ensure CSV files exist in `/data/` folder:")
    st.code("data/sales_processed.csv\ndata/fact_sales.csv")
    st.stop()

# --- NORMALIZE ETL COLUMNS (ensure consistent naming) ---
if not df_fact_raw.empty:
    mapping = {}
    for target_name in ['Order Date', 'Region', 'Sales Channel', 'Item Type']:
        found_col = get_col(df_fact_raw, target_name)
        if found_col and found_col != target_name:
            mapping[found_col] = target_name
    if mapping:
        df_fact_raw = df_fact_raw.rename(columns=mapping)

# --- SIDEBAR: GLOBAL FILTERS ---
st.sidebar.header("🔍 Global Filters")

# ===== DATE FILTER =====
all_dates = []
for df_source in [df_elt_raw, df_fact_raw]:
    if not df_source.empty:
        date_col = get_col(df_source, 'Order Date')
        if date_col:
            dates = pd.to_datetime(df_source[date_col], errors='coerce').dropna()
            all_dates.extend(dates.tolist())

if all_dates:
    min_date = min(all_dates)
    max_date = max(all_dates)
    selected_range = st.sidebar.date_input(
        "📅 Select Date Range",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date()
    )
    # Ensure we have both start and end dates
    if len(selected_range) != 2:
        selected_range = [min_date.date(), max_date.date()]
else:
    selected_range = [None, None]

# ===== REGION FILTER =====
all_regions = []
for df_source in [df_elt_raw, df_fact_raw]:
    if not df_source.empty:
        reg_col = get_col(df_source, 'Region')
        if reg_col:
            all_regions.extend(df_source[reg_col].dropna().astype(str).unique().tolist())
all_regions = sorted(set(all_regions)) if all_regions else []

selected_regions = st.sidebar.multiselect(
    "🌍 Regions",
    options=all_regions,
    default=all_regions
)

# ===== ITEM TYPE FILTER =====
all_items = []
for df_source in [df_elt_raw, df_fact_raw]:
    if not df_source.empty:
        item_col = get_col(df_source, 'Item Type')
        if item_col:
            all_items.extend(df_source[item_col].dropna().astype(str).unique().tolist())
all_items = sorted(set(all_items)) if all_items else []

selected_items = st.sidebar.multiselect(
    "📦 Item Types",
    options=all_items,
    default=all_items
)

# ===== SALES CHANNEL FILTER =====
all_channels = []
for df_source in [df_elt_raw, df_fact_raw]:
    if not df_source.empty:
        chan_col = get_col(df_source, 'Sales Channel')
        if chan_col:
            all_channels.extend(df_source[chan_col].dropna().astype(str).unique().tolist())
all_channels = sorted(set(all_channels)) if all_channels else []

selected_channels = st.sidebar.multiselect(
    "🛒 Sales Channels",
    options=all_channels,
    default=all_channels
)


# --- FILTERING FUNCTION ---
def apply_filters(df):
    """Apply all sidebar filters to dataframe"""
    if df is None or df.empty:
        return pd.DataFrame()

    # Get column names
    date_col = get_col(df, 'Order Date')
    reg_col = get_col(df, 'Region')
    item_col = get_col(df, 'Item Type')
    chan_col = get_col(df, 'Sales Channel')

    # Start with all rows selected
    mask = pd.Series(True, index=df.index)

    # Apply date filter
    if date_col and selected_range[0] and selected_range[1]:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        start = pd.to_datetime(selected_range[0])
        end = pd.to_datetime(selected_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        mask &= df[date_col].between(start, end)

    # Apply region filter
    if reg_col and selected_regions:
        mask &= df[reg_col].isin(selected_regions)

    # Apply item type filter
    if item_col and selected_items:
        mask &= df[item_col].isin(selected_items)

    # Apply channel filter
    if chan_col and selected_channels:
        mask &= df[chan_col].isin(selected_channels)

    return df.loc[mask].copy()


# Apply filters to both datasets
f_df_elt = apply_filters(df_elt_raw)
f_df_etl = apply_filters(df_fact_raw)

# --- MAIN UI ---
st.title("🏆 Sales Intelligence Dashboard")
st.markdown("Dashboard Implementasi Pipeline Big Data ETL dan ELT - Interactive Analytics")

tab1, tab2 = st.tabs(["🔴 ELT View (Warehouse)", "🔵 ETL View (Star Schema)"])


def render_content(df, pipeline_name):
    """Render dashboard content for a given pipeline"""
    if df is None or df.empty:
        st.warning(f"⚠️ No data available for {pipeline_name} pipeline after applying filters.")
        st.info("Try adjusting the filters in the sidebar or check if data files exist.")
        return

    main_color = "#FF4B4B" if pipeline_name == "ELT" else "#0083B0"

    # Map columns
    rev_col = get_col(df, 'Total Revenue')
    prof_col = get_col(df, 'Total Profit')
    units_col = get_col(df, 'Units Sold')
    date_col = get_col(df, 'Order Date')
    reg_col = get_col(df, 'Region')
    chan_col = get_col(df, 'Sales Channel')

    # Check critical columns
    if not rev_col or not prof_col or not units_col:
        st.error(f"❌ Critical columns missing! Available columns: {list(df.columns)}")
        return

    # === 1. KPI METRICS ===
    st.subheader("📊 Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    total_revenue = df[rev_col].fillna(0).sum()
    total_profit = df[prof_col].fillna(0).sum()
    total_units = df[units_col].fillna(0).sum()
    profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
    
    col1.metric("💰 Total Revenue", f"${total_revenue:,.0f}")
    col2.metric("📈 Total Profit", f"${total_profit:,.0f}")
    col3.metric("📦 Units Sold", f"{total_units:,.0f}")
    col4.metric("💹 Profit Margin", f"{profit_margin:.1f}%")

    st.markdown("---")

    # === 2. TIME TREND ===
    st.subheader("📅 Profit Trend Over Time")
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df_trend = df.dropna(subset=[date_col, prof_col])
        
        if not df_trend.empty:
            trend_data = df_trend.groupby(pd.Grouper(key=date_col, freq='M'))[prof_col].sum().reset_index()
            
            chart_trend = alt.Chart(trend_data).mark_area(
                color=main_color,
                opacity=0.4,
                line={'color': main_color}
            ).encode(
                x=alt.X(f'{date_col}:T', title='Month'),
                y=alt.Y(f'{prof_col}:Q', title='Monthly Profit ($)'),
                tooltip=[
                    alt.Tooltip(f'{date_col}:T', title='Month', format='%b %Y'),
                    alt.Tooltip(f'{prof_col}:Q', title='Profit', format='$,.0f')
                ]
            ).properties(height=350)
            
            st.altair_chart(chart_trend, use_container_width=True)
        else:
            st.info("No valid date data available for trend analysis.")
    else:
        st.warning("Date column not found in dataset.")

    st.markdown("---")

    # === 3. DISTRIBUTION & COMPARISON ===
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("📊 Profit Distribution")
        hist_chart = alt.Chart(df).mark_bar(color=main_color).encode(
            x=alt.X(f"{prof_col}:Q", bin=alt.Bin(maxbins=30), title="Profit Range"),
            y=alt.Y('count()', title='Frequency'),
            tooltip=['count()']
        ).properties(height=300)
        st.altair_chart(hist_chart, use_container_width=True)

    with col_right:
        st.subheader("🛒 Sales Channel Performance")
        if chan_col:
            channel_data = df.groupby(chan_col)[prof_col].sum().reset_index()
            
            channel_chart = alt.Chart(channel_data).mark_bar().encode(
                x=alt.X(f'{chan_col}:N', title='Channel', axis=alt.Axis(labelAngle=0)),
                y=alt.Y(f'{prof_col}:Q', title='Total Profit ($)'),
                color=alt.Color(f'{chan_col}:N', legend=None),
                tooltip=[
                    alt.Tooltip(f'{chan_col}:N', title='Channel'),
                    alt.Tooltip(f'{prof_col}:Q', title='Profit', format='$,.0f')
                ]
            ).properties(height=300)
            
            st.altair_chart(channel_chart, use_container_width=True)
        else:
            st.info("Sales Channel column not available.")

    # === 4. REGIONAL ANALYSIS ===
    st.subheader("🌍 Regional Performance")
    if reg_col:
        region_data = df.groupby(reg_col)[prof_col].sum().reset_index().sort_values(prof_col, ascending=False)
        
        region_chart = alt.Chart(region_data).mark_bar().encode(
            x=alt.X(f'{prof_col}:Q', title='Total Profit ($)'),
            y=alt.Y(f'{reg_col}:N', sort='-x', title='Region'),
            color=alt.Color(f'{reg_col}:N', legend=None),
            tooltip=[
                alt.Tooltip(f'{reg_col}:N', title='Region'),
                alt.Tooltip(f'{prof_col}:Q', title='Profit', format='$,.0f')
            ]
        ).properties(height=300)
        
        st.altair_chart(region_chart, use_container_width=True)
    else:
        st.info("Region column not available.")

    # === 5. DATA EXPLORER ===
    with st.expander("📄 View Raw Data (Top 100 Rows)"):
        st.dataframe(df.head(100), use_container_width=True)
        st.caption(f"Showing {min(100, len(df))} of {len(df):,} total rows")


# Render both tabs
with tab1:
    render_content(f_df_elt, "ELT")

with tab2:
    render_content(f_df_etl, "ETL")

# Footer
st.markdown("---")
st.caption("🎓 Kelompok 7 - Tugas Besar Big Data | Dashboard Requirements: KPI, Trend, Distribution, Comparison, Interactive Filters")