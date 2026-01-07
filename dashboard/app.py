import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt

# --- PRE-CONFIG ---
st.set_page_config(
    page_title="Big Data Pipelines Dashboard",
    page_icon="🚀",
    layout="wide",
)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_engine(db_name='elt_sales_db'):
    return create_engine(f'mysql+pymysql://root:@localhost/{db_name}')

@st.cache_data
def load_elt_data():
    engine = get_engine('elt_sales_db')
    try:
        df = pd.read_sql("SELECT * FROM raw_sales_api", engine)
        if 'Order Date' in df.columns:
            df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"ELT load error: {e}")
        return pd.DataFrame()


@st.cache_data
def load_etl_data():
    engine = get_engine('dw_sales')
    # Star Schema needs joins to get descriptors
    query = """
    SELECT 
        f.*, 
        d.order_date,
        c.region, c.country,
        i.item_type,
        ch.sales_channel
    FROM fact_sales f
    LEFT JOIN dim_date d ON f.date_id = d.date_id
    LEFT JOIN dim_country c ON f.country_id = c.country_id
    LEFT JOIN dim_item i ON f.item_id = i.item_id
    LEFT JOIN dim_channel ch ON f.channel_id = ch.channel_id
    """
    try:
        df = pd.read_sql(query, engine)
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        return df
    except:
        # Fallback to fact table only if joins fail
        try:
            return pd.read_sql("SELECT * FROM fact_sales", engine)
        except:
            return pd.DataFrame()

# --- HELPER: ROBUST COLUMN ACCESS ---
def get_col(df, target_name):
    """
    Searches for a column name in a case-insensitive and space-insensitive way.
    Example: 'Total Revenue' will match 'total_revenue', 'totalrevenue', 'Total Revenue', etc.
    """
    if df.empty: return None
    target_clean = target_name.lower().replace(" ", "").replace("_", "")
    for col in df.columns:
        col_clean = str(col).lower().replace(" ", "").replace("_", "")
        if col_clean == target_clean:
            return col
    return None

# --- LOADING DATA ---
df_elt_raw = load_elt_data()
df_fact_raw = load_etl_data()

if df_elt_raw.empty and df_fact_raw.empty:
    st.error("No data found. Please run your ETL/ELT pipelines first.")
    st.stop()

# --- SIDEBAR: GLOBAL FILTERS ---
st.sidebar.header("🔍 Global Filters")

# Consolidate filter options
all_dates = []
for d_df in [df_elt_raw, df_fact_raw]:
    if not d_df.empty:
        dc = get_col(d_df, 'Order Date')
        if dc:
            valid_dates = pd.to_datetime(d_df[dc]).dropna().tolist()
            all_dates.extend(valid_dates)

if all_dates:
    all_dates = pd.to_datetime(all_dates)
    min_date, max_date = all_dates.min(), all_dates.max()
    selected_range = st.sidebar.date_input("Select Date Horizon", [min_date.date(), max_date.date()])
else:
    selected_range = [None, None]

# Regions
all_reg = sorted([str(x) for x in df_elt_raw['Region'].dropna().unique()]) if 'Region' in df_elt_raw.columns else []
if not all_reg and 'region' in df_fact_raw.columns:
    all_reg = sorted([str(x) for x in df_fact_raw['region'].dropna().unique()])
selected_regions = st.sidebar.multiselect("Select Regions", options=all_reg, default=all_reg)

# Items
all_items = sorted([str(x) for x in df_elt_raw['Item Type'].dropna().unique()]) if 'Item Type' in df_elt_raw.columns else []
if not all_items and 'item_type' in df_fact_raw.columns:
    all_items = sorted([str(x) for x in df_fact_raw['item_type'].dropna().unique()])
selected_items = st.sidebar.multiselect("Select Item Types", options=all_items, default=all_items)

# Channels
all_chan = sorted([str(x) for x in df_elt_raw['Sales Channel'].dropna().unique()]) if 'Sales Channel' in df_elt_raw.columns else []
if not all_chan and 'sales_channel' in df_fact_raw.columns:
    all_chan = sorted([str(x) for x in df_fact_raw['sales_channel'].dropna().unique()])
selected_channels = st.sidebar.multiselect("Sales Channel", options=all_chan, default=all_chan)

# --- FILTERING ---
def apply_filters(df):
    if df.empty: return df
    d_col = get_col(df, 'Order Date')
    r_col = get_col(df, 'Region')
    i_col = get_col(df, 'Item Type')
    c_col = get_col(df, 'Sales Channel')
    
    mask = pd.Series([True] * len(df))
    if d_col and len(selected_range) == 2:
        df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        mask &= (df[d_col].dt.date >= selected_range[0]) & (df[d_col].dt.date <= selected_range[1])
    if r_col:
        mask &= df[r_col].isin(selected_regions)
    if i_col:
        mask &= df[i_col].isin(selected_items)
    if c_col:
        mask &= df[c_col].isin(selected_channels)
    return df[mask]

f_df_elt = apply_filters(df_elt_raw)
f_df_etl = apply_filters(df_fact_raw)

# --- MAIN UI ---
st.title("🏆 Sales Intelligence Dashboard")
st.markdown("Dashboard untuk Implementasi Pipeline Big Data ETL dan ELT pada Studi Kasus Catatan Penjualan dengan Interactive Filters.")
tab1, tab2 = st.tabs(["🔴 ELT View (Warehouse)", "🔵 ETL View (Star Schema)"])

def render_content(df, p_name):
    if df.empty:
        st.warning(f"No data for {p_name} pipeline. Ensure filters are not too restrictive.")
        return

    # Column Mapping
    rev = get_col(df, 'Total Revenue')
    prof = get_col(df, 'Total Profit')
    units = get_col(df, 'Units Sold')
    date_c = get_col(df, 'Order Date')
    item_c = get_col(df, 'Item Type')
    reg_c = get_col(df, 'Region')
    chan_c = get_col(df, 'Sales Channel')
    prio_c = get_col(df, 'Order Priority')

    if not rev or not prof or not units:
        st.error(f"Critical columns for {p_name} viz not found.")
        return

    # 1. KPI UTAMA
    st.subheader("1. KPI Utama")
    k1, k2, k3, k4 = st.columns(4)
    t_rev = df[rev].sum()
    t_prof = df[prof].sum()
    t_units = df[units].sum()
    k1.metric("Total Revenue", f"${t_rev:,.0f}")
    k2.metric("Total Profit", f"${t_prof:,.0f}")
    k3.metric("Units Sold", f"{t_units:,.0f}")
    k4.metric("Profit Margin", f"{(t_prof/t_rev*100):.1f}%" if t_rev != 0 else "0%")

    st.markdown("---")

    # 2. TREN WAKTU
    st.subheader("2. Tren Waktu")
    if date_c:
        trend = df.groupby(pd.Grouper(key=date_c, freq='M'))[prof].sum().reset_index()
        m_color = "#FF4B4B" if p_name == "ELT" else "#0083B0"
        chart_trend = alt.Chart(trend).mark_area(
            color=m_color, opacity=0.4, line={'color': m_color}
        ).encode(
            x=alt.X(f'{date_c}:T', title="Month"),
            y=alt.Y(f'{prof}:Q', title="Monthly Profit")
        ).properties(height=300)
        st.altair_chart(chart_trend, use_container_width=True)

    st.markdown("---")

    # 3. DISTRIBUSI & 4. PERBANDINGAN
    col_dist, col_comp = st.columns(2)
    
    with col_dist:
        st.subheader("3. Distribusi")
        # Profit Distribution (Histogram)
        st.write("**Profit Distribution (Histogram)**")
        dist_chart = alt.Chart(df).mark_bar(color=m_color).encode(
            alt.X(f"{prof}:Q", bin=True, title="Profit Bins"),
            y='count()',
        ).properties(height=300)
        st.altair_chart(dist_chart, use_container_width=True)
        
        # Order Priority (Pie)
        if prio_c:
            st.write("**Order Priority (Pie)**")
            prio_data = df[prio_c].value_counts().reset_index()
            prio_data.columns = ['Priority', 'Count']
            chart_prio = alt.Chart(prio_data).mark_arc().encode(
                theta=alt.Theta(field="Count", type="quantitative"),
                color=alt.Color(field="Priority", type="nominal"),
                tooltip=['Priority', 'Count']
            ).properties(height=300)
            st.altair_chart(chart_prio, use_container_width=True)

    with col_comp:
        st.subheader("4. Perbandingan")
        # Sales Channel Performance
        if chan_c:
            st.write("**Online vs Offline Performance**")
            chan_data = df.groupby(chan_c)[prof].sum().reset_index()
            chart_chan = alt.Chart(chan_data).mark_bar().encode(
                x=alt.X(f'{chan_c}:N', title="Sales Channel"),
                y=alt.Y(f'{prof}:Q', title="Total Profit"),
                color=alt.Color(f'{chan_c}:N', legend=None),
                tooltip=[chan_c, prof]
            ).properties(height=300)
            st.altair_chart(chart_chan, use_container_width=True)
            
        # Regional Comparison
        if reg_c:
            st.write("**Regional Profit Contribution**")
            reg_data = df.groupby(reg_c)[prof].sum().reset_index()
            chart_reg = alt.Chart(reg_data).mark_bar().encode(
                x=alt.X(f'{prof}:Q', title="Revenue"),
                y=alt.Y(f'{reg_c}:N', sort='-x', title="Region"),
                color=alt.Color(f'{reg_c}:N', legend=None),
                tooltip=[reg_c, prof]
            ).properties(height=300)
            st.altair_chart(chart_reg, use_container_width=True)

    # 5. EXPLORER
    with st.expander("📄 Data Explorer & Raw Records"):
        st.write(f"Showing top 100 rows filtered for {p_name} pipeline.")
        st.dataframe(df.head(100), use_container_width=True)

with tab1: render_content(f_df_elt, "ELT")
with tab2: render_content(f_df_etl, "ETL")

st.markdown("---")
st.caption("Kelompok 7 Tubes Big Data | Requirements: KPI, Trend, Distribusi, Perbandingan, Filter")
