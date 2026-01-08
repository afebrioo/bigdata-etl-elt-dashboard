import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- 1. PRE-CONFIG ---
st.set_page_config(
    page_title="Big Data Pipelines Dashboard",
    page_icon="🚀",
    layout="wide",
)

# --- 2. HELPER FUNCTIONS ---
def get_col(df, target_name):
    if df is None or df.empty: return None
    target_clean = target_name.lower().replace(" ", "").replace("_", "")
    for col in df.columns:
        col_clean = str(col).lower().replace(" ", "").replace("_", "")
        if col_clean == target_clean:
            return col
    return None

def apply_filters(df, selected_range, selected_regions, selected_items, selected_channels):
    if df is None or df.empty:
        return pd.DataFrame()

    d_col = get_col(df, 'Order Date')
    r_col = get_col(df, 'Region')
    i_col = get_col(df, 'Item Type')
    c_col = get_col(df, 'Sales Channel')

    mask = pd.Series(True, index=df.index)

    # Date Filter
    if d_col and selected_range[0] and selected_range[1]:
        df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        start, end = pd.to_datetime(selected_range[0]), pd.to_datetime(selected_range[1])
        mask &= df[d_col].between(start, end)

    if r_col and selected_regions:
        mask &= df[r_col].isin(selected_regions)
    if i_col and selected_items:
        mask &= df[i_col].isin(selected_items)
    if c_col and selected_channels:
        mask &= df[c_col].isin(selected_channels)

    return df.loc[mask].copy()

# --- 3. DATA LOADING ---
@st.cache_data
def load_elt_data():
    try:
        # DB Logic omitted for brevity, using CSV fallback logic
        csv_path = os.path.join(BASE_DIR, "data", "sales_processed.csv")
        df = pd.read_csv(csv_path)
        d_col = get_col(df, 'Order Date')
        if d_col: df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"ELT Load Error: {e}")
        return pd.DataFrame()

@st.cache_data
def load_etl_data():
    try:
        csv_path = os.path.join(BASE_DIR, "data", "fact_sales.csv")
        df = pd.read_csv(csv_path)
        d_col = get_col(df, 'Order Date')
        if d_col: df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"ETL Load Error: {e}")
        return pd.DataFrame()

# Load data di awal
df_elt_raw = load_elt_data()
df_fact_raw = load_etl_data()

if df_elt_raw.empty and df_fact_raw.empty:
    st.error("No data found. Please check your CSV files in /data/ folder.")
    st.stop()

# --- 4. SIDEBAR FILTERS ---
st.sidebar.header("🔍 Global Filters")

# Ambil list untuk filter dari ELT (karena biasanya lebih lengkap)
all_dates = []
for d_df in [df_elt_raw, df_fact_raw]:
    dc = get_col(d_df, 'Order Date')
    if dc: all_dates.extend(d_df[dc].dropna().tolist())

if all_dates:
    min_d, max_d = min(all_dates), max(all_dates)
    selected_range = st.sidebar.date_input("Date Horizon", [min_d.date(), max_d.date()])
else:
    selected_range = [None, None]

# Regions, Items, Channels
def get_unique(df1, df2, col_name):
    c1, c2 = get_col(df1, col_name), get_col(df2, col_name)
    res = set()
    if c1: res.update(df1[c1].dropna().unique())
    if c2: res.update(df2[c2].dropna().unique())
    return sorted(list(res))

selected_regions = st.sidebar.multiselect("Regions", get_unique(df_elt_raw, df_fact_raw, 'Region'))
selected_items = st.sidebar.multiselect("Items", get_unique(df_elt_raw, df_fact_raw, 'Item Type'))
selected_channels = st.sidebar.multiselect("Channels", get_unique(df_elt_raw, df_fact_raw, 'Sales Channel'))

# --- 5. DATA PROCESSING (NORMALISASI & FILTERING) ---
# Jalankan Filter
f_df_elt = apply_filters(df_elt_raw, selected_range, selected_regions, selected_items, selected_channels)

# Normalisasi ETL View (Biasanya kolom dari SQL join namanya berantakan)
if not df_fact_raw.empty:
    mapping = {
        get_col(df_fact_raw, 'Order Date'): 'Order Date',
        get_col(df_fact_raw, 'Region'): 'Region',
        get_col(df_fact_raw, 'Sales Channel'): 'Sales Channel',
        get_col(df_fact_raw, 'Item Type'): 'Item Type'
    }
    mapping = {k: v for k, v in mapping.items() if k is not None}
    df_fact_raw = df_fact_raw.rename(columns=mapping)

f_df_etl = apply_filters(df_fact_raw, selected_range, selected_regions, selected_items, selected_channels)

# --- 6. VISUALIZATION ENGINE ---
def render_content(df, p_name):
    if df is None or df.empty:
        st.warning(f"No data for {p_name}. Check your filters.")
        return

    m_color = "#FF4B4B" if p_name == "ELT" else "#0083B0"
    
    # Mapping
    rev, prof, unt = get_col(df, 'Total Revenue'), get_col(df, 'Total Profit'), get_col(df, 'Units Sold')
    date_c, reg_c, chan_c = get_col(df, 'Order Date'), get_col(df, 'Region'), get_col(df, 'Sales Channel')

    if not rev or not prof:
        st.error(f"Missing core columns in {p_name}. Columns found: {list(df.columns)}")
        return

    # KPI
    st.subheader("1. KPI Utama")
    c1, c2, c3 = st.columns(3)
    c1.metric("Revenue", f"${df[rev].sum():,.0f}")
    c2.metric("Profit", f"${df[prof].sum():,.0f}")
    c3.metric("Units", f"{df[unt].sum():,.0f}")

    # Trend
    st.subheader("2. Tren Waktu")
    if date_c:
        df[date_c] = pd.to_datetime(df[date_c])
        trend = df.groupby(pd.Grouper(key=date_c, freq='M'))[prof].sum().reset_index()
        chart = alt.Chart(trend).mark_area(color=m_color, opacity=0.5).encode(
            x=f'{date_c}:T', y=f'{prof}:Q'
        ).properties(height=250)
        st.altair_chart(chart, use_container_width=True)

    # Distribution & Comparison
    st.subheader("3. Analisis Perbandingan")
    colA, colB = st.columns(2)
    with colA:
        if reg_c:
            reg_chart = alt.Chart(df).mark_bar(color=m_color).encode(
                x=f'sum({prof}):Q', y=alt.Y(f'{reg_c}:N', sort='-x')
            )
            st.altair_chart(reg_chart, use_container_width=True)
    with colB:
        if chan_c:
            chan_chart = alt.Chart(df).mark_arc().encode(
                theta=f'sum({prof}):Q', color=f'{chan_c}:N'
            )
            st.altair_chart(chan_chart, use_container_width=True)

# --- 7. MAIN UI ---
st.title("🏆 Sales Intelligence Dashboard")
tab1, tab2 = st.tabs(["🔴 ELT View", "🔵 ETL View"])
with tab1: render_content(f_df_elt, "ELT")
with tab2: render_content(f_df_etl, "ETL")