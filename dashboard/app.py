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


# --- DATABASE CONNECTION ---
@st.cache_resource
def get_engine(db_name='elt_sales_db'):
    return create_engine(f'mysql+pymysql://root:@localhost/{db_name}')


@st.cache_data
def load_elt_data():
    try:
        # Coba koneksi database lokal
        engine = get_engine('elt_sales_db')
        df = pd.read_sql("SELECT * FROM sales_processed", engine)
        return df
    except:
        # Fallback ke CSV (untuk Deploy GitHub)
        try:
            # Menggabungkan path folder app.py dengan subfolder data
            csv_path = os.path.join(BASE_DIR, "data", "sales_processed.csv")
            df = pd.read_csv(csv_path)
            
            d_col = get_col(df, 'Order Date')
            if d_col:
                df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
            return df
        except Exception as e:
            st.error(f"ELT load error: {e}")
            return pd.DataFrame()

@st.cache_data
def load_etl_data():
    try:
        # Coba koneksi database lokal
        engine = get_engine('dw_sales')
        query = "SELECT f.*, d.order_date, c.region, c.country, i.item_type, ch.sales_channel FROM fact_sales f LEFT JOIN dim_date d ON f.date_id = d.date_id LEFT JOIN dim_country c ON f.country_id = c.country_id LEFT JOIN dim_item i ON f.item_id = i.item_id LEFT JOIN dim_channel ch ON f.channel_id = ch.channel_id"
        df = pd.read_sql(query, engine)
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        return df
    except:
        # Fallback ke CSV (untuk Deploy GitHub)
        try:
            csv_path = os.path.join(BASE_DIR, "data", "fact_sales.csv")
            df = pd.read_csv(csv_path)
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            return df
        except Exception as e:
            st.error(f"ETL load error: {e}")
            return pd.DataFrame()

# --- LOADING DATA ---
df_elt_raw = load_elt_data()
df_fact_raw = load_etl_data()

if df_elt_raw.empty and df_fact_raw.empty:
    st.error("No data found. Please run your ETL/ELT pipelines first.")
    st.stop()

# --- SIDEBAR: GLOBAL FILTERS ---
# --- SIDEBAR: GLOBAL FILTERS ---
st.sidebar.header("🔍 Global Filters")

# ===== DATE FILTER =====
all_dates = []
for d_df in [df_elt_raw, df_fact_raw]:
    if d_df is not None and not d_df.empty:
        dc = get_col(d_df, 'Order Date')
        if dc:
            all_dates.extend(pd.to_datetime(d_df[dc], errors='coerce').dropna().tolist())

if all_dates:
    min_date, max_date = min(all_dates), max(all_dates)
    selected_range = st.sidebar.date_input(
        "Select Date Horizon",
        [min_date.date(), max_date.date()]
    )
else:
    selected_range = [None, None]

# ===== REGION FILTER =====
r_col_elt = get_col(df_elt_raw, 'Region')
r_col_etl = get_col(df_fact_raw, 'Region')

if r_col_elt:
    all_regions = sorted(df_elt_raw[r_col_elt].dropna().astype(str).unique())
elif r_col_etl:
    all_regions = sorted(df_fact_raw[r_col_etl].dropna().astype(str).unique())
else:
    all_regions = []

selected_regions = st.sidebar.multiselect(
    "Select Regions", options=all_regions, default=all_regions
)

# ===== ITEM TYPE FILTER =====
i_col_elt = get_col(df_elt_raw, 'Item Type')
i_col_etl = get_col(df_fact_raw, 'Item Type')

if i_col_elt:
    all_items = sorted(df_elt_raw[i_col_elt].dropna().astype(str).unique())
elif i_col_etl:
    all_items = sorted(df_fact_raw[i_col_etl].dropna().astype(str).unique())
else:
    all_items = []

selected_items = st.sidebar.multiselect(
    "Select Item Types", options=all_items, default=all_items
)

# ===== SALES CHANNEL FILTER =====
c_col_elt = get_col(df_elt_raw, 'Sales Channel')
c_col_etl = get_col(df_fact_raw, 'Sales Channel')

if c_col_elt:
    all_channels = sorted(df_elt_raw[c_col_elt].dropna().astype(str).unique())
elif c_col_etl:
    all_channels = sorted(df_fact_raw[c_col_etl].dropna().astype(str).unique())
else:
    all_channels = []

selected_channels = st.sidebar.multiselect(
    "Sales Channel", options=all_channels, default=all_channels
)

# --- FILTERING ---
def apply_filters(df):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    d_col = get_col(df, 'Order Date')
    r_col = get_col(df, 'Region')
    i_col = get_col(df, 'Item Type')
    c_col = get_col(df, 'Sales Channel')

    mask = pd.Series(True, index=df.index)

    # DATE FILTER
    if d_col and selected_range[0] and selected_range[1]:
        df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
        start = pd.to_datetime(selected_range[0])
        end = pd.to_datetime(selected_range[1])
        mask &= df[d_col].between(start, end)

    if r_col and selected_regions:
        mask &= df[r_col].isin(selected_regions)

    if i_col and selected_items:
        mask &= df[i_col].isin(selected_items)

    if c_col and selected_channels:
        mask &= df[c_col].isin(selected_channels)

    return df.loc[mask].copy()




f_df_elt = apply_filters(df_elt_raw)
f_df_etl = apply_filters(df_fact_raw)

# --- MAIN UI ---
st.title("🏆 Sales Intelligence Dashboard")
st.markdown("Dashboard untuk Implementasi Pipeline Big Data ETL dan ELT pada Studi Kasus Catatan Penjualan dengan Interactive Filters.")
tab1, tab2 = st.tabs(["🔴 ELT View (Warehouse)", "🔵 ETL View (Star Schema)"])

def render_content(df, p_name):
    if df is None or df.empty:
        st.warning(f"No data for {p_name} pipeline.")
        return

    m_color = "#FF4B4B" if p_name == "ELT" else "#0083B0"

    # 1. Mapping Kolom dengan Debugging
    rev = get_col(df, 'Total Revenue')
    prof = get_col(df, 'Total Profit')
    units = get_col(df, 'Units Sold')
    date_c = get_col(df, 'Order Date')
    reg_c = get_col(df, 'Region')
    chan_c = get_col(df, 'Sales Channel')
    prio_c = get_col(df, 'Order Priority')

    # Cek kolom kritikal
    if not rev or not prof or not units:
        st.error(f"Kolom Utama Tidak Ditemukan! Kolom tersedia: {list(df.columns)}")
        return

    # 2. KPI UTAMA
    st.subheader("1. KPI Utama")
    k1, k2, k3, k4 = st.columns(4)
    t_rev = df[rev].fillna(0).sum()
    t_prof = df[prof].fillna(0).sum()
    t_units = df[units].fillna(0).sum()
    k1.metric("Total Revenue", f"${t_rev:,.0f}")
    k2.metric("Total Profit", f"${t_prof:,.0f}")
    k3.metric("Units Sold", f"{t_units:,.0f}")
    k4.metric("Profit Margin", f"{(t_prof/t_rev*100):.1f}%" if t_rev != 0 else "0%")

    st.markdown("---")

    # 3. TREN WAKTU (FIX LOGIC)
    st.subheader("2. Tren Waktu")
    if date_c:
        # Konversi paksa ke datetime
        df[date_c] = pd.to_datetime(df[date_c], errors='coerce')
        # Hapus baris yang tanggalnya NaT atau profitnya NaN
        df_trend = df.dropna(subset=[date_c, prof])
        
        if not df_trend.empty:
            trend = df_trend.groupby(pd.Grouper(key=date_c, freq='M'))[prof].sum().reset_index()
            chart_trend = alt.Chart(trend).mark_area(
                color=m_color, opacity=0.4, line={'color': m_color}
            ).encode(
                x=alt.X(f'{date_c}:T', title="Month"),
                y=alt.Y(f'{prof}:Q', title="Monthly Profit"),
                tooltip=[alt.Tooltip(f'{date_c}:T', title="Date"), alt.Tooltip(f'{prof}:Q', format="$,.0f")]
            ).properties(height=300)
            st.altair_chart(chart_trend, use_container_width=True)
        else:
            st.info(f"Kolom '{date_c}' ditemukan, tapi datanya kosong atau formatnya salah.")
    else:
        st.warning("Grafik Tren: Kolom 'Order Date' tidak terdeteksi.")

    st.markdown("---")

    # 4. DISTRIBUSI & PERBANDINGAN
    col_dist, col_comp = st.columns(2)
    
    with col_dist:
        st.subheader("3. Distribusi")
        st.write("**Profit Distribution**")
        dist_chart = alt.Chart(df).mark_bar(color=m_color).encode(
            alt.X(f"{prof}:Q", bin=alt.Bin(maxbins=20), title="Profit Bins"),
            y=alt.Y('count()', title="Frequency")
        ).properties(height=300)
        st.altair_chart(dist_chart, use_container_width=True)

    with col_comp:
        st.subheader("4. Perbandingan")
        # Perbandingan Channel
        if chan_c:
            st.write("**Sales Channel Performance**")
            chan_data = df.groupby(chan_c)[prof].sum().reset_index()
            chart_chan = alt.Chart(chan_data).mark_bar().encode(
                x=alt.X(f'{chan_c}:N', title="Channel", axis=alt.Axis(labelAngle=0)),
                y=alt.Y(f'{prof}:Q', title="Total Profit"),
                color=alt.Color(f'{chan_c}:N', legend=None),
                tooltip=[chan_c, alt.Tooltip(f'{prof}:Q', format="$,.0f")]
            ).properties(height=300)
            st.altair_chart(chart_chan, use_container_width=True)
        else:
            st.info("Kolom 'Sales Channel' tidak ditemukan.")
            
        # Perbandingan Region
        if reg_c:
            st.write("**Regional Profit Contribution**")
            reg_data = df.groupby(reg_c)[prof].sum().reset_index()
            chart_reg = alt.Chart(reg_data).mark_bar().encode(
                x=alt.X(f'{prof}:Q', title="Profit"),
                y=alt.Y(f'{reg_c}:N', sort='-x', title="Region"),
                color=alt.Color(f'{reg_c}:N', legend=None),
                tooltip=[reg_c, alt.Tooltip(f'{prof}:Q', format="$,.0f")]
            ).properties(height=300)
            st.altair_chart(chart_reg, use_container_width=True)
        else:
            st.info("Kolom 'Region' tidak ditemukan.")

    # 5. DATA EXPLORER
    with st.expander("📄 Raw Data Preview (Top 100)"):
        st.dataframe(df.head(100), use_container_width=True)
        
with tab1: render_content(f_df_elt, "ELT")
with tab2: render_content(f_df_etl, "ETL")

st.markdown("---")
st.caption("Kelompok 7 Tubes Big Data | Requirements: KPI, Trend, Distribusi, Perbandingan, Filter")
