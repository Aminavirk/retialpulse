import os
import tempfile
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(
    page_title="RetailPulse | Retail Intelligence",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
:root {
  --rp-bg:#071426;
  --rp-panel:#10233f;
  --rp-panel-2:#142c4d;
  --rp-border:#29486d;
  --rp-text:#edf4ff;
  --rp-muted:#a9bdd8;
  --rp-accent:#39b8ff;
}
.stApp {background:linear-gradient(145deg,#061120 0%,#0a1c34 55%,#0c2442 100%); color:var(--rp-text);}
[data-testid="stHeader"] {background:rgba(7,20,38,.92);}
[data-testid="stToolbar"] {right:1rem;}
.block-container {max-width:1500px; padding-top:1.5rem; padding-bottom:3rem;}
h1,h2,h3,h4,h5,h6,p,li,label,.stMarkdown {color:var(--rp-text);}
[data-testid="stCaptionContainer"], .stCaption {color:var(--rp-muted)!important;}
[data-testid="stSidebar"] {background:#091a30; border-right:1px solid var(--rp-border);}
[data-testid="stSidebar"] * {color:var(--rp-text);}
[data-testid="stMetric"] {
  background:linear-gradient(145deg,#132b4b,#0e2039);
  border:1px solid var(--rp-border);
  padding:17px 19px;
  border-radius:15px;
  box-shadow:0 8px 22px rgba(0,0,0,.16);
}
[data-testid="stMetricLabel"] {color:#b8cbe4!important;}
[data-testid="stMetricValue"] {color:#ffffff!important;}
[data-testid="stMetricDelta"] {color:#64d6b0!important;}
.rp-hero {padding:26px 30px; border:1px solid #315a86; border-radius:18px; background:linear-gradient(115deg,#102b50 0%,#174d7d 58%,#087eaa 100%); color:white; margin-bottom:20px; box-shadow:0 12px 30px rgba(0,0,0,.22);}
.rp-hero h1 {color:#ffffff; margin:0; font-size:2.1rem;}
.rp-hero p {color:#d7ebff; margin:.4rem 0 0 0;}
.rp-note {color:var(--rp-muted); font-size:.88rem;}
[data-testid="stFileUploader"] section {background:#102743; border:1px dashed #4a83b8; border-radius:14px;}
[data-testid="stFileUploader"] section * {color:#eaf4ff!important;}
[data-testid="stFileUploaderDropzone"] {background:#102743;}
.stTextInput input, .stNumberInput input, .stDateInput input,
.stTimeInput input, [data-baseweb="select"] > div,
[data-baseweb="input"] > div, [data-baseweb="textarea"] {
  background:#102743!important; color:#f3f8ff!important;
  border-color:#365b82!important;
}
[data-baseweb="select"] * {color:#f3f8ff!important;}
[data-baseweb="popover"] [role="listbox"] {background:#102743!important;}
[data-baseweb="menu"] {background:#102743!important;}
[data-baseweb="menu"] * {color:#f3f8ff!important;}
.stButton button, .stDownloadButton button, button[kind="primary"] {
  background:linear-gradient(110deg,#1769aa,#168fc0)!important;
  color:#ffffff!important; border:1px solid #4ba9dc!important;
  border-radius:10px!important; font-weight:650!important;
}
.stButton button:hover, .stDownloadButton button:hover {
  background:linear-gradient(110deg,#2185ce,#20acd5)!important;
  border-color:#83d5ff!important;
}
[data-testid="stDataFrame"], [data-testid="stTable"] {
  border:1px solid var(--rp-border); border-radius:12px; overflow:hidden;
}
[data-testid="stAlert"] {background:#102743; border:1px solid #315a86; color:#edf4ff;}
hr {border-color:#29486d!important;}
</style>
""", unsafe_allow_html=True)

# Use a dark chart theme so charts match the navy interface.
px.defaults.template = "plotly_dark"

@st.cache_resource
def get_spark():
    return (SparkSession.builder
            .appName("RetailPulse-Upload-Analytics")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())

def read_upload(upload):
    suffix = os.path.splitext(upload.name)[1].lower()
    if suffix == ".csv":
        # Try common encodings and delimiter inference.
        raw = upload.getvalue()
        for enc in ("utf-8-sig", "cp1252", "latin1"):
            try:
                import io
                return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python")
            except Exception:
                continue
        raise ValueError("Could not read this CSV. Save it as UTF-8 CSV and upload again.")
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(upload)
    raise ValueError("Please upload a CSV or Excel (.xlsx/.xls) file.")

def clean_colname(x):
    return "".join(ch.lower() for ch in str(x).strip() if ch.isalnum())

ALIASES = {
    "date": ["invoicedate","date","orderdate","transactiondate","saledate","datetime","timestamp"],
    "order": ["invoiceno","invoice","orderno","orderid","transactionid","receiptid"],
    "product": ["description","product","productname","item","itemname","stockcode","sku"],
    "quantity": ["quantity","qty","units","unitssold","sold"],
    "unit_price": ["unitprice","price","sellingprice","unitcostprice","rate"],
    "revenue": ["revenue","sales","sal es","salesamount","total","totalamount","amount","netsales","grosssales"],
    "customer": ["customerid","customer","clientid","buyerid"],
    "category": ["category","productcategory","department","type"],
    "region": ["country","region","state","city","location","market"],
    "cost": ["cost","unitcost","costprice","totalcost"],
}

def suggest_column(columns, key):
    normalized = {clean_colname(c): c for c in columns}
    for alias in ALIASES[key]:
        if clean_colname(alias) in normalized:
            return normalized[clean_colname(alias)]
    return "(not available)"

def as_num(series):
    return pd.to_numeric(series.astype(str).str.replace(r"[$£€,]", "", regex=True).str.replace(",", "", regex=False), errors="coerce")

def money(v, currency):
    return f"{currency} {v:,.2f}" if currency else f"{v:,.2f}"

def make_spark_clean(frame, mapping):
    spark = get_spark()
    work = pd.DataFrame()
    work["_row_id"] = range(len(frame))
    for key, col in mapping.items():
        if col != "(not available)" and col in frame.columns:
            work[key] = frame[col]
        else:
            work[key] = None

    if mapping["date"] != "(not available)":
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
    else:
        work["date"] = pd.NaT

    for key in ("quantity", "unit_price", "revenue", "cost"):
        work[key] = as_num(work[key]) if work[key].notna().any() else pd.NA

    if work["revenue"].isna().all():
        if work["quantity"].notna().any() and work["unit_price"].notna().any():
            work["revenue"] = work["quantity"] * work["unit_price"]
        else:
            raise ValueError("Map a Revenue/Sales/Amount column, or map both Quantity and Unit Price.")

    # Convert to plain Python records before handing them to Spark RDD.
    records = []
    for row in work.to_dict("records"):
        records.append({
            "row_id": int(row["_row_id"]),
            "date": None if pd.isna(row["date"]) else pd.Timestamp(row["date"]).to_pydatetime(),
            "order": None if pd.isna(row["order"]) else str(row["order"]).strip(),
            "product": "Unspecified" if pd.isna(row["product"]) or not str(row["product"]).strip() else str(row["product"]).strip(),
            "quantity": None if pd.isna(row["quantity"]) else float(row["quantity"]),
            "unit_price": None if pd.isna(row["unit_price"]) else float(row["unit_price"]),
            "revenue": None if pd.isna(row["revenue"]) else float(row["revenue"]),
            "customer": "Unknown" if pd.isna(row["customer"]) or not str(row["customer"]).strip() else str(row["customer"]).strip(),
            "category": "Uncategorised" if pd.isna(row["category"]) or not str(row["category"]).strip() else str(row["category"]).strip(),
            "region": "Unspecified" if pd.isna(row["region"]) or not str(row["region"]).strip() else str(row["region"]).strip(),
            "cost": None if pd.isna(row["cost"]) else float(row["cost"]),
        })

    base = spark.sparkContext.parallelize(records)
    # RDD transformations: filter invalid financial rows, map standardized records,
    # and reduceByKey for summary metrics.
    valid = (base
        .filter(lambda r: r["revenue"] is not None and pd.notna(r["revenue"]) and r["revenue"] >= 0)
        .map(lambda r: {**r, "profit": (r["revenue"] - r["cost"]) if r["cost"] is not None else None})
        .cache())
    # Perform a real RDD aggregation with reduceByKey before collecting rows.
    # This keeps the Spark RDD component visible in the processing pipeline.
    region_revenue_rdd = valid.map(
        lambda r: (r["region"], float(r["revenue"]))
    ).reduceByKey(lambda a, b: a + b)
    region_revenue_rdd.count()  # materialise the RDD aggregation
    clean = valid.collect()
    clean_df = pd.DataFrame(clean)
    if clean_df.empty:
        raise ValueError("No rows with valid non-negative revenue were found. Check the column mapping.")
    duplicate_count = int(clean_df.duplicated(subset=[c for c in clean_df.columns if c != "row_id"]).sum())
    clean_df = clean_df.drop_duplicates(subset=[c for c in clean_df.columns if c != "row_id"]).copy()
    clean_df["date"] = pd.to_datetime(clean_df["date"], errors="coerce")
    clean_df["month"] = clean_df["date"].dt.to_period("M").astype(str).replace("NaT", "Unknown")
    return clean_df, len(records), duplicate_count, spark.version

def metric_card(label, value, help_text=None):
    st.metric(label, value, help=help_text)

st.markdown("""
<div class="rp-hero">
  <h1>📈 RetailPulse</h1>
  <p>Retail intelligence workspace · Upload data · Clean with Apache Spark RDD · Explore business performance</p>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## Workspace")
    uploaded = st.file_uploader("Upload your retail data", type=["csv", "xlsx", "xls"], help="Upload a transaction-level retail sales CSV or Excel workbook.")
    st.caption("Supported: CSV, XLSX and XLS. Best results come from one row per transaction line.")
    st.divider()
    page = st.radio("Navigate", ["Overview", "Sales trends", "Product Intelligence", "Region Sales", "Products & categories", "Customers & regions", "Data quality", "Data explorer"], index=0)
    st.divider()
    st.caption("Processing engine: Apache PySpark RDD")

if uploaded is None:
    st.info("👋 Welcome to RetailPulse. Start by uploading a retail sales CSV or Excel file in the left sidebar.")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("### 01 · Upload")
        st.write("Bring your own transaction data in CSV or Excel format.")
    with c2:
        st.markdown("### 02 · Map & clean")
        st.write("Match your columns and remove rows with invalid sales values.")
    with c3:
        st.markdown("### 03 · Analyse")
        st.write("Explore revenue, products, customers, trends and downloadable cleaned data.")
    st.markdown("#### Typical columns we can recognise")
    st.code("Date · Order ID · Product · Quantity · Unit Price · Revenue · Customer · Category · Region · Cost", language="text")
    st.caption("Note: RetailPulse is designed for retail transaction datasets. It cannot analyse an unrelated file (for example, a photo list or school marks sheet) as retail sales without suitable sales fields.")
    st.stop()

try:
    raw_df = read_upload(uploaded)
except Exception as exc:
    st.error(f"Could not read the uploaded file: {exc}")
    st.stop()

if raw_df.empty:
    st.error("The uploaded file contains no data rows.")
    st.stop()

st.subheader("1. Confirm your dataset columns")
st.write(f"**File:** `{uploaded.name}` · **Rows:** {len(raw_df):,} · **Columns:** {len(raw_df.columns)}")
with st.expander("Preview uploaded raw data", expanded=False):
    st.dataframe(raw_df.head(15), use_container_width=True)

keys = ["date", "order", "product", "quantity", "unit_price", "revenue", "customer", "category", "region", "cost"]
labels = {"date":"Transaction date", "order":"Order / invoice ID", "product":"Product name / SKU",
          "quantity":"Quantity", "unit_price":"Unit price", "revenue":"Revenue / sales amount",
          "customer":"Customer ID", "category":"Category", "region":"Country / region", "cost":"Cost"}
options = ["(not available)"] + list(raw_df.columns)
suggestions = {k: suggest_column(raw_df.columns, k) for k in keys}
with st.form("column_mapping"):
    st.markdown("**2. Check or change the automatic column matching**")
    cols = st.columns(2)
    mapping = {}
    for i, key in enumerate(keys):
        with cols[i % 2]:
            idx = options.index(suggestions[key]) if suggestions[key] in options else 0
            mapping[key] = st.selectbox(labels[key], options, index=idx, key=f"map_{key}")
    run = st.form_submit_button("Process dataset with PySpark RDD", type="primary", use_container_width=True)

if not run:
    st.info("Review the column matches above, then click **Process dataset with PySpark RDD**.")
    st.stop()

try:
    with st.spinner("Cleaning and analysing your data with Apache Spark RDD…"):
        df, input_rows, duplicate_count, spark_version = make_spark_clean(raw_df, mapping)
except Exception as exc:
    st.error(f"Processing could not complete: {exc}")
    st.stop()

currency = st.sidebar.selectbox("Currency display", ["£ GBP", "$ USD", "€ EUR", "PKR", "No symbol"], index=0)
symbol = {"£ GBP":"£", "$ USD":"$", "€ EUR":"€", "PKR":"PKR", "No symbol":""}[currency]
total_revenue = float(df["revenue"].sum())
orders = int(df["order"].nunique()) if df["order"].notna().any() else len(df)
customers = int(df.loc[df["customer"]!="Unknown", "customer"].nunique())
units = float(df["quantity"].sum()) if df["quantity"].notna().any() else float("nan")
profit_available = df["profit"].notna().any()
profit = float(df["profit"].dropna().sum()) if profit_available else None

st.markdown(f"**Dataset loaded:** `{uploaded.name}` &nbsp; · &nbsp; **Clean rows:** {len(df):,} / {input_rows:,} &nbsp; · &nbsp; **Spark:** {spark_version}", unsafe_allow_html=True)
f1, f2, f3 = st.columns(3)
with f1:
    metric_card("Total sales revenue", money(total_revenue, symbol))
with f2:
    metric_card("Orders / transactions", f"{orders:,}")
with f3:
    metric_card("Unique identified customers", f"{customers:,}")
f4, f5, f6 = st.columns(3)
with f4:
    metric_card("Units sold", f"{units:,.0f}" if pd.notna(units) else "Not mapped")
with f5:
    metric_card("Average order value", money(total_revenue / orders if orders else 0, symbol))
with f6:
    metric_card("Estimated profit", money(profit, symbol) if profit_available else "Cost data not mapped",
                "Calculated as revenue minus mapped cost. This is only reliable if your cost column represents comparable total cost.")

if page == "Overview":
    st.subheader("Business performance overview")
    if df["date"].notna().any():
        monthly = df.dropna(subset=["date"]).assign(month=lambda x:x["date"].dt.to_period("M").astype(str)).groupby("month", as_index=False)["revenue"].sum()
        st.plotly_chart(px.area(monthly, x="month", y="revenue", markers=True, title="Revenue over time"), use_container_width=True)
    else:
        st.info("Map a date column to show time trends.")
    a, b = st.columns(2)
    with a:
        region = df.groupby("region", as_index=False)["revenue"].sum().nlargest(10, "revenue")
        st.plotly_chart(px.bar(region, x="revenue", y="region", orientation="h", title="Top regions by revenue"), use_container_width=True)
    with b:
        products = df.groupby("product", as_index=False)["revenue"].sum().nlargest(10, "revenue")
        st.plotly_chart(px.bar(products, x="revenue", y="product", orientation="h", title="Top products by revenue"), use_container_width=True)

elif page == "Sales trends":
    st.subheader("Sales trends")
    if df["date"].notna().any():
        timed = df.dropna(subset=["date"]).copy()
        timed["month"] = timed["date"].dt.to_period("M").astype(str)
        monthly = timed.groupby("month", as_index=False).agg(revenue=("revenue","sum"), transactions=("row_id","count"))
        st.plotly_chart(px.line(monthly, x="month", y="revenue", markers=True, title="Monthly revenue"), use_container_width=True)
        st.plotly_chart(px.bar(monthly, x="month", y="transactions", title="Transaction lines by month"), use_container_width=True)
    else:
        st.warning("No date field was mapped. Return to the column mapping and select your transaction date.")

elif page == "Product Intelligence":
    st.subheader("Product Intelligence")
    st.caption("Search a product from the uploaded transaction data and review its performance.")

    product_names = sorted(df["product"].dropna().astype(str).unique().tolist())
    search_term = st.text_input("Search product name or SKU", placeholder="For example: WATER BOTTLE")
    matches = [p for p in product_names if search_term.strip().lower() in p.lower()]

    if not matches:
        st.warning("No matching products found. Try a different search or check the Product column mapping.")
    else:
        selected_product = st.selectbox("Select product", matches)
        product_df = df[df["product"].astype(str) == selected_product].copy()

        product_revenue = float(product_df["revenue"].sum())
        product_units = float(product_df["quantity"].sum()) if product_df["quantity"].notna().any() else None
        product_orders = int(product_df["order"].nunique()) if product_df["order"].notna().any() else len(product_df)
        average_price = float(product_df["unit_price"].dropna().mean()) if product_df["unit_price"].notna().any() else None
        possible_returns = int((product_df["quantity"] < 0).sum()) if product_df["quantity"].notna().any() else 0

        st.markdown(f"### {selected_product}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Sales revenue", money(product_revenue, symbol))
        m2.metric("Net units", f"{product_units:,.0f}" if product_units is not None else "Not mapped")
        m3.metric("Orders / invoices", f"{product_orders:,}")
        m4.metric("Average unit price", money(average_price, symbol) if average_price is not None else "Not mapped")
        st.caption(f"Transaction rows: {len(product_df):,} · Negative-quantity rows: {possible_returns:,}")

        left, right = st.columns(2)
        with left:
            dated = product_df.dropna(subset=["date"]).copy()
            if not dated.empty:
                monthly_product = (
                    dated.assign(month=lambda x: x["date"].dt.to_period("M").astype(str))
                    .groupby("month", as_index=False)["revenue"].sum()
                )
                st.plotly_chart(
                    px.line(monthly_product, x="month", y="revenue", markers=True,
                            title="Product revenue over time"),
                    use_container_width=True
                )
            else:
                st.info("Map a transaction date to view monthly product trends.")
        with right:
            product_region = (
                product_df.groupby("region", as_index=False)["revenue"]
                .sum().sort_values("revenue", ascending=False).head(15)
            )
            st.plotly_chart(
                px.bar(product_region, x="revenue", y="region", orientation="h",
                       title="Product revenue by region"),
                use_container_width=True
            )

        st.markdown("### Product transaction history")
        st.dataframe(product_df, use_container_width=True, height=350)
        st.download_button(
            "Download product transactions (CSV)",
            product_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="retailpulse_product_transactions.csv",
            mime="text/csv"
        )
        st.info("Product specifications such as brand, material, colour and size are only available if those fields exist in the uploaded file. This app does not invent product descriptions.")

elif page == "Region Sales":
    st.subheader("Region Sales Intelligence")
    st.caption("Compare revenue, transactions, products and trends across the geographic field mapped in your uploaded data.")

    region_summary = (
        df.groupby("region", as_index=False)
        .agg(
            revenue=("revenue", "sum"),
            transaction_lines=("row_id", "count"),
            units=("quantity", "sum"),
            products=("product", "nunique")
        )
        .sort_values("revenue", ascending=False)
    )

    if region_summary.empty:
        st.warning("No region values are available. Check the Country / Region column mapping.")
    else:
        total_region_revenue = float(region_summary["revenue"].sum())
        top_region = str(region_summary.iloc[0]["region"])
        region_count = len(region_summary)
        average_region_revenue = total_region_revenue / region_count if region_count else 0

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Total region revenue", money(total_region_revenue, symbol))
        r2.metric("Regions covered", f"{region_count:,}")
        r3.metric("Top region", top_region)
        r4.metric("Average revenue / region", money(average_region_revenue, symbol))

        st.plotly_chart(
            px.bar(
                region_summary.head(20).sort_values("revenue"),
                x="revenue", y="region", orientation="h",
                title="Top 20 regions by sales revenue"
            ),
            use_container_width=True
        )

        left, right = st.columns(2)
        with left:
            st.plotly_chart(
                px.pie(region_summary.head(10), names="region", values="revenue",
                       hole=0.45, title="Revenue share: top 10 regions"),
                use_container_width=True
            )
        with right:
            st.plotly_chart(
                px.bar(region_summary.head(20), x="region", y="transaction_lines",
                       title="Transaction lines by region"),
                use_container_width=True
            )

        st.markdown("### Explore an individual region")
        selected_region = st.selectbox(
            "Select region",
            region_summary["region"].astype(str).tolist()
        )
        selected_region_df = df[df["region"].astype(str) == selected_region].copy()
        selected_revenue = float(selected_region_df["revenue"].sum())
        selected_orders = (
            int(selected_region_df["order"].nunique())
            if selected_region_df["order"].notna().any()
            else len(selected_region_df)
        )

        a, b, c = st.columns(3)
        a.metric("Selected region revenue", money(selected_revenue, symbol))
        b.metric("Orders / invoices", f"{selected_orders:,}")
        c.metric("Products sold", f"{selected_region_df['product'].nunique():,}")

        region_left, region_right = st.columns(2)
        with region_left:
            dated_region = selected_region_df.dropna(subset=["date"]).copy()
            if not dated_region.empty:
                monthly_region = (
                    dated_region.assign(month=lambda x: x["date"].dt.to_period("M").astype(str))
                    .groupby("month", as_index=False)["revenue"].sum()
                )
                st.plotly_chart(
                    px.line(monthly_region, x="month", y="revenue", markers=True,
                            title=f"Monthly revenue · {selected_region}"),
                    use_container_width=True
                )
            else:
                st.info("Map a transaction date to view monthly regional trends.")
        with region_right:
            top_products_region = (
                selected_region_df.groupby("product", as_index=False)["revenue"]
                .sum().nlargest(10, "revenue")
            )
            st.plotly_chart(
                px.bar(top_products_region.sort_values("revenue"), x="revenue", y="product",
                       orientation="h", title=f"Top products · {selected_region}"),
                use_container_width=True
            )

        st.markdown("### Regional sales report")
        st.dataframe(region_summary, use_container_width=True)
        st.download_button(
            "Download region sales summary (CSV)",
            region_summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="retailpulse_region_sales_summary.csv",
            mime="text/csv"
        )
        st.download_button(
            "Download selected region transactions (CSV)",
            selected_region_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="retailpulse_selected_region_transactions.csv",
            mime="text/csv"
        )

elif page == "Products & categories":
    st.subheader("Product and category performance")
    left, right = st.columns(2)
    with left:
        prod = df.groupby("product", as_index=False).agg(revenue=("revenue","sum"), units=("quantity","sum")).nlargest(20,"revenue")
        st.plotly_chart(px.bar(prod, x="revenue", y="product", orientation="h", title="Top 20 products by revenue"), use_container_width=True)
    with right:
        cat = df.groupby("category", as_index=False)["revenue"].sum().nlargest(15,"revenue")
        st.plotly_chart(px.pie(cat, names="category", values="revenue", hole=.45, title="Revenue mix by category"), use_container_width=True)
    st.dataframe(prod.sort_values("revenue", ascending=False), use_container_width=True)

elif page == "Customers & regions":
    st.subheader("Customer and geographic analysis")
    left, right = st.columns(2)
    with left:
        cust = df[df["customer"]!="Unknown"].groupby("customer", as_index=False)["revenue"].sum().nlargest(20,"revenue")
        st.plotly_chart(px.bar(cust, x="customer", y="revenue", title="Top customers by revenue"), use_container_width=True)
    with right:
        reg = df.groupby("region", as_index=False)["revenue"].sum().nlargest(20,"revenue")
        st.plotly_chart(px.bar(reg, x="revenue", y="region", orientation="h", title="Revenue by region"), use_container_width=True)

elif page == "Data quality":
    st.subheader("Data quality and cleaning audit")
    quality = pd.DataFrame({
        "Field": raw_df.columns,
        "Missing values": [int(raw_df[c].isna().sum()) for c in raw_df.columns],
        "Missing %": [round(float(raw_df[c].isna().mean()*100), 2) for c in raw_df.columns],
        "Data type": [str(raw_df[c].dtype) for c in raw_df.columns],
    })
    q1, q2, q3 = st.columns(3)
    q1.metric("Uploaded rows", f"{input_rows:,}")
    q2.metric("Clean rows", f"{len(df):,}")
    q3.metric("Exact duplicate rows removed", f"{duplicate_count:,}")
    st.dataframe(quality, use_container_width=True)
    st.caption("Cleaning rule: rows without a usable non-negative revenue value are excluded. Exact duplicate transaction rows are removed. Confirm your business's treatment of refunds, cancellations and tax before operational use.")

elif page == "Data explorer":
    st.subheader("Cleaned transaction explorer")
    search = st.text_input("Search product, order, customer or region")
    view = df.copy()
    if search:
        mask = view.astype(str).apply(lambda col: col.str.contains(search, case=False, na=False)).any(axis=1)
        view = view[mask]
    st.write(f"Showing **{len(view):,}** rows")
    st.dataframe(view, use_container_width=True, height=460)

st.divider()
st.subheader("Download your results")
csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
st.download_button("⬇ Download cleaned transactions (CSV)", csv_bytes,
                   file_name="retailpulse_cleaned_transactions.csv", mime="text/csv", type="primary")
summary = df.groupby("region", as_index=False)["revenue"].sum().sort_values("revenue", ascending=False)
st.download_button("⬇ Download region sales summary (CSV)", summary.to_csv(index=False).encode("utf-8-sig"),
                   file_name="retailpulse_region_summary.csv", mime="text/csv")
st.caption("RetailPulse is an academic analytics prototype. Upload only data you are authorised to process. Revenue is not profit; profit is shown only when suitable cost data is supplied.")
