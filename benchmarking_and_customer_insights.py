import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import plotly.express as px

# Load environment variables
load_dotenv()

# Page configuration for better performance
st.set_page_config(page_title="Benchmarking Metrics & Customer Insights",
                   layout="wide", initial_sidebar_state="expanded")

# Initialize Snowflake connection


@st.cache_resource
def init_connection():
    try:
        return snowflake.connector.connect(
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            account=os.getenv("ACCOUNT"),
            warehouse=os.getenv("WAREHOUSE"),
            database=os.getenv("DATABASE"),
            schema=os.getenv("SCHEMA")
        )
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        st.stop()


# Establish connection
try:
    conn = init_connection()
except Exception as e:
    st.error(f"Failed to initialize connection: {e}")
    st.stop()

# Query execution with error handling


@st.cache_data(ttl=3600)
def run_query(query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            results = cur.fetchall()
            return pd.DataFrame(results, columns=columns)
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

def benchmarking_and_customer_insights():
    # Streamlit UI
    st.title("👨 Benchmarking Metrics & Customer Insights")
    st.markdown("---")

    # 3. Benchmarking Metrics
    st.header("📈 Benchmarking Metrics")

    # 3.1 Price Comparison with Benchmarks
    st.subheader("💰 Price Comparison with Benchmarks")

    # Add filters for price comparison
    brands_df = run_query("SELECT DISTINCT BRAND FROM Products ORDER BY BRAND;")
    categories = run_query(
        "SELECT DISTINCT BENCHMARK_CATG FROM BENCHMARK ORDER BY BENCHMARK_CATG;")
    brands_list = ["All"]
    if brands_df is not None and not brands_df.empty:
        brands_list.extend(brands_df["BRAND"].tolist())

    # Search criteria section
    col1, col2, col3 = st.columns(3)
    with col1:
        product_filter = st.selectbox(
            "Filter by Brand", brands_list, key="price_product")
    with col2:
        comparison_filter = st.selectbox("Filter by Price Comparison", [
            "All", "Above Benchmark", "Below Benchmark", "At Benchmark"], key="price_comparison")
    with col3:
        search_text = st.text_input("Search Product Title", "", key="price_search")

    price_comparison_query = """
    SELECT 
        p.PRODUCT_TITLE,
        p.BRAND,
        pr.PRODUCT_PRICE,
        pr.BENCHMARK_BASE_PRICE,
        pr.BENCHMARK_SITE_PRICE,
        CASE 
        WHEN pr.PRODUCT_PRICE > pr.BENCHMARK_SITE_PRICE THEN 'Above Benchmark'
        WHEN pr.PRODUCT_PRICE < pr.BENCHMARK_SITE_PRICE THEN 'Below Benchmark'
        ELSE 'At Benchmark'
        END AS PRICE_COMPARISON
    FROM 
        Products p
    JOIN 
        Pricing pr ON p.ITEM_ID = pr.ITEM_ID
    ORDER BY 
        PRICE_COMPARISON, p.PRODUCT_TITLE;
    """
    price_comparison_df = run_query(price_comparison_query)

    if price_comparison_df is not None and not price_comparison_df.empty:
        # Apply filters
        if product_filter != "All":
            price_comparison_df = price_comparison_df[price_comparison_df["BRAND"]
                                                    == product_filter]
        if comparison_filter != "All":
            price_comparison_df = price_comparison_df[price_comparison_df["PRICE_COMPARISON"]
                                                    == comparison_filter]
        if search_text:
            price_comparison_df = price_comparison_df[price_comparison_df["PRODUCT_TITLE"].str.contains(
                search_text, case=False, na=False)]

        if price_comparison_df.empty:
            st.warning(
                "❌ Product not found. Please adjust your filters or search criteria.")
        else:
            if product_filter != "All":
                st.dataframe(price_comparison_df.drop(columns=["BRAND"]))
            else:
                st.dataframe(price_comparison_df)
            # Display metrics for price comparison
            col1, col2, col3 = st.columns(3)

            at_benchmark = len(
                price_comparison_df[price_comparison_df["PRICE_COMPARISON"] == "At Benchmark"])
            below_benchmark = len(
                price_comparison_df[price_comparison_df["PRICE_COMPARISON"] == "Below Benchmark"])
            above_benchmark = len(
                price_comparison_df[price_comparison_df["PRICE_COMPARISON"] == "Above Benchmark"])

            with col1:
                if comparison_filter in ["All", "At Benchmark"]:
                    st.metric("At Benchmark", at_benchmark)
            with col2:
                if comparison_filter in ["All", "Below Benchmark"]:
                    st.metric("Below Benchmark", below_benchmark)
            with col3:
                if comparison_filter in ["All", "Above Benchmark"]:
                    st.metric("Above Benchmark", above_benchmark)
    else:
        st.warning("No data available for price comparison.")

    st.markdown("---")

    # 3.2 Competitor Pricing Trends
    st.subheader("📉 Competitor Pricing Trends")

    # Add filters for competitor pricing
    stores = run_query(
        "SELECT DISTINCT BENCHMARK_STORE FROM Benchmark ORDER BY BENCHMARK_STORE;")
    brand_names = run_query(
        "SELECT DISTINCT BENCHMARK_BRAND_NAME FROM Benchmark ORDER BY BENCHMARK_BRAND_NAME;")

    # Multi-select filters for brand and store
    col1, col2 = st.columns(2)
    with col1:
        brands_list = brand_names["BENCHMARK_BRAND_NAME"].tolist()
        selected_brands = st.selectbox(
            "Filter by Brand", brands_list, key="competitor_brand")
    with col2:
        selected_stores = st.multiselect(
            "Filter by Store(s)", stores["BENCHMARK_STORE"].tolist(), default=stores["BENCHMARK_STORE"].tolist(), key="competitor_store")

    # Date filter
    date_range = st.date_input(
        "Select Date Range",
        value=(pd.to_datetime("2023-01-01"), pd.to_datetime("2025-12-31")),
        key="date_filter"
    )

    # Initialize a flag to track validation
    valid_inputs = True

    if len(date_range) != 2:
        start_date, end_date = None, None
        valid_inputs = False
    else:
        start_date, end_date = date_range

    if not selected_stores:
        valid_inputs = False

    # Execute the query only if inputs are valid
    if valid_inputs:
        # Competitor pricing query with filters
        competitor_pricing_query = f"""
            SELECT
                b.BENCHMARK_BRAND_NAME,
                pr.BENCHMARK_SITE_PRICE,
                b.BENCHMARK_STORE,
                b.BENCHMARK_CATG,
                b.BENCHMARK_SUBCATG,
                pr.PRICE_SCRAPE_DATE
            FROM
                Benchmark b
            JOIN
                Pricing pr ON b.BENCHMARK_ID = pr.BENCHMARK_ID
            WHERE
                pr.BENCHMARK_SITE_PRICE IS NOT NULL
                AND b.BENCHMARK_BRAND_NAME = '{selected_brands}'
                AND b.BENCHMARK_STORE IN ({','.join([f"'{store}'" for store in selected_stores])})
                AND pr.PRICE_SCRAPE_DATE BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                pr.BENCHMARK_SITE_PRICE DESC;
            """

        competitor_pricing_df = run_query(competitor_pricing_query)

        if competitor_pricing_df is not None and not competitor_pricing_df.empty:
            # Create a line chart
            fig = px.line(
                competitor_pricing_df,
                x="BENCHMARK_SITE_PRICE",
                y="PRICE_SCRAPE_DATE",
                color="BENCHMARK_STORE",
                line_group="BENCHMARK_STORE",
                title="Competitor Pricing Trends Over Time",
                labels={
                    "PRICE_SCRAPE_DATE": "Date",
                    "BENCHMARK_SITE_PRICE": "Price",
                    "BENCHMARK_BRAND_NAME": "Brand"
                },
                hover_data=["BENCHMARK_CATG", "BENCHMARK_SUBCATG"]
            )
            st.plotly_chart(fig)
        else:
            st.warning("No data available for competitor pricing trends.")
    else:
        st.info("Please adjust the filters above to see competitor pricing trends.")

    st.markdown("---")

    # 3.3 Benchmark Category Performance
    st.subheader("📊 Benchmark Category Performance")

    benchmark_category_query = """
    SELECT
        b.BENCHMARK_CATG,
        b.BENCHMARK_SUBCATG,
        SUM(s.TOTAL_SALE_AMOUNT) AS TOTAL_SALES
    FROM
        Sales s
    JOIN
        Benchmark b ON s.ITEM_ID = b.BENCHMARK_ID
    GROUP BY
        b.BENCHMARK_CATG, b.BENCHMARK_SUBCATG
    ORDER BY
        TOTAL_SALES DESC;
    """
    benchmark_category_df = run_query(benchmark_category_query)

    if benchmark_category_df is not None and not benchmark_category_df.empty:
        # Create a sunburst chart
        fig = px.sunburst(
            benchmark_category_df,
            path=["BENCHMARK_CATG", "BENCHMARK_SUBCATG"],
            values="TOTAL_SALES",
            color="TOTAL_SALES",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig)
    else:
        st.warning("No data available for benchmark category performance.")

    st.markdown("---")

    # 4. Customer Insights
    st.header("👥 Customer Insights")

    # 4.1 Top Payment Methods
    st.subheader("💳 Top Payment Methods")

    payment_methods_query = """
    SELECT
        s.PAYMENT_METHOD,
        COUNT(s.SALE_ID) AS TOTAL_TRANSACTIONS
    FROM
        Sales s
    GROUP BY
        s.PAYMENT_METHOD
    ORDER BY
        TOTAL_TRANSACTIONS DESC;
    """
    payment_methods_df = run_query(payment_methods_query)

    if payment_methods_df is not None and not payment_methods_df.empty:
        # Create a pie chart using plotly
        fig = px.pie(
            payment_methods_df,
            values="TOTAL_TRANSACTIONS",
            names="PAYMENT_METHOD",
        )

        # Display the pie chart in Streamlit
        st.plotly_chart(fig)
    else:
        st.warning("No data available for payment methods.")

    st.markdown("---")

    # 4.2 Customer Segmentation
    st.subheader("📂 Customer Segmentation")

    customer_segmentation_query = """
    SELECT
        s.CUSTOMER_ID,
        SUM(s.TOTAL_SALE_AMOUNT) AS TOTAL_SPENDING,
        AVG(s.TOTAL_SALE_AMOUNT) AS AVERAGE_ORDER_VALUE,
        COUNT(s.SALE_ID) AS PURCHASE_FREQUENCY
    FROM
        Sales s
    GROUP BY
        s.CUSTOMER_ID
    ORDER BY
        TOTAL_SPENDING DESC;
    """
    customer_segmentation_df = run_query(customer_segmentation_query)

    # Add filters for customer segmentation
    col1, col2 = st.columns(2)
    with col1:
        min_spending = st.number_input(
            "Minimum Total Spending", min_value=min(customer_segmentation_df["TOTAL_SPENDING"]), max_value=max(customer_segmentation_df["TOTAL_SPENDING"]), key="customer_min_spending")
    with col2:
        min_frequency = st.number_input(
            "Minimum Purchase Frequency", min_value=min(customer_segmentation_df["PURCHASE_FREQUENCY"]), max_value=max(customer_segmentation_df["PURCHASE_FREQUENCY"]), key="customer_min_frequency")

    customer_segmentation_df = customer_segmentation_df[
        (customer_segmentation_df["TOTAL_SPENDING"] >= min_spending) & (
            customer_segmentation_df["PURCHASE_FREQUENCY"] >= min_frequency)]

    if customer_segmentation_df is not None and not customer_segmentation_df.empty:
        # Create a bubble chart
        fig = px.scatter(
            customer_segmentation_df,
            x="PURCHASE_FREQUENCY",
            y="TOTAL_SPENDING",
            size="AVERAGE_ORDER_VALUE",
            color="CUSTOMER_ID",
            labels={"PURCHASE_FREQUENCY": "Purchase Frequency",
                    "TOTAL_SPENDING": "Total Spending"},
            hover_data=["CUSTOMER_ID"]
        )
        st.plotly_chart(fig)
        st.metric("Total Customers", len(customer_segmentation_df))
    else:
        st.warning("No data available for customer segmentation.")

    st.markdown("---")
    st.markdown("*Dashboard last updated: December 29, 2025*")
