import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

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


@st.cache_data
def run_query(query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return []


def sales_performance_metrics():
    # Fetch filter options with error handling
    try:
        brands = run_query(
            "SELECT DISTINCT BRAND FROM Products ORDER BY BRAND;")
        categories = run_query(
            "SELECT DISTINCT BENCHMARK_CATG FROM BENCHMARK ORDER BY BENCHMARK_CATG;")
        merchants = run_query(
            "SELECT DISTINCT THIRD_PARTY_MERCHANT_NAME FROM Third_Party_Merchants ORDER BY THIRD_PARTY_MERCHANT_NAME;")
    except Exception as e:
        st.error(f"Error fetching filter options: {e}")
        st.stop()

    # Streamlit UI for filters
    st.title("📈 Sales Performance Metrics")
    st.markdown("---")

    col1, col2 = st.columns(2)
    with col2:
        selected_category = st.selectbox(
            "Select Category", options=["All"] + categories)
        selected_merchant = st.selectbox(
            "Select Merchant", options=["All"] + merchants)

    with col1:
        selected_brand = st.selectbox("Select Brand", options=["All"] + brands)

        # Dynamically fetch subcategories based on the selected category
        if selected_category != "All":
            subcategories = run_query(
                f"SELECT DISTINCT BENCHMARK_SUBCATG FROM BENCHMARK WHERE BENCHMARK_CATG = '{selected_category}' ORDER BY BENCHMARK_SUBCATG;")
        else:
            subcategories = run_query(
                "SELECT DISTINCT BENCHMARK_SUBCATG FROM BENCHMARK ORDER BY BENCHMARK_SUBCATG;")

        selected_subcategory = st.selectbox(
            "Select Subcategory", options=["All"] + subcategories)

    date_range = st.date_input(
        "Select Date Range",
        value=(pd.to_datetime("2023-01-01"), pd.to_datetime("2025-12-31")),
        max_value=pd.to_datetime("2025-12-31")
    )

    # Handle invalid date range
    if len(date_range) != 2:
        st.error("Please select a valid start and end date.")
        st.stop()

    start_date, end_date = date_range

    # Validate date range
    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    # Convert dates to strings for Snowflake
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Prepare filter values
    brand_filter = None if selected_brand == "All" else selected_brand
    category_filter = None if selected_category == "All" else selected_category
    subcategory_filter = None if selected_subcategory == "All" else selected_subcategory
    merchant_filter = None if selected_merchant == "All" else selected_merchant

    # Query to fetch results with proper filtering
    query = """
    SELECT 
        p.BRAND,
        SPLIT_PART(p.TAXONOMY, ' > ', 1) AS CATEGORY,
        SPLIT_PART(p.TAXONOMY, ' > ', 2) AS SUBCATEGORY,
        s.MERCHANT_ID,
        m.THIRD_PARTY_MERCHANT_NAME,
        s.SALE_DATE,
        SUM(s.TOTAL_SALE_AMOUNT) AS TOTAL_SALES
    FROM 
        Sales s
    JOIN 
        Products p ON s.ITEM_ID = p.ITEM_ID
    JOIN 
        Third_Party_Merchants m ON s.MERCHANT_ID = m.MERCHANT_ID
    WHERE 
        s.SALE_DATE BETWEEN %s AND %s
        AND (%s IS NULL OR p.BRAND = %s)
        AND (%s IS NULL OR SPLIT_PART(p.TAXONOMY, ' > ', 1) = %s)
        AND (%s IS NULL OR SPLIT_PART(p.TAXONOMY, ' > ', 2) = %s)
        AND (%s IS NULL OR m.THIRD_PARTY_MERCHANT_NAME = %s)
    GROUP BY 
        p.BRAND, 
        SPLIT_PART(p.TAXONOMY, ' > ', 1), 
        SPLIT_PART(p.TAXONOMY, ' > ', 2),
        s.MERCHANT_ID, 
        m.THIRD_PARTY_MERCHANT_NAME, 
        s.SALE_DATE
    ORDER BY 
        s.SALE_DATE, TOTAL_SALES DESC;
    """

    params = (
        start_date_str, end_date_str,
        brand_filter, brand_filter,
        category_filter, category_filter,
        subcategory_filter, subcategory_filter,
        merchant_filter, merchant_filter
    )

    # Fetch results with error handling

    @st.cache_data
    def fetch_results(query, params):
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = cur.fetchall()
                return columns, results
        except Exception as e:
            st.error(f"Error fetching results: {e}")
            return None, None

    columns, results = fetch_results(query, params)

    # Display results or handle empty results
    if results:
        try:
            df = pd.DataFrame(results, columns=columns)
            df['SALE_DATE'] = pd.to_datetime(df['SALE_DATE'])

            st.markdown("---")

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Sales", f"${df['TOTAL_SALES'].sum():,.2f}")
            with col2:
                st.metric("Average Daily Sales",
                          f"${df.groupby('SALE_DATE')['TOTAL_SALES'].sum().mean():,.2f}")
            with col3:
                st.metric("Number of Transactions", len(df))

            st.markdown("---")

            # Conditional Display: Total Sales by Brand
            if selected_brand == "All":
                st.subheader("Total Sales by Brand")
                sales_by_brand = df.groupby(
                    'BRAND')['TOTAL_SALES'].sum().sort_values(ascending=False)
                st.bar_chart(sales_by_brand)

            # Conditional Display: Total Sales by Category
            if selected_category == "All":
                st.subheader("Total Sales by Category")
                sales_by_category = df.groupby(
                    'CATEGORY')['TOTAL_SALES'].sum().sort_values(ascending=False)
                st.bar_chart(sales_by_category)

            # Conditional Display: Total Sales by Subcategory
            if selected_subcategory == "All" and selected_category != "All":
                st.subheader("Total Sales by Subcategory")
                sales_by_subcategory = df.groupby(
                    'SUBCATEGORY')['TOTAL_SALES'].sum().sort_values(ascending=False)
                st.bar_chart(sales_by_subcategory)

            # Conditional Display: Total Sales by Merchant
            if selected_merchant == "All":
                st.subheader("Total Sales by Merchant")
                sales_by_merchant = df.groupby('THIRD_PARTY_MERCHANT_NAME')[
                    'TOTAL_SALES'].sum().sort_values(ascending=False)
                st.bar_chart(sales_by_merchant)

            # Total Sales Over Time (always show)
            st.subheader("Total Sales Over Time")
            sales_over_time = df.groupby('SALE_DATE')[
                'TOTAL_SALES'].sum().reset_index()
            st.line_chart(sales_over_time.set_index('SALE_DATE'))

        except Exception as e:
            st.error(f"Error displaying results: {e}")
    else:
        st.warning(
            "No data found for the selected filters. Please adjust your filter criteria and try again.")

    # Query to fetch top-performing products for each brand
    top_products_query = """
    WITH ProductSales AS (
        SELECT 
            p.BRAND,
            p.PRODUCT_TITLE,
            SUM(s.TOTAL_SALE_AMOUNT) AS TOTAL_SALES
        FROM 
            Sales s
        JOIN 
            Products p ON s.ITEM_ID = p.ITEM_ID
        WHERE 
            s.SALE_DATE BETWEEN %s AND %s
            AND (%s IS NULL OR p.BRAND = %s)
        GROUP BY 
            p.BRAND, p.PRODUCT_TITLE
    ),
    RankedProducts AS (
        SELECT 
            BRAND,
            PRODUCT_TITLE,
            TOTAL_SALES,
            RANK() OVER (PARTITION BY BRAND ORDER BY TOTAL_SALES DESC) AS RANK
        FROM 
            ProductSales
    )
    SELECT 
        BRAND,
        PRODUCT_TITLE,
        TOTAL_SALES
    FROM 
        RankedProducts
    WHERE 
        RANK <= 5
    ORDER BY 
        BRAND, RANK;
    """

    # Parameters for the query
    top_products_params = (
        start_date_str, end_date_str,
        brand_filter, brand_filter
    )

    # Fetch top-performing products

    @st.cache_data
    def fetch_top_products(query, params):
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = cur.fetchall()
                return columns, results
        except Exception as e:
            st.error(f"Error fetching top-performing products: {e}")
            return None, None

    top_columns, top_results = fetch_top_products(
        top_products_query, top_products_params)

    # Display top-performing products
    if top_results:
        try:
            top_df = pd.DataFrame(top_results, columns=top_columns)

            st.markdown("---")

            # Display a bar chart for each brand
            for brand in top_df['BRAND'].unique():
                st.subheader(f"Top 5 Products for {brand}")
                brand_data = top_df[top_df['BRAND'] == brand]
                st.bar_chart(brand_data.set_index(
                    'PRODUCT_TITLE')['TOTAL_SALES'])

        except Exception as e:
            st.error(f"Error displaying top-performing products: {e}")
    else:
        st.warning("No data found for top-performing products.")

    st.markdown("---")
    st.markdown("*Dashboard last updated: December 29, 2025*")
