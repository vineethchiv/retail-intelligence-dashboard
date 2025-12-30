import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Page configuration for better performance
st.set_page_config(page_title="Product Performance Metrics",
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

def product_performance_metrics():
    # Streamlit UI
    st.title("📦 Product Performance Metrics")
    st.markdown("---")

    # Add a brand filter dropdown
    brand_query = "SELECT DISTINCT BRAND FROM Products ORDER BY BRAND;"
    brands_df = run_query(brand_query)

    if brands_df is not None and not brands_df.empty:
        brands = ["All"] + brands_df["BRAND"].tolist()
        selected_brand = st.selectbox("Filter by Brand", options=brands)
    else:
        st.error("No brands found.")
        st.stop()

    # Apply brand filter to queries
    brand_filter_condition = f"AND p.BRAND = '{selected_brand}'" if selected_brand != "All" else ""

    # 1. Top 10 Products by Quantity Sold
    st.subheader("📈 Top 10 Products by Quantity Sold")
    col1, col2, col3 = st.columns([2, 1, 1])

    top_products_query = f"""
    SELECT 
        p.PRODUCT_TITLE,
        SUM(s.QUANTITY_SOLD) AS TOTAL_QUANTITY_SOLD,
        COUNT(s.SALE_ID) AS TOTAL_SALES
    FROM 
        Sales s
    JOIN 
        Products p ON s.ITEM_ID = p.ITEM_ID
    WHERE 
        1=1 {brand_filter_condition}
    GROUP BY 
        p.PRODUCT_TITLE
    ORDER BY 
        TOTAL_QUANTITY_SOLD DESC
    LIMIT 10
    """
    top_df = run_query(top_products_query)

    if top_df is not None and not top_df.empty:
        with col1:
            st.bar_chart(top_df.set_index("PRODUCT_TITLE")[
                        "TOTAL_QUANTITY_SOLD"], width='stretch')

        with col2:
            st.metric("Top Seller Qty", int(top_df["TOTAL_QUANTITY_SOLD"].max()))
            st.metric("Total Products Sold", int(
                top_df["TOTAL_QUANTITY_SOLD"].sum()))

        with col3:
            st.metric("Avg Qty per Product", int(
                top_df["TOTAL_QUANTITY_SOLD"].mean()))
            st.metric("Total Sales Transactions", int(top_df["TOTAL_SALES"].sum()))
    else:
        st.warning("No data available.")

    st.markdown("---")

    # 2. Average Sale Price per Product


    @st.cache_data
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')


    st.subheader("💰 Average Sale Price per Product")

    avg_price_query = f"""
    SELECT 
        p.PRODUCT_TITLE,
        AVG(s.SALE_PRICE) AS AVERAGE_SALE_PRICE,
        COUNT(s.SALE_ID) AS TOTAL_SALES,
        MIN(s.SALE_PRICE) AS MIN_PRICE,
        MAX(s.SALE_PRICE) AS MAX_PRICE
    FROM 
        Sales s
    JOIN 
        Products p ON s.ITEM_ID = p.ITEM_ID
    WHERE 
        1=1 {brand_filter_condition}
    GROUP BY 
        p.PRODUCT_TITLE
    ORDER BY 
        AVERAGE_SALE_PRICE DESC
    """

    avg_price_df = run_query(avg_price_query)

    if avg_price_df is not None and not avg_price_df.empty:
        # Filter by product title
        search_term = st.text_input("Search Product Title", "")
        filtered_df = avg_price_df[avg_price_df["PRODUCT_TITLE"].str.contains(
            search_term, case=False, na=False)]

        # Filter by price range
        min_price, max_price = st.slider(
            "Filter by Average Sale Price",
            min_value=float(avg_price_df["AVERAGE_SALE_PRICE"].min()),
            max_value=float(avg_price_df["AVERAGE_SALE_PRICE"].max()),
            value=(float(avg_price_df["AVERAGE_SALE_PRICE"].min()), float(
                avg_price_df["AVERAGE_SALE_PRICE"].max()))
        )
        filtered_df = filtered_df[
            (filtered_df["AVERAGE_SALE_PRICE"] >= min_price) &
            (filtered_df["AVERAGE_SALE_PRICE"] <= max_price)
        ]

        if filtered_df is not None and not filtered_df.empty:
            # Display filtered data
            st.dataframe(
                filtered_df[["PRODUCT_TITLE", "AVERAGE_SALE_PRICE",
                            "TOTAL_SALES", "MIN_PRICE", "MAX_PRICE"]],
                width='stretch'
            )
        else:
            st.warning("No products match the search criteria.")
    else:
        st.warning("No data available.")

    st.markdown("---")

    # 3. Product Availability Status
    st.subheader("📦 Product Availability Status")

    availability_query = f"""
    SELECT
        a.AVAILABILITY_INDICATOR,
        COUNT(a.ITEM_ID) AS PRODUCT_COUNT
    FROM
        Availability a
    JOIN
        Products p ON a.ITEM_ID = p.ITEM_ID
    WHERE 
        1=1 {brand_filter_condition}
    GROUP BY
        a.AVAILABILITY_INDICATOR
    ORDER BY
        PRODUCT_COUNT DESC
    """

    availability_df = run_query(availability_query)

    if availability_df is not None and not availability_df.empty:
        col1, col2 = st.columns([1, 1])

        with col1:
            st.bar_chart(availability_df.set_index("AVAILABILITY_INDICATOR")[
                        "PRODUCT_COUNT"], width='stretch')

        with col2:
            for _, row in availability_df.iterrows():
                st.metric(f"Status: {row['AVAILABILITY_INDICATOR']}", int(
                    row['PRODUCT_COUNT']))
                
        # Search input
        product_search = st.text_input(
            "Search for a product by name",
            placeholder="e.g., Samsung, TV, Laptop...",
            help="Enter product name to check availability status"
        )

        if product_search:
            # Query to search products with availability
            product_availability_query = f"""
            SELECT
                p.PRODUCT_TITLE,
                p.BRAND,
                a.AVAILABILITY_INDICATOR,
                p.SKU
            FROM
                Products p
            JOIN
                Availability a ON p.ITEM_ID = a.ITEM_ID
            WHERE
                (LOWER(p.PRODUCT_TITLE) LIKE LOWER('%{product_search}%')
                OR LOWER(p.BRAND) LIKE LOWER('%{product_search}%'))
                {brand_filter_condition}
            ORDER BY
                a.AVAILABILITY_INDICATOR, p.PRODUCT_TITLE
            """
            
            product_avail_df = run_query(product_availability_query)
            
            if product_avail_df is not None and not product_avail_df.empty:
                st.success(f"Found {len(product_avail_df)} product(s) matching '{product_search}'")
                
                # Display results with color coding
                for _, row in product_avail_df.iterrows():
                    status = row['AVAILABILITY_INDICATOR']
                    
                    # Color code based on availability
                    if 'IN_STOCK' in status:
                        st.success(
                            f"✅ **{row['PRODUCT_TITLE']}**\n\n"
                            f"Brand: {row['BRAND']} | SKU: {row['SKU']} | Status: **{status}**"
                        )
                    elif 'LIMITED_STOCK' in status:
                        st.warning(
                            f"⚠️ **{row['PRODUCT_TITLE']}**\n\n"
                            f"Brand: {row['BRAND']} | SKU: {row['SKU']} | Status: **{status}**"
                        )
                    else:
                        st.error(
                            f"❌ **{row['PRODUCT_TITLE']}**\n\n"
                            f"Brand: {row['BRAND']} | SKU: {row['SKU']} | Status: **{status}**"
                        )
            else:
                st.info(f"No products found matching '{product_search}'. Try a different search term.")
        else:
            st.info("👆 Enter a product name above to check its availability status")

    else:
        st.warning("No data available.")

    st.markdown("---")

    # 4. Product Review Insights
    st.subheader("⭐ Product Review Insights")

    review_query = f"""
    SELECT
        p.PRODUCT_TITLE,
        AVG(r.ITEM_REVIEW_RATING) AS AVERAGE_RATING,
        SUM(r.ITEM_REVIEW_COUNT) AS TOTAL_REVIEWS,
        COUNT(DISTINCT p.ITEM_ID) AS PRODUCT_COUNT
    FROM
        Reviews r
    JOIN
        Products p ON r.ITEM_ID = p.ITEM_ID
    WHERE 
        1=1 {brand_filter_condition}
    GROUP BY
        p.PRODUCT_TITLE
    ORDER BY
        AVERAGE_RATING DESC
    """

    review_df = run_query(review_query)

    if review_df is not None and not review_df.empty:
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Avg Rating (All)",
                    f"{review_df['AVERAGE_RATING'].mean():.2f}⭐")
        with col2:
            st.metric("Highest Rated",
                    f"{review_df['AVERAGE_RATING'].max():.2f}⭐")
        with col3:
            st.metric("Lowest Rated",
                    f"{review_df['AVERAGE_RATING'].min():.2f}⭐")
        with col4:
            st.metric("Total Reviews", int(review_df['TOTAL_REVIEWS'].sum()))

        # Highlights
        st.markdown("### 🏆 Top Performer")
        col1, col2 = st.columns(2)

        highest_rated = review_df.iloc[0]
        with col1:
            st.success(
                f"⭐ **{highest_rated['PRODUCT_TITLE']}**\nRating: **{highest_rated['AVERAGE_RATING']:.2f}** | Reviews: **{int(highest_rated['TOTAL_REVIEWS'])}**"
            )

        if len(review_df) > 1:
            lowest_rated = review_df.iloc[-1]
            with col2:
                st.warning(
                    f"⚠️ **{lowest_rated['PRODUCT_TITLE']}**\nRating: **{lowest_rated['AVERAGE_RATING']:.2f}** | Reviews: **{int(lowest_rated['TOTAL_REVIEWS'])}** (Needs Attention)"
                )
    else:
        st.warning("No reviews found.")

    st.markdown("---")
    st.markdown("*Dashboard last updated: December 29, 2025*")
