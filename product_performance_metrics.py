import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import plotly.express as px


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
            st.metric("Top Seller Qty", int(
                top_df["TOTAL_QUANTITY_SOLD"].max()))
            st.metric("Total Products Sold", int(
                top_df["TOTAL_QUANTITY_SOLD"].sum()))

        with col3:
            st.metric("Avg Qty per Product", int(
                top_df["TOTAL_QUANTITY_SOLD"].mean()))
            st.metric("Total Sales Transactions",
                      int(top_df["TOTAL_SALES"].sum()))
    else:
        st.warning("No data available.")

    st.markdown("---")

    # 2. Average Sale Price per Product
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
        search_term = st.text_input("",
                                    placeholder=f"Search for a {selected_brand} product" if selected_brand != "All" else "Search for a product",
                                    help="Enter product name to check average sale price")
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
            # Limit to top 15 for better visualization
            top_n = st.slider("Number of products to display", 5, 30, 15)
            display_df = filtered_df.head(top_n)

            # Calculate error ranges
            display_df['ERROR_MINUS'] = display_df['AVERAGE_SALE_PRICE'] - \
                display_df['MIN_PRICE']
            display_df['ERROR_PLUS'] = display_df['MAX_PRICE'] - \
                display_df['AVERAGE_SALE_PRICE']

            # Create figure with price range
            fig = go.Figure()

            # Add bar chart with error bars
            fig.add_trace(go.Bar(
                x=display_df['PRODUCT_TITLE'],
                y=display_df['AVERAGE_SALE_PRICE'],
                name='Average Price',
                error_y=dict(
                    type='data',
                    symmetric=False,
                    array=display_df['ERROR_PLUS'],
                    arrayminus=display_df['ERROR_MINUS'],
                    color='rgba(255, 0, 0, 0.3)',
                    thickness=1.5,
                    width=4,
                ),
                marker=dict(
                    color=display_df['TOTAL_SALES'],
                    colorscale='Blues',
                    showscale=True,
                    colorbar=dict(title="Total Sales")
                ),
                text=display_df['AVERAGE_SALE_PRICE'].round(2),
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>' +
                'Avg Price: $%{y:.2f}<br>' +
                'Min: $' + display_df['MIN_PRICE'].round(2).astype(str) + '<br>' +
                'Max: $' + display_df['MAX_PRICE'].round(2).astype(str) + '<br>' +
                'Total Sales: %{marker.color}<br>' +
                '<extra></extra>'
            ))

            fig.update_layout(
                title="Product Pricing Analysis (Avg with Min-Max Range)",
                xaxis_title="",
                yaxis_title="Price ($)",
                height=600,
                hovermode='x unified',
                xaxis=dict(tickangle=-45),
                showlegend=False
            )

            st.plotly_chart(fig, width='stretch')
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
        # Create columns based on number of availability statuses
        col_count = len(availability_df)
        cols = st.columns(col_count)

        # Display each metric in its respective column
        for idx, (_, row) in enumerate(availability_df.iterrows()):
            with cols[idx]:
                st.metric(
                    label=f"{row['AVAILABILITY_INDICATOR']}",
                    value=int(row['PRODUCT_COUNT'])
                )

        # Search input
        product_search = st.text_input(
            "",
            placeholder=f"Search for a {selected_brand} product" if selected_brand != "All" else "Search for a product",
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
                st.success(
                    f"Found {len(product_avail_df)} product(s) matching '{product_search}'")

                # Display results with color coding
                if len(product_avail_df) > 3:
                    st.warning(
                        "Too many results to display. Please refine your search.")
                else:
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
                st.info(
                    f"No products found matching '{product_search}'. Try a different search term.")
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
        p.BRAND,
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
        p.PRODUCT_TITLE, p.BRAND
    ORDER BY
        AVERAGE_RATING DESC
    """

    review_df = run_query(review_query)

    if review_df is not None and not review_df.empty:
        # Key Metrics Row
        col1, col2, col3, col4, col5 = st.columns(5)

        avg_overall = review_df['AVERAGE_RATING'].mean()
        total_products = len(review_df)
        highly_rated = len(review_df[review_df['AVERAGE_RATING'] >= 4.0])
        total_reviews = int(review_df['TOTAL_REVIEWS'].sum())

        with col1:
            st.metric("Overall Avg Rating", f"{avg_overall:.2f}⭐")
        with col2:
            st.metric("Highest Rated",
                      f"{review_df['AVERAGE_RATING'].max():.2f}⭐")
        with col3:
            st.metric("Lowest Rated",
                      f"{review_df['AVERAGE_RATING'].min():.2f}⭐")
        with col4:
            st.metric("Highly Rated (≥4.0)",
                      f"{highly_rated}/{total_products}")
        with col5:
            st.metric("Total Reviews", f"{total_reviews:,}")

        st.markdown("---")

        # Create two columns for visualizations
        viz_col1, viz_col2 = st.columns([3, 2])

        with viz_col1:
            st.markdown("#### 📊 Rating vs Review Volume Analysis")

            # Scatter plot: Rating vs Number of Reviews (Bubble Chart)
            fig_scatter = px.scatter(
                review_df.head(30),
                x='AVERAGE_RATING',
                y='TOTAL_REVIEWS',
                size='TOTAL_REVIEWS',
                color='AVERAGE_RATING',
                hover_name='PRODUCT_TITLE',
                hover_data={
                    'BRAND': True,
                    'AVERAGE_RATING': ':.2f',
                    'TOTAL_REVIEWS': ':,d'
                },
                labels={
                    'AVERAGE_RATING': 'Average Rating',
                    'TOTAL_REVIEWS': 'Number of Reviews'
                },
                color_continuous_scale='RdYlGn',
                size_max=50
            )

            # Add quadrant lines
            fig_scatter.add_hline(y=review_df['TOTAL_REVIEWS'].median(),
                                  line_dash="dash", line_color="gray",
                                  annotation_text="Median Reviews")
            fig_scatter.add_vline(x=4.0,
                                  line_dash="dash", line_color="orange",
                                  annotation_text="4.0 Rating Threshold")

            fig_scatter.update_layout(
                height=400,
                xaxis_range=[review_df['AVERAGE_RATING'].min() - 0.2, 5.2],
                showlegend=False
            )

            st.plotly_chart(fig_scatter, width='stretch')

            # Insight text
            high_rating_high_reviews = len(review_df[(review_df['AVERAGE_RATING'] >= 4.0) &
                                                     (review_df['TOTAL_REVIEWS'] >= review_df['TOTAL_REVIEWS'].median())])
            st.info(
                f"💡 **Insight:** {high_rating_high_reviews} products have both high ratings (≥4.0) AND above-median review counts - these are your star performers!")

        with viz_col2:
            st.markdown("#### 🎯 Rating Distribution")

            # Create rating categories
            review_df['RATING_CATEGORY'] = pd.cut(
                review_df['AVERAGE_RATING'],
                bins=[0, 2, 3, 4, 5],
                labels=['Poor (0-2)', 'Fair (2-3)',
                        'Good (3-4)', 'Excellent (4-5)']
            )

            rating_dist = review_df['RATING_CATEGORY'].value_counts(
            ).sort_index()

            # Pie chart for rating distribution
            fig_pie = go.Figure(data=[go.Pie(
                labels=rating_dist.index,
                values=rating_dist.values,
                hole=0.4,
                marker=dict(
                    colors=['#ff4444', '#ffaa00', '#66bb66', '#00cc66']),
                textinfo='label+percent',
                textposition='auto'
            )])

            fig_pie.update_layout(
                height=400,
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5)
            )

            st.plotly_chart(fig_pie, width='stretch')

        st.markdown("---")

        # Top and Bottom Performers with filtering
        st.markdown("### 🏆 Top & Bottom Performers")

        perf_col1, perf_col2 = st.columns(2)

        with perf_col1:
            st.markdown("#### ⭐ Top 5 Best Rated Products")
            top_5 = review_df.head(5).copy()

            # Create horizontal bar chart for top products
            fig_top = go.Figure(go.Bar(
                x=top_5['AVERAGE_RATING'],
                y=top_5['PRODUCT_TITLE'].str[:40] + '...',
                orientation='h',
                marker=dict(
                    color=top_5['AVERAGE_RATING'],
                    colorscale='Greens',
                    showscale=False
                ),
                text=top_5['AVERAGE_RATING'].round(2),
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>' +
                'Rating: %{x:.2f}⭐<br>' +
                'Reviews: ' + top_5['TOTAL_REVIEWS'].astype(str) + '<br>' +
                '<extra></extra>'
            ))

            fig_top.update_layout(
                height=300,
                xaxis_range=[0, 5.5],
                xaxis_title="Rating",
                yaxis_title="",
                showlegend=False,
                margin=dict(l=0, r=50, t=20, b=40)
            )

            st.plotly_chart(fig_top, width='stretch')

            # Display detailed info
            for idx, row in top_5.iterrows():
                st.success(
                    f"⭐ **{row['PRODUCT_TITLE'][:50]}**  \n"
                    f"Rating: **{row['AVERAGE_RATING']:.2f}** | Reviews: **{int(row['TOTAL_REVIEWS']):,}** | Brand: **{row['BRAND']}**"
                )

        with perf_col2:
            st.markdown("#### ⚠️ Bottom 5 Products (Need Attention)")
            bottom_5 = review_df.tail(5).copy()

            # Create horizontal bar chart for bottom products
            fig_bottom = go.Figure(go.Bar(
                x=bottom_5['AVERAGE_RATING'],
                y=bottom_5['PRODUCT_TITLE'].str[:40] + '...',
                orientation='h',
                marker=dict(
                    color=bottom_5['AVERAGE_RATING'],
                    colorscale='Reds',
                    showscale=False
                ),
                text=bottom_5['AVERAGE_RATING'].round(2),
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>' +
                'Rating: %{x:.2f}⭐<br>' +
                'Reviews: ' + bottom_5['TOTAL_REVIEWS'].astype(str) + '<br>' +
                '<extra></extra>'
            ))

            fig_bottom.update_layout(
                height=300,
                xaxis_range=[0, 5.5],
                xaxis_title="Rating",
                yaxis_title="",
                showlegend=False,
                margin=dict(l=0, r=50, t=20, b=40)
            )

            st.plotly_chart(fig_bottom, width='stretch')

            # Display detailed info
            for idx, row in bottom_5.iterrows():
                st.error(
                    f"❌ **{row['PRODUCT_TITLE'][:50]}**  \n"
                    f"Rating: **{row['AVERAGE_RATING']:.2f}** | Reviews: **{int(row['TOTAL_REVIEWS']):,}** | Brand: **{row['BRAND']}**"
                )

        st.markdown("---")

        # Interactive Data Table with Search
        st.markdown(f"### 🔍 Search {selected_brand} Product Reviews")

        search_product = st.text_input(
            "Search by product name or brand",
            placeholder="e.g., Samsung, Laptop, TV..."
        )

        # Filter options
        filter_col1, filter_col2 = st.columns(2)

        with filter_col1:
            min_rating = st.slider(
                "Minimum Rating",
                min_value=0.0,
                max_value=5.0,
                value=0.0,
                step=0.1
            )

        with filter_col2:
            min_reviews = st.number_input(
                "Minimum Review Count",
                min_value=0,
                value=0,
                step=10
            )

        # Apply filters
        filtered_reviews = review_df.copy()

        if search_product:
            filtered_reviews = filtered_reviews[
                (filtered_reviews['PRODUCT_TITLE'].str.contains(search_product, case=False, na=False)) |
                (filtered_reviews['BRAND'].str.contains(
                    search_product, case=False, na=False))
            ]

        filtered_reviews = filtered_reviews[
            (filtered_reviews['AVERAGE_RATING'] >= min_rating) &
            (filtered_reviews['TOTAL_REVIEWS'] >= min_reviews)
        ]

        if not filtered_reviews.empty:
            st.success(
                f"Found {len(filtered_reviews)} products matching criteria")

            # Display formatted dataframe
            display_cols = filtered_reviews[[
                'PRODUCT_TITLE', 'BRAND', 'AVERAGE_RATING', 'TOTAL_REVIEWS']].copy()
            display_cols['AVERAGE_RATING'] = display_cols['AVERAGE_RATING'].round(
                2)
            display_cols.columns = ['Product', 'Brand',
                                    'Avg Rating ⭐', 'Total Reviews']

            st.dataframe(
                display_cols,
                width='stretch',
                hide_index=True,
                height=400
            )
        else:
            st.warning("No products match the selected criteria")

    st.markdown("---")
    st.markdown("*Dashboard last updated: December 29, 2025*")
