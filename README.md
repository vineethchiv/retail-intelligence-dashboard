# Retail Intelligence Dashboard

A comprehensive Streamlit-based retail analytics dashboard powered by Snowflake, providing real-time insights into sales performance, product metrics, benchmarking analysis, and AI-driven customer intelligence.

## Features

### 📊 **Product Performance Metrics**
- Top 10 products by quantity sold
- Average sale price analysis with filtering capabilities
- Product availability status tracking
- Product review insights and ratings
- Search and price range filtering

### 📈 **Sales Performance Metrics**
- Multi-dimensional sales analysis (Brand, Category, Subcategory, Merchant)
- Interactive date range selection
- Sales trend visualization over time
- Top-performing products by brand
- Dynamic filter options with real-time updates
- Summary metrics (Total Sales, Average Daily Sales, Transaction Count)

### 💰 **Benchmarking & Customer Insights**
- Price comparison analysis (Above/Below/At Benchmark)
- Competitor pricing trends visualization
- Benchmark category performance sunburst charts
- Payment method analysis
- Customer segmentation with spending and purchase frequency analysis
- Multi-store and multi-brand filtering

### 💬 **Cortex AI Agent**
- Natural language queries for data insights
- Predefined suggestion prompts
- Interactive chat interface with data-driven responses
- Automatic chart generation (Line, Bar, Data Table)

## Tech Stack

- **Frontend**: [Streamlit](https://streamlit.io/)
- **Data Source**: [Snowflake](https://www.snowflake.com/)
- **Data Processing**: [Pandas](https://pandas.pydata.org/)
- **Visualization**: [Plotly](https://plotly.com/)
- **Database Connector**: [snowflake-connector-python](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example)
- **Environment Management**: [python-dotenv](https://python-dotenv.readthedocs.io/)

## Installation

### Prerequisites
- Python 3.8 or higher
- Snowflake account with appropriate credentials
- Internet connection

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd retail-intelligence-dashboard
   ```

2. **Create a virtual environment** (optional but recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   
   Create or update the `.env` file with your Snowflake credentials:
   ```env
   DATABASE = "YOUR_DATABASE_NAME"
   SCHEMA = "YOUR_SCHEMA_NAME"
   WAREHOUSE = "YOUR_WAREHOUSE_NAME"
   HOST = "YOUR_SNOWFLAKE_HOST"
   ACCOUNT = "YOUR_ACCOUNT_ID"
   USER = "YOUR_USERNAME"
   PASSWORD = "YOUR_PASSWORD"
   ROLE = "YOUR_ROLE"
   SEMANTIC_VIEW = "YOUR_SEMANTIC_VIEW_NAME"
   ```

## Usage

### Running the Dashboard

```bash
streamlit run dashboard.py
```

The dashboard will open in your default browser at `http://localhost:8501`

### Navigation

Use the sidebar to navigate between different sections:
- **Product Performance Metrics** - Analyze product sales and reviews
- **Sales Performance Metrics** - Track sales by brand, category, and merchant
- **Benchmarking and Customer Insights** - Compare prices and analyze customer behavior
- **Chat with Agent** - Ask AI-powered questions about your data

## Project Structure

```
retail-intelligence-dashboard/
├── dashboard.py                          # Main entry point and navigation
├── product_performance_metrics.py        # Product analytics module
├── sales_performance_metrics.py          # Sales analytics module
├── benchmarking_and_customer_insights.py # Benchmarking and customer analysis
├── cortex_analyst.py                     # AI chat agent integration
├── requirements.txt                      # Python dependencies
├── .env                                  # Environment variables (not in git)
├── .gitignore                            # Git ignore rules
└── README.md                             # This file
```

## Key Modules

### [dashboard.py](dashboard.py)
Main navigation hub that routes users to different analytical sections.

### [product_performance_metrics.py](product_performance_metrics.py)
Provides insights into product sales volume, pricing, availability, and customer reviews.

### [sales_performance_metrics.py](sales_performance_metrics.py)
Analyzes sales trends across brands, categories, subcategories, and merchants with temporal analysis.

### [benchmarking_and_customer_insights.py](benchmarking_and_customer_insights.py)
Compares pricing against benchmarks, tracks competitor trends, and segments customers by spending behavior.

### [cortex_analyst.py](cortex_analyst.py)
AI-powered chat interface using Snowflake Cortex for natural language data queries.

## Database Schema

The dashboard connects to Snowflake tables including:
- **Sales** - Transaction and sales data
- **Products** - Product catalog with brand and taxonomy information
- **Pricing** - Current and benchmark pricing data
- **Third_Party_Merchants** - Merchant information
- **Benchmark** - Competitive benchmark data
- **Availability** - Product availability status
- **Reviews** - Customer reviews and ratings
- **Customers** - Customer information

```SQL
CREATE TABLE Products (
    ITEM_ID INT AUTOINCREMENT PRIMARY KEY,
    ITEM_NAME VARCHAR(255),
    PRODUCT_TITLE VARCHAR(255),
    MODEL VARCHAR(100),
    SKU VARCHAR(100),
    TAXONOMY VARCHAR(255),
    WEIGHTS_AND_DIMENSIONS VARCHAR(100),
    BRAND VARCHAR(100),
    COMPANY_NAME VARCHAR(255)
);

CREATE TABLE Availability (
    ITEM_ID INT,
    AVAILABILITY_INDICATOR VARCHAR(50),
    PRIMARY KEY (ITEM_ID),
    FOREIGN KEY (ITEM_ID) REFERENCES Products(ITEM_ID)
);

CREATE TABLE Benchmark (
    BENCHMARK_ID INT AUTOINCREMENT PRIMARY KEY,
    BENCHMARK_BRAND_NAME VARCHAR(255),
    BENCHMARK_CATG VARCHAR(100),
    BENCHMARK_CATG_ID VARCHAR(50),
    BENCHMARK_COLOR_DESC VARCHAR(50),
    BENCHMARK_DEPT VARCHAR(255),
    BENCHMARK_ITEM_ATTRIBS VARCHAR(500),
    BENCHMARK_ITEM_MDL_NUM VARCHAR(100),
    BENCHMARK_ITEM_SUB_DESC VARCHAR(255),
    BENCHMARK_STORE VARCHAR(255),
    BENCHMARK_SUBCATG VARCHAR(100),
    BENCHMARK_UPC_NUM VARCHAR(100)
);

CREATE TABLE Pricing (
    ITEM_ID INT,
    PRODUCT_PRICE FLOAT,
    PRICE_SCRAPE_DATE DATE,
    BENCHMARK_ID INT,
    BENCHMARK_BASE_PRICE FLOAT,
    BENCHMARK_SITE_PRICE FLOAT,
    PRIMARY KEY (ITEM_ID, BENCHMARK_ID),
    FOREIGN KEY (ITEM_ID) REFERENCES Products(ITEM_ID),
    FOREIGN KEY (BENCHMARK_ID) REFERENCES Benchmark(BENCHMARK_ID)
);

CREATE TABLE Reviews (
    ITEM_ID INT,
    ITEM_REVIEW_COUNT INT,
    ITEM_REVIEW_RATING FLOAT,
    PRIMARY KEY (ITEM_ID),
    FOREIGN KEY (ITEM_ID) REFERENCES Products(ITEM_ID)
);

CREATE TABLE Third_Party_Merchants (
    MERCHANT_ID INT AUTOINCREMENT PRIMARY KEY,
    THIRD_PARTY_MERCHANT_NAME VARCHAR(255)
);

CREATE TABLE Product_Merchant_Mapping (
    ITEM_ID INT,
    MERCHANT_ID INT,
    PRIMARY KEY (ITEM_ID, MERCHANT_ID),
    FOREIGN KEY (ITEM_ID) REFERENCES Products(ITEM_ID),
    FOREIGN KEY (MERCHANT_ID) REFERENCES Third_Party_Merchants(MERCHANT_ID)
);

CREATE TABLE Sales (
SALE_ID INT AUTOINCREMENT PRIMARY KEY,
    ITEM_ID INT,
    MERCHANT_ID INT,
    SALE_DATE DATE,
    QUANTITY_SOLD INT,
    SALE_PRICE FLOAT,
    TOTAL_SALE_AMOUNT FLOAT,
    DISCOUNT_APPLIED FLOAT,
    CUSTOMER_ID INT,
    PAYMENT_METHOD VARCHAR(50),
    FOREIGN KEY (ITEM_ID) REFERENCES Products(ITEM_ID),
    FOREIGN KEY (MERCHANT_ID) REFERENCES Third_Party_Merchants(MERCHANT_ID)
);
```

## Features & Capabilities

### Real-time Caching
- `@st.cache_resource` for persistent database connections
- `@st.cache_data` for query results (1-hour TTL)
- Improves dashboard performance and reduces database load

### Error Handling
- Comprehensive try-catch blocks for database operations
- User-friendly error messages
- Graceful degradation when data is unavailable

### Interactive Filtering
- Multi-select filters for flexible data exploration
- Dynamic subcategory loading based on category selection
- Date range selection with validation
- Search functionality for product titles

### Visualizations
- Bar charts for comparative analysis
- Line charts for trend analysis
- Sunburst charts for hierarchical data
- Pie charts for distribution analysis
- Bubble charts for multi-dimensional analysis

## Performance Optimization

- Streamlit caching mechanisms to reduce redundant queries
- Conditional chart rendering based on filter selections
- Efficient Snowflake SQL queries with aggregation
- Lazy loading of data based on user selections

## Security

⚠️ **Important**: Never commit `.env` file with credentials to version control. The `.gitignore` is configured to exclude sensitive files.

- Credentials are stored in `.env` file (not in version control)
- Use environment variables for all sensitive information
- Implement role-based access control through Snowflake roles

## Troubleshooting

### Connection Issues
- Verify Snowflake credentials in `.env` file
- Check network connectivity to Snowflake
- Ensure warehouse is active and running

### Data Not Loading
- Check if tables exist in the specified schema
- Verify SQL queries for syntax errors
- Confirm user has appropriate Snowflake permissions

### Performance Issues
- Clear Streamlit cache: Delete `.streamlit/` directory
- Increase warehouse compute resources
- Optimize SQL queries for better performance

## Future Enhancements

- [ ] Advanced anomaly detection
- [ ] Predictive analytics capabilities
- [ ] Real-time alerts and notifications
- [ ] Custom dashboard builder
- [ ] Mobile-responsive design improvements

## Support

For issues or questions, please open an issue in the repository or contact the development team.

---

**Last Updated**: December 30, 2025