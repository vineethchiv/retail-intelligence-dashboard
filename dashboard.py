import streamlit as st
from streamlit_option_menu import option_menu
from product_performance_metrics import product_performance_metrics
from sales_performance_metrics import sales_performance_metrics
from benchmarking_and_customer_insights import benchmarking_and_customer_insights
from cortex_analyst import cortext_analyst


# Sidebar navigation with modern menu
with st.sidebar:
    st.markdown("### Retail Intelligence Dashboard")

    selection = option_menu(
        menu_title=None,  # No menu title
        options=["Product Performance", "Sales Performance",
                 "Benchmarking & Insights", "Chat with Agent"],
        icons=["box-seam", "graph-up-arrow",
               "people", "chat-dots"],  # Bootstrap icons
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {
                "padding": "5px",
                "background-color": "#262730"
            },
            "icon": {
                "color": "#007AFF",
                "font-size": "18px"
            },
            "nav-link": {
                "font-size": "15px",
                "text-align": "left",
                "margin": "5px",
                "padding": "10px 15px",
                "border-radius": "8px",
                "--hover-color": "#f0f8ff13",
            },
            "nav-link-selected": {
                "background": "linear-gradient(135deg, #67bed9 0%, #0051D5 100%)",
                "color": "white",
                "font-weight": "600",
                "box-shadow": "0 4px 12px rgba(0, 122, 255, 0.3)"
            },
        }
    )

# Map selections to functions
if selection == "Product Performance":
    product_performance_metrics()
elif selection == "Sales Performance":
    sales_performance_metrics()
elif selection == "Benchmarking & Insights":
    benchmarking_and_customer_insights()
elif selection == "Chat with Agent":
    cortext_analyst()
