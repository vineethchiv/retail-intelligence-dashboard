import streamlit as st
from product_performance_metrics import product_performance_metrics
from sales_performance_metrics import sales_performance_metrics
from benchmarking_and_customer_insights import benchmarking_and_customer_insights
from cortex_analyst import cortext_analyst

# Sidebar navigation
st.sidebar.title("Navigation")
selection = st.sidebar.radio("Go to",
                             ["Product Performance Metrics",
                              "Sales Performance Metrics",
                              "Benchmarking and Customer Insights",
                              "Chat with Agent"])

if selection == "Product Performance Metrics":
    product_performance_metrics()
elif selection == "Sales Performance Metrics":
    sales_performance_metrics()
elif selection == "Benchmarking and Customer Insights":
    benchmarking_and_customer_insights()
elif selection == "Chat with Agent":
    cortext_analyst()
