from typing import Any, Dict, List
import pandas as pd
import requests
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
import os

load_dotenv()

# Custom CSS for chat interface styling


def load_css():
    st.markdown("""
        <style>
        /* Move user messages to the right */
        .stChatMessage[data-testid="user-message"] {
            flex-direction: row-reverse;
            text-align: right;
        }
        
        /* Style for user message content */
        .stChatMessage[data-testid="user-message"] .stMarkdown {
            background-color: #007AFF;
            color: white;
            padding: 10px 15px;
            border-radius: 18px;
            display: inline-block;
            max-width: 70%;
        }
        
        /* Style for assistant message content */
        .stChatMessage[data-testid="assistant-message"] .stMarkdown {
            background-color: #E8E8E8;
            color: black;
            padding: 10px 15px;
            border-radius: 18px;
            display: inline-block;
            max-width: 70%;
        }
        
        /* Hide default streamlit elements for cleaner look */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        
        /* Chat container */
        .chat-container {
            max-width: 800px;
            margin: 0 auto;
        }
        
        /* Title styling */
        h1 {
            text-align: center;
            padding: 20px 0;
        }
        </style>
    """, unsafe_allow_html=True)


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


def send_message(prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_view": f"{os.getenv('DATABASE')}.{os.getenv('SCHEMA')}.{os.getenv('SEMANTIC_VIEW')}",
    }
    resp = requests.post(
        url=f"https://{os.getenv('HOST')}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}
    else:
        raise Exception(
            f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
        )


def display_content(content: List[Dict[str, str]], role: str) -> None:
    """Displays content within appropriate chat message container."""
    with st.chat_message(role):
        for item in content:
            if item["type"] == "text":
                st.markdown(item["text"])
            elif item["type"] == "sql":
                # with st.expander("📊 View SQL Query", expanded=False):
                #     st.code(item["statement"], language="sql")

                with st.spinner("Running SQL..."):
                    df = pd.read_sql(item["statement"], st.session_state.CONN)

                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(
                            ["📋 Data", "📈 Line Chart", "📊 Bar Chart"]
                        )
                        with data_tab:
                            st.dataframe(df, width='stretch')
                        if len(df.columns) > 1:
                            df_chart = df.set_index(df.columns[0])
                            with line_tab:
                                st.line_chart(df_chart)
                            with bar_tab:
                                st.bar_chart(df_chart)
                    else:
                        st.dataframe(df, width='stretch')


def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    # Add user message to session state
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )

    # Get assistant response
    with st.spinner("🤔 Thinking..."):
        response = send_message(prompt=prompt)
        content = response["message"]["content"]

    # Add assistant message to session state
    st.session_state.messages.append(
        {"role": "assistant", "content": content}
    )


def cortext_analyst():
    # Page configuration
    st.set_page_config(
        page_title="Cortex Analyst Chat",
        page_icon="💬",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Load custom CSS
    load_css()

    # Establish connection
    try:
        if "CONN" not in st.session_state:
            st.session_state.CONN = init_connection()
    except Exception as e:
        st.error(f"Failed to initialize connection: {e}")
        st.stop()

    # Title
    st.title("💬 Cortex Agent")

    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Predefined suggestions
    SUGGESTIONS = [
        "Which store has the highest average sales price?",
        "What are the top 10 products by sales in the Furniture category?",
        "Which brand has the highest sales during October and November of 2024?",
        "Which product categories have the highest sales growth over the last 6 months?",
    ]

    # Clear chat button at the top
    col1, col2, col3 = st.columns([6, 1, 1])
    with col3:
        if st.button("🗑️ Clear Chat", width='stretch'):
            st.session_state.messages = []
            st.rerun()

    # Show suggestions only if no messages exist
    if len(st.session_state.messages) == 0:
        st.markdown("### 💡 Try asking:")
        cols = st.columns(2)
        for idx, suggestion in enumerate(SUGGESTIONS):
            with cols[idx % 2]:
                if st.button(suggestion, key=f"suggestion_{idx}", width='stretch'):
                    process_message(prompt=suggestion)
                    st.rerun()

        st.markdown("---")

    # Display all chat history from session state
    for message in st.session_state.messages:
        display_content(content=message["content"], role=message["role"])

    # Chat input at the bottom (always visible)
    if user_input := st.chat_input("💭 Ask me anything about your data..."):
        process_message(prompt=user_input)
        st.rerun()

    # AI disclaimer
    st.markdown(
        "<p style='text-align: center; font-size: 11px; color: #888; margin-top: 10px;'>"
        "⚠️ AI-generated responses. Please verify important information.</p>",
        unsafe_allow_html=True,
    )
