"""
Streamlit app: Home page with navigation to reports
"""

import streamlit as st

# Page config
st.set_page_config(
    page_title="Home | Music Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] {
        font-size: 2.5rem;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.9rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title and description
st.title("Music Tracker")
st.markdown(
    """
    Welcome to your personal music analytics dashboard. 
    
    Select a report from the sidebar to get started:
    """
)

# Info about available reports
col1, col2 = st.columns([1, 1])

with col1:
    st.info(
        """
        **📊 Artists Last 24 Hours**
        
        View your top artists and listening activity from the past 24 hours.
        Includes KPIs, top artists chart, and detailed track information.
        """
    )

with col2:
    st.info(
        """
        **🔮 Coming Soon**
        
        Additional reports and analytics features will be added here soon.
        """
    )

st.markdown("---")
st.markdown(
    """
    ### 🚀 How to Use
    
    1. Navigate to any report using the sidebar menu
    2. Click refresh to fetch the latest data
    3. Interact with charts and explore details
    4. Use the sidebar controls to customize your view
    """
)
