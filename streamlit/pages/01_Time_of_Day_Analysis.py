"""
Streamlit page: Time of Day Analysis Report
Displays listening patterns by hour of day and time periods with historical trends.
"""

import logging
from datetime import datetime, timedelta

import plotly.graph_objects as go
import streamlit as st
from streamlit.logger import get_logger

# Configuration
from config import PAGE_ICON, LAYOUT

# Import utils
from utils.db_connection import (
    get_countries,
    get_tracks_by_year,
    get_tracks_by_hour,
    get_tracks_by_time_of_day,
)

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Page config
st.set_page_config(
    page_title="Time of Day Analysis",
    page_icon=PAGE_ICON,
    layout=LAYOUT,
    initial_sidebar_state="collapsed",
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
    /* Reduce top margin/padding */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
    }
    h1 {
        padding-top: 0rem;
        margin-top: 0rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Initialize session state
if "time_last_refresh" not in st.session_state:
    st.session_state.time_last_refresh = None
if "start_date" not in st.session_state:
    # Default to 2020-08-24 to match report spec
    st.session_state.start_date = datetime(2020, 8, 24).date()
if "end_date" not in st.session_state:
    # Default to current date
    st.session_state.end_date = datetime.now().date()
if "selected_country" not in st.session_state:
    st.session_state.selected_country = None


def format_timestamp(dt) -> str:
    """Format datetime object to readable string."""
    if dt is None:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def display_date_range_slicer():
    """Display date range slicer for filtering."""
    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input(
            "Start Date",
            value=st.session_state.start_date,
            key="date_start_input",
        )
        st.session_state.start_date = start

    with col2:
        end = st.date_input(
            "End Date",
            value=st.session_state.end_date,
            key="date_end_input",
        )
        st.session_state.end_date = end

    return start, end


def display_country_slicer():
    """Display country code slicer for filtering."""
    try:
        countries = get_countries()
        selected = st.selectbox(
            "Select Country (optional)",
            options=[None] + countries,
            index=0,
            format_func=lambda x: "All Countries" if x is None else x,
            key="country_select",
        )
        st.session_state.selected_country = selected
        return selected
    except Exception as e:
        logger.error(f"Failed to display country slicer: {e}")
        st.error("Failed to load countries")
        return None


def display_tracks_by_year_chart(start_date, end_date, country_code):
    """Display horizontal bar chart of tracks played by year."""
    try:
        data = get_tracks_by_year(start_date, end_date, country_code)

        if data is None or len(data) == 0:
            st.info("No data available for tracks by year")
            return

        fig = go.Figure(
            go.Bar(
                x=data["track_count"],
                y=data["year_num"].cast(str),
                orientation="h",
                marker=dict(color="steelblue"),
                hovertemplate="<b>%{y}</b><br>Tracks: %{x:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Tracks Played by Year",
            xaxis_title="",
            yaxis_title="",
            height=300,
            margin=dict(l=50, r=10, t=30, b=30),
            showlegend=False,
            hovermode="closest",
            yaxis=dict(type="category"),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Failed to display tracks by year chart: {e}")
        st.error("Failed to display tracks by year chart")


def display_tracks_by_hour_chart(start_date, end_date, country_code):
    """Display vertical column chart of tracks by hour of day."""
    try:
        data = get_tracks_by_hour(start_date, end_date, country_code)

        if data is None or len(data) == 0:
            st.info("No data available for tracks by hour")
            return

        fig = go.Figure(
            go.Bar(
                x=data["hour_of_day"],
                y=data["track_count"],
                marker=dict(
                    color=data["track_count"],
                    colorscale="Viridis",
                    showscale=False,
                ),
                hovertemplate="<b>Hour %{x}:00</b><br>Tracks: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Tracks by Hour of Day",
            xaxis_title="Hour of Day",
            yaxis_title="",
            height=350,
            margin=dict(l=50, r=10, t=30, b=30),
            showlegend=False,
            hovermode="closest",
            xaxis=dict(type="category"),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Failed to display tracks by hour chart: {e}")
        st.error("Failed to display tracks by hour chart")


def display_tracks_by_time_of_day_chart(start_date, end_date, country_code):
    """Display vertical column chart of tracks by time of day."""
    try:
        data = get_tracks_by_time_of_day(start_date, end_date, country_code)

        if data is None or len(data) == 0:
            st.info("No data available for tracks by time of day")
            return

        # Define color mapping for time periods
        color_map = {
            "Morning": "#FFD700",  # Gold
            "Afternoon": "#FF8C00",  # Dark Orange
            "Evening": "#4169E1",  # Royal Blue
            "Night": "#191970",  # Midnight Blue
        }

        colors = [color_map.get(tod, "steelblue") for tod in data["time_of_day"]]

        fig = go.Figure(
            go.Bar(
                x=data["time_of_day"],
                y=data["track_count"],
                marker=dict(color=colors),
                hovertemplate="<b>%{x}</b><br>Tracks: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Tracks by Time of Day",
            xaxis_title="",
            yaxis_title="",
            height=350,
            margin=dict(l=50, r=10, t=30, b=30),
            showlegend=False,
            hovermode="closest",
            xaxis=dict(type="category"),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Failed to display tracks by time of day chart: {e}")
        st.error("Failed to display tracks by time of day chart")


def main():
    """Main app logic."""
    st.title("Time of Day Analysis Report")

    # Sidebar info
    with st.sidebar:
        st.markdown("### Last Updated")
        st.markdown(f"`{format_timestamp(st.session_state.time_last_refresh)}`")
        st.markdown("---")
        st.markdown("**Filters** available in the main panel")

    # Compact filters section at the top
    with st.expander("🔍 Filters", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Date Range**")
            start_date, end_date = display_date_range_slicer()

        with col2:
            st.markdown("**Geography**")
            country_code = display_country_slicer()

        # Update refresh timestamp
        st.session_state.time_last_refresh = datetime.now()

    st.markdown("---")

    # Visualizations section
    col1, col2 = st.columns([1, 2])

    with col1:
        st.markdown("### Year Distribution")
        display_tracks_by_year_chart(start_date, end_date, country_code)

    with col2:
        st.markdown("### Hourly Breakdown")
        display_tracks_by_hour_chart(start_date, end_date, country_code)

    st.markdown("---")

    # Time of day chart - full width
    st.markdown("### Time of Day Patterns")
    display_tracks_by_time_of_day_chart(start_date, end_date, country_code)


if __name__ == "__main__":
    main()
