"""
Streamlit page: Artist Genre Analysis Report
Displays listening patterns through the lens of musical genres, showing genre distribution,
top artists within genres, and historical trends.
"""

import logging
from datetime import datetime

import plotly.graph_objects as go
import streamlit as st
from streamlit.logger import get_logger

# Configuration
from config import PAGE_ICON, LAYOUT

# Import utils
from utils.db_connection import (
    get_genres,
    get_tracks_by_year_and_genre,
    get_genre_distribution_for_analysis,
    get_artists_by_genre,
)

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Page config
st.set_page_config(
    page_title="Artist Genre Analysis",
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
if "genre_last_refresh" not in st.session_state:
    st.session_state.genre_last_refresh = None
if "genre_data" not in st.session_state:
    st.session_state.genre_data = None
if "start_date" not in st.session_state:
    # Default to current year
    today = datetime.now().date()
    st.session_state.start_date = datetime(today.year, 1, 1).date()
if "end_date" not in st.session_state:
    st.session_state.end_date = datetime.now().date()
if "selected_genres" not in st.session_state:
    st.session_state.selected_genres = []
if "selected_years" not in st.session_state:
    st.session_state.selected_years = []


def format_timestamp(dt) -> str:
    """Format datetime object to readable string."""
    if dt is None:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def refresh_data(start_date, end_date):
    """Refresh the genre data from DuckDB."""
    with st.spinner("Loading genre data..."):
        try:
            genre_data = get_genre_distribution_for_analysis(start_date, end_date)
            st.session_state.genre_data = genre_data
            st.session_state.genre_last_refresh = datetime.now()
            logger.info("Genre data refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh genre data: {e}")
            st.error("Failed to load genre data")


def display_date_range_slicer():
    """Display date range slicer for filtering."""
    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input(
            "Start Date",
            value=st.session_state.start_date,
            key="genre_date_start_input",
        )
        st.session_state.start_date = start

    with col2:
        end = st.date_input(
            "End Date",
            value=st.session_state.end_date,
            key="genre_date_end_input",
        )
        st.session_state.end_date = end

    return start, end


def display_tracks_by_year_chart(start_date, end_date, genres):
    """Display horizontal bar chart of tracks played by year for selected genres."""
    try:
        data = get_tracks_by_year_and_genre(
            start_date, end_date, genres if genres else None
        )

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
            title="Count Tracks Played",
            xaxis_title="",
            yaxis_title="",
            height=300,
            margin=dict(l=50, r=10, t=30, b=30),
            showlegend=False,
            hovermode="closest",
            yaxis=dict(type="category"),
        )

        st.plotly_chart(fig, width="stretch")

    except Exception as e:
        logger.error(f"Failed to display tracks by year chart: {e}")
        st.error("Failed to display tracks by year chart")


def display_genre_treemap(start_date, end_date, genres):
    """Display treemap of genre distribution."""
    try:
        logger.debug(
            f"Fetching genre distribution for {start_date} to {end_date}, genres: {genres}"
        )
        data = get_genre_distribution_for_analysis(
            start_date, end_date, genres if genres else None
        )
        logger.info(f"Genre data fetched - Type: {type(data)}, Is None: {data is None}")

        if data is None:
            logger.warning("Genre distribution data is None")
            st.warning("⚠️ No data returned from query")
            return None

        logger.info(f"Data length: {len(data)}, Data is empty: {len(data) == 0}")
        if len(data) == 0:
            logger.warning("Genre distribution data is empty")
            st.info("ℹ️ No genres found for the selected date range")
            return None

        logger.info(f"Genre data shape: {data.shape}, columns: {data.columns}")

        # Create hover text with artist information
        genre_list = data["genre"].to_list()
        track_count_list = data["track_count"].to_list()

        logger.info(f"Genres: {genre_list[:3] if len(genre_list) > 3 else genre_list}")
        logger.info(
            f"Track counts: {track_count_list[:3] if len(track_count_list) > 3 else track_count_list}"
        )

        hover_text = []
        for row in data.iter_rows(named=True):
            tooltip = f"<b>{row['genre']}</b><br>"
            tooltip += f"Tracks: {row['track_count']:,}<br>"
            if row.get("top_artist") is not None:
                tooltip += f"Top Artist: {row['top_artist']}<br>"
            if row.get("most_popular_artist") is not None:
                tooltip += f"Most Popular: {row['most_popular_artist']}"
            hover_text.append(tooltip)

        logger.info(f"Created {len(hover_text)} hover texts")

        fig = go.Figure(
            go.Treemap(
                labels=genre_list,
                parents=[""] * len(genre_list),  # Required for flat treemap structure
                values=track_count_list,
                text=[f"{int(c)} tracks" for c in track_count_list],
                hovertemplate="<b>%{label}</b><br>Tracks: %{value}<extra></extra>",
                marker=dict(
                    colorscale="Viridis",
                    cmid=float(data["track_count"].median()),
                    colorbar=dict(title="Track Count"),
                ),
            )
        )

        fig.update_layout(
            title="Genre Distribution",
            height=350,
            margin=dict(l=10, r=10, t=30, b=10),
        )

        logger.info("About to render treemap")
        st.plotly_chart(fig, width="stretch", key="genre_treemap")
        logger.info("Genre treemap displayed successfully")
        return data

    except Exception as e:
        logger.error(f"Failed to display genre treemap: {e}", exc_info=True)
        st.error(f"❌ Failed to display genre treemap: {str(e)}")
        return None


def display_artists_chart(start_date, end_date, genres, years):
    """Display horizontal bar chart of artists by track count in selected genres."""
    try:
        data = get_artists_by_genre(
            start_date,
            end_date,
            genres if genres else None,
            years if years else None,
        )

        if data is None or len(data) == 0:
            st.info("No data available for artists in selected genres")
            return

        fig = go.Figure(
            go.Bar(
                x=data["track_count"],
                y=data["artist_name"],
                orientation="h",
                marker=dict(color="coral"),
                hovertemplate="<b>%{y}</b><br>Tracks: %{x:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Artists in Selected Genres",
            xaxis_title="",
            yaxis_title="",
            height=500,
            margin=dict(l=200, r=10, t=30, b=30),
            showlegend=False,
            hovermode="closest",
            yaxis=dict(autorange="reversed"),
        )

        st.plotly_chart(fig, width="stretch")

    except Exception as e:
        logger.error(f"Failed to display artists chart: {e}")
        st.error("Failed to display artists chart")


def update_genres(selected):
    """Callback to update selected genres in session state."""
    st.session_state.selected_genres = selected


def update_years(selected):
    """Callback to update selected years in session state."""
    st.session_state.selected_years = selected


def main():
    """Main app logic."""
    st.title("Artist Genre Analysis")

    # Sidebar info
    with st.sidebar:
        st.markdown("### Last Updated")
        st.markdown(f"`{format_timestamp(st.session_state.genre_last_refresh)}`")

    # Compact filters section at the top
    with st.expander("🔍 Filters", expanded=True):
        filter_col1, filter_col2 = st.columns(2)

        with filter_col1:
            st.markdown("**Date Range**")
            start_date, end_date = display_date_range_slicer()

        with filter_col2:
            st.markdown("**Genre Filter**")
            try:
                available_genres = get_genres()
                selected_genres = st.multiselect(
                    "Select Genres (optional)",
                    options=available_genres,
                    key="genre_multiselect",
                    on_change=update_genres,
                    args=(st.session_state.get("genre_multiselect", []),),
                )
                st.session_state.selected_genres = selected_genres
            except Exception as e:
                logger.error(f"Failed to load genres: {e}")
                st.error("Failed to load genres")

        # Refresh button
        if st.button("🔄 Refresh Data", width="stretch"):
            refresh_data(start_date, end_date)

    st.markdown("---")

    # Initial data load if not already loaded
    if st.session_state.genre_data is None:
        refresh_data(start_date, end_date)

    # Visualizations section
    if st.session_state.genre_data is not None:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("### Year Distribution")
            display_tracks_by_year_chart(
                start_date, end_date, st.session_state.selected_genres
            )

        with col2:
            st.markdown("### Genre Distribution")
            display_genre_treemap(
                start_date, end_date, st.session_state.selected_genres
            )

        st.markdown("---")

        # Get available years from the data for year filter
        if st.session_state.genre_data is not None:
            available_years = (
                get_tracks_by_year_and_genre(
                    start_date, end_date, st.session_state.selected_genres
                )
                .select("year_num")
                .unique()
                .sort("year_num")
            )
            if len(available_years) > 0:
                year_list = available_years["year_num"].to_list()
                selected_years = st.multiselect(
                    "Filter by Year (optional)",
                    options=sorted(year_list),
                    key="year_multiselect",
                    on_change=update_years,
                    args=(st.session_state.get("year_multiselect", []),),
                )
                st.session_state.selected_years = selected_years

        st.markdown("### Artists in Selected Genres")
        display_artists_chart(
            start_date,
            end_date,
            st.session_state.selected_genres,
            st.session_state.selected_years,
        )


if __name__ == "__main__":
    main()
