"""
Streamlit page: Artists Last 24 Hours Report
Displays listening activity metrics and top artists for the last 24-hour period.
"""

import logging
from datetime import datetime

import polars as pl
import streamlit as st
from streamlit.logger import get_logger

# Configuration
from config import (
    PAGE_ICON,
    LAYOUT,
    TOP_ARTISTS_LIMIT,
)

# Import utils
from utils.db_connection import (
    get_last_24h_tracks,
    get_artist_aggregates,
    get_tracks_for_artist,
)

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Page config
st.set_page_config(
    page_title="Artists Last 24 Hours",
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
    </style>
    """,
    unsafe_allow_html=True,
)

# Initialize session state
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = None
if "tracks_data" not in st.session_state:
    st.session_state.tracks_data = None
if "selected_artist" not in st.session_state:
    st.session_state.selected_artist = None


def format_timestamp(dt) -> str:
    """Format datetime object to readable string."""
    if dt is None:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def refresh_data():
    """Refresh the data from DuckDB."""
    with st.spinner("Loading data..."):
        try:
            data = get_last_24h_tracks()
            if data is None or len(data) == 0:
                st.warning("⚠️ No tracks found in the last 24 hours")
                return

            st.session_state.tracks_data = data
            st.session_state.last_refresh = datetime.now()
            st.success("✅ Data refreshed successfully")
        except Exception as e:
            st.error(f"❌ Failed to load data: {e}")
            logger.error(f"Data refresh failed: {e}", exc_info=True)


def display_kpi_cards(data):
    """Display KPI cards with key metrics."""
    if data is None or len(data) == 0:
        st.warning("No data available for the last 24 hours")
        return

    # Calculate metrics
    total_minutes = data["minutes_played"].sum()
    total_tracks = len(data)
    total_artists = data["artist"].n_unique()

    # Display metrics in columns
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="📊 Total Minutes",
            value=f"{total_minutes:,.0f}",
            help="Total listening time in the last 24 hours",
        )

    with col2:
        st.metric(
            label="🎵 Total Tracks",
            value=f"{total_tracks:,}",
            help="Number of unique tracks played",
        )

    with col3:
        st.metric(
            label="👤 Total Artists",
            value=f"{total_artists:,}",
            help="Number of unique artists",
        )


def display_artists_chart(data):
    """Display horizontal bar chart of artists by minutes played."""
    if data is None or len(data) == 0:
        st.info("No artist data available")
        return

    # Aggregate by artist
    artist_data = get_artist_aggregates(data)

    if len(artist_data) == 0:
        st.info("No artist data available")
        return

    # Get top N artists
    top_artists = artist_data.head(TOP_ARTISTS_LIMIT)

    # Create interactive chart with Plotly
    import plotly.graph_objects as go

    fig = go.Figure(
        data=[
            go.Bar(
                x=top_artists["total_minutes"],
                y=top_artists["artist"],
                orientation="h",
                marker=dict(
                    color=top_artists["total_minutes"],
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(
                        title=dict(text="Minutes", font=dict(color="black", size=12)),
                        tickfont=dict(color="black", size=11),
                    ),
                ),
                text=[f"{m:.0f} min" for m in top_artists["total_minutes"]],
                textposition="outside",
                textfont=dict(color="black", size=12),
                hovertemplate="<b>%{y}</b><br>%{x:.0f} minutes<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        title="Top Artists by Listening Time (Last 24 Hours)",
        xaxis_title="Minutes Played",
        yaxis_title="",
        height=500,
        margin=dict(l=200, r=50, t=50, b=50),
        hovermode="closest",
        plot_bgcolor="rgba(240, 240, 240, 0.5)",
        paper_bgcolor="white",
        font=dict(size=12),
    )

    # Make y-axis readable with dark text
    fig.update_yaxes(autorange="reversed", tickfont=dict(color="black", size=11))
    fig.update_xaxes(tickfont=dict(color="black", size=11))

    st.plotly_chart(fig, use_container_width=True)

    # Artist details modal
    st.markdown("---")
    st.subheader("Artist Details")

    col1, col2 = st.columns([3, 1])
    with col1:
        selected_artist = st.selectbox(
            "Select an artist to view their tracks",
            options=top_artists["artist"].to_list(),
            key="artist_selector",
        )

    if selected_artist:
        artist_tracks = get_tracks_for_artist(data, selected_artist)

        if len(artist_tracks) > 0:
            # Display artist info
            artist_stats = artist_data.filter(
                pl.col("artist") == selected_artist
            ).row(0, named=True)
            total_min, track_count = (
                artist_stats["total_minutes"],
                artist_stats["track_count"],
            )

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Minutes", f"{total_min:.0f}")
            with col2:
                st.metric("Track Count", f"{track_count}")
            with col3:
                avg_min = total_min / track_count if track_count > 0 else 0
                st.metric("Avg Minutes/Track", f"{avg_min:.1f}")

            # Display tracks in a table
            st.markdown(f"#### Tracks by {selected_artist}")

            # Format for display
            display_df = artist_tracks[
                ["track_name", "album", "minutes_played", "played_at", "popularity"]
            ].rename(
                {
                    "track_name": "Track",
                    "album": "Album",
                    "minutes_played": "Minutes",
                    "played_at": "Played At",
                    "popularity": "Popularity",
                }
            ).with_columns(
                pl.col("Minutes").round(2)
            )

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
            )


def main():
    """Main app logic."""
    st.title("Artists Last 24 Hours Report")
    # Sidebar with refresh button
    with st.sidebar:
        st.markdown("### Controls")
        if st.button("🔄 Refresh Data", use_container_width=True):
            refresh_data()

        st.markdown("---")
        if st.session_state.last_refresh:
            st.caption(
                f"Last updated: {format_timestamp(st.session_state.last_refresh)}"
            )
        else:
            st.caption("No data loaded yet")

    # Initial data load if not already loaded
    if st.session_state.tracks_data is None:
        refresh_data()

    # Display KPIs
    if st.session_state.tracks_data is not None:
        display_kpi_cards(st.session_state.tracks_data)

        st.markdown("---")

        # Display chart and artist details
        display_artists_chart(st.session_state.tracks_data)


if __name__ == "__main__":
    main()
