"""
Streamlit page: Geographic Analysis Report
Displays listening activity by artist geographic origins with regional patterns and genre data.
"""

import logging
from datetime import datetime, timedelta

import polars as pl
import streamlit as st
from streamlit.logger import get_logger

# Configuration
from config import PAGE_ICON, LAYOUT

# Import utils
from utils.db_connection import (
    get_geographic_data,
    get_continents,
    get_countries_for_continents,
    get_track_count_by_geography,
    get_genre_distribution,
    get_artists_by_geography,
)

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Page config
st.set_page_config(
    page_title="Geographic Analysis",
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
if "geo_last_refresh" not in st.session_state:
    st.session_state.geo_last_refresh = None
if "geo_data" not in st.session_state:
    st.session_state.geo_data = None
if "selected_continents" not in st.session_state:
    st.session_state.selected_continents = []
if "selected_countries" not in st.session_state:
    st.session_state.selected_countries = []
if "selected_genres" not in st.session_state:
    st.session_state.selected_genres = []
if "start_date" not in st.session_state:
    today = datetime.now().date()
    st.session_state.start_date = datetime(today.year, 1, 1).date()
if "end_date" not in st.session_state:
    st.session_state.end_date = datetime.now().date()
if "aggregation_level" not in st.session_state:
    st.session_state.aggregation_level = "Continent"
if "projection_type" not in st.session_state:
    st.session_state.projection_type = "Equirectangular"


def format_timestamp(dt) -> str:
    """Format datetime object to readable string."""
    if dt is None:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def refresh_data(start_date, end_date):
    """Refresh the data from DuckDB."""
    with st.spinner("Loading geographic data..."):
        try:
            data = get_geographic_data(start_date, end_date)
            if data is None or len(data) == 0:
                st.warning("⚠️ No geographic data found for selected date range")
                return

            st.session_state.geo_data = data
            st.session_state.geo_last_refresh = datetime.now()
        except Exception as e:
            st.error(f"❌ Failed to load data: {e}")
            logger.error(f"Data refresh failed: {e}", exc_info=True)


def display_track_count_card(start_date, end_date, continents, countries, genres):
    """Display KPI card with total track count."""
    try:
        track_count = get_track_count_by_geography(
            start_date,
            end_date,
            continents if continents else None,
            countries if countries else None,
            genres if genres else None,
        )
        st.metric(
            label="🎵 Total Tracks Played",
            value=f"{track_count:,}",
            help="Total tracks played in selected period and geography",
        )
    except Exception as e:
        logger.error(f"Failed to display track count card: {e}")
        st.error("Failed to load track count")


def display_continent_slicer():
    """Display multi-select slicer for continents."""
    try:
        continents = get_continents()
        st.multiselect(
            "Select Continents",
            options=continents,
            key="selected_continents",
        )
        return st.session_state.selected_continents
    except Exception as e:
        logger.error(f"Failed to display continent slicer: {e}")
        st.error("Failed to load continents")
        return []


def display_country_slicer(continents):
    """Display multi-select slicer for countries filtered by continents."""
    try:
        countries = get_countries_for_continents(continents) if continents else []
        # Filter out any previously selected countries that are no longer available
        if st.session_state.selected_countries:
            st.session_state.selected_countries = [
                c for c in st.session_state.selected_countries if c in countries
            ]
        st.multiselect(
            "Select Countries",
            options=countries,
            key="selected_countries",
        )
        return st.session_state.selected_countries
    except Exception as e:
        logger.error(f"Failed to display country slicer: {e}")
        st.error("Failed to load countries")
        return []


def display_geographic_map(data, continents, countries, genres):
    """Display bubble map of geographic distribution."""
    if data is None or len(data) == 0:
        st.info("No data available for map")
        return

    try:
        import plotly.graph_objects as go

        # Read aggregation level and projection from session state
        aggregation_level = st.session_state.aggregation_level
        projection = st.session_state.projection_type

        # Filter data if needed
        filtered_data = data
        if continents:
            filtered_data = filtered_data.filter(pl.col("continent").is_in(continents))
        if countries:
            filtered_data = filtered_data.filter(pl.col("country").is_in(countries))
        if genres:
            filtered_data = filtered_data.filter(pl.col("primary_genre").is_in(genres))

        if len(filtered_data) == 0:
            st.info("No geographic data for selected filters")
            return

        # Aggregate by location based on selected level
        if aggregation_level == "City":
            group_cols = ["lat", "longitude", "country", "state_province", "city"]
            hover_text_template = (
                lambda row: f"<b>{row['city']}, {row['state_province']}</b><br>Country: {row['country']}<br>Tracks: {row['total_tracks']}"
            )
            agg_data = (
                filtered_data.group_by(group_cols)
                .agg(pl.col("track_count").sum().alias("total_tracks"))
                .sort("total_tracks", descending=True)
            )
        elif aggregation_level == "State/Province":
            group_cols = ["country", "state_province"]
            hover_text_template = (
                lambda row: f"<b>{row['state_province']}, {row['country']}</b><br>Tracks: {row['total_tracks']}"
            )
            agg_data = (
                filtered_data.group_by(group_cols)
                .agg(
                    pl.col("track_count").sum().alias("total_tracks"),
                    pl.col("lat").mean().alias("lat"),
                    pl.col("longitude").mean().alias("longitude"),
                )
                .sort("total_tracks", descending=True)
            )
        elif aggregation_level == "Country":
            group_cols = ["country"]
            hover_text_template = (
                lambda row: f"<b>{row['country']}</b><br>Tracks: {row['total_tracks']}"
            )
            agg_data = (
                filtered_data.group_by(group_cols)
                .agg(
                    pl.col("track_count").sum().alias("total_tracks"),
                    pl.col("lat").mean().alias("lat"),
                    pl.col("longitude").mean().alias("longitude"),
                )
                .sort("total_tracks", descending=True)
            )
        else:  # Continent
            group_cols = ["continent"]
            hover_text_template = (
                lambda row: f"<b>{row['continent']}</b><br>Tracks: {row['total_tracks']}"
            )
            agg_data = (
                filtered_data.group_by(group_cols)
                .agg(
                    pl.col("track_count").sum().alias("total_tracks"),
                    pl.col("lat").mean().alias("lat"),
                    pl.col("longitude").mean().alias("longitude"),
                )
                .sort("total_tracks", descending=True)
            )

        fig = go.Figure()

        # Add bubble markers
        fig.add_trace(
            go.Scattergeo(
                lon=agg_data["longitude"],
                lat=agg_data["lat"],
                mode="markers",
                marker=dict(
                    size=agg_data["total_tracks"] / agg_data["total_tracks"].max() * 30
                    + 5,
                    color=agg_data["total_tracks"],
                    colorscale="Viridis",
                    showscale=False,
                    line=dict(width=1, color="white"),
                ),
                text=[hover_text_template(row) for row in agg_data.to_dicts()],
                hovertemplate="%{text}<extra></extra>",
            )
        )

        # Map projection type to Plotly values
        projection_map = {
            "Equirectangular": "equirectangular",
            "Natural Earth": "natural earth",
            "Mercator": "mercator",
            "Orthographic": "orthographic",
        }

        fig.update_layout(
            title="Geographic Distribution of Artists",
            geo=dict(
                projection_type=projection_map[projection],
                showland=True,
                landcolor="rgb(243, 243, 243)",
                coastlinecolor="rgb(204, 204, 204)",
            ),
            height=600,
            margin=dict(l=0, r=0, t=30, b=0),
            hovermode="closest",
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Failed to display geographic map: {e}")
        st.error("Failed to display map")


def display_genre_treemap(start_date, end_date, continents, countries, genres):
    """Display treemap of genre distribution."""
    try:
        genre_data = get_genre_distribution(
            start_date,
            end_date,
            continents if continents else None,
            countries if countries else None,
        )

        # Filter by selected genres if provided
        if genre_data is not None and len(genre_data) > 0 and genres:
            genre_data = genre_data.filter(pl.col("primary_genre").is_in(genres))

        if genre_data is None or len(genre_data) == 0:
            st.info("No genre data available")
            return

        import plotly.graph_objects as go

        fig = go.Figure(
            go.Treemap(
                labels=genre_data["primary_genre"],
                parents=[""] * len(genre_data),
                values=genre_data["track_count"],
                marker=dict(
                    colorscale="Viridis",
                    cmid=0,
                    colorbar=dict(title="Track Count"),
                ),
                text=[f"{c} tracks" for c in genre_data["track_count"]],
                hovertemplate="<b>%{label}</b><br>Tracks: %{value}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Top Genres by Track Count",
            height=500,
            margin=dict(l=10, r=10, t=30, b=10),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Failed to display genre treemap: {e}")
        st.error("Failed to display genre treemap")


def update_genres(genres):
    """Callback to update selected genres in session state."""
    st.session_state.selected_genres = genres


def display_artists_chart(start_date, end_date, continents, countries, genres):
    """Display horizontal bar chart of artists by track count."""
    try:
        artist_data = get_artists_by_geography(
            start_date,
            end_date,
            continents if continents else None,
            countries if countries else None,
            genres if genres else None,
        )

        if artist_data is None or len(artist_data) == 0:
            st.info("No artist data available")
            return

        import plotly.graph_objects as go

        fig = go.Figure(
            data=[
                go.Bar(
                    x=artist_data["track_count"],
                    y=artist_data["artist_name"],
                    orientation="h",
                    marker=dict(
                        color=artist_data["track_count"],
                        colorscale="Viridis",
                        showscale=True,
                        colorbar=dict(
                            title=dict(
                                text="Tracks", font=dict(color="black", size=12)
                            ),
                            tickfont=dict(color="black", size=11),
                        ),
                    ),
                    text=[f"{c:,.0f} tracks" for c in artist_data["track_count"]],
                    textposition="outside",
                    textfont=dict(color="black", size=12),
                    hovertemplate="<b>%{y}</b><br>%{x:,.0f} tracks<extra></extra>",
                )
            ]
        )

        fig.update_layout(
            title="Top 25 Artists by Track Count",
            xaxis_title="Track Count",
            yaxis_title="",
            height=500,
            margin=dict(l=200, r=50, t=30, b=50),
            hovermode="closest",
            plot_bgcolor="rgba(240, 240, 240, 0.5)",
            paper_bgcolor="white",
            font=dict(size=12),
        )

        fig.update_yaxes(autorange="reversed", tickfont=dict(color="black", size=11))
        fig.update_xaxes(tickfont=dict(color="black", size=11))

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Failed to display artists chart: {e}")
        st.error("Failed to display artists chart")


def main():
    """Main app logic."""
    st.title("Geographic Analysis Report")

    # Sidebar controls
    with st.sidebar:
        st.markdown("### Controls")

        if st.button("🔄 Refresh Data", use_container_width=True):
            refresh_data(st.session_state.start_date, st.session_state.end_date)

        st.markdown("---")
        if st.session_state.geo_last_refresh:
            st.caption(
                f"Last updated: {format_timestamp(st.session_state.geo_last_refresh)}"
            )
        else:
            st.caption("No data loaded yet")

    # Initial data load if not already loaded
    if st.session_state.geo_data is None:
        refresh_data(st.session_state.start_date, st.session_state.end_date)

    # Compact filters section at the top
    with st.expander("🔍 Filters", expanded=True):
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

        with filter_col1:
            st.date_input("From", key="start_date")

        with filter_col2:
            st.date_input("To", key="end_date")

        with filter_col3:
            continents = display_continent_slicer()

        with filter_col4:
            countries = display_country_slicer(continents)

        # Second row for genre filter and track count
        filter_col5, filter_col6 = st.columns([3, 1])

        with filter_col5:
            try:
                genre_data = get_genre_distribution(
                    st.session_state.start_date,
                    st.session_state.end_date,
                    continents if continents else None,
                    countries if countries else None,
                )
                if genre_data is not None and len(genre_data) > 0:
                    available_genres = genre_data["primary_genre"].to_list()
                    # Filter out any previously selected genres that are no longer available
                    if st.session_state.selected_genres:
                        st.session_state.selected_genres = [
                            g
                            for g in st.session_state.selected_genres
                            if g in available_genres
                        ]
                    st.multiselect(
                        "Select Genres",
                        options=available_genres,
                        key="selected_genres",
                    )
                else:
                    st.info("No genres available for selected filters")
            except Exception as e:
                logger.error(f"Failed to load genre filter: {e}")
                st.error(f"Error loading genres: {str(e)}")

        with filter_col6:
            display_track_count_card(
                st.session_state.start_date,
                st.session_state.end_date,
                st.session_state.selected_continents,
                st.session_state.selected_countries,
                st.session_state.selected_genres,
            )

    st.markdown("---")

    # Visualizations section
    if st.session_state.geo_data is not None:
        # Map controls section (before columns to enable proper reruns)
        st.markdown("### Geographic Distribution")
        map_opt_col1, map_opt_col2 = st.columns(2)
        with map_opt_col1:
            st.selectbox(
                "Aggregation Level",
                options=["City", "State/Province", "Country", "Continent"],
                key="aggregation_level",
                help="Choose the geographic granularity level for data aggregation",
            )
        with map_opt_col2:
            st.selectbox(
                "Map Projection",
                options=[
                    "Equirectangular",
                    "Natural Earth",
                    "Mercator",
                    "Orthographic",
                ],
                key="projection_type",
                help="Choose the map projection style",
            )

        # Top row: Map on left, Genre Treemap on right
        col_map, col_genre = st.columns([2, 1])

        with col_map:
            display_geographic_map(
                st.session_state.geo_data,
                continents,
                countries,
                st.session_state.selected_genres,
            )

        with col_genre:
            st.markdown("### Genre Distribution")
            display_genre_treemap(
                st.session_state.start_date,
                st.session_state.end_date,
                continents,
                countries,
                st.session_state.selected_genres,
            )

        # Bottom row: Artists chart
        st.markdown("---")
        st.markdown("### Artists by Track Count")
        display_artists_chart(
            st.session_state.start_date,
            st.session_state.end_date,
            continents,
            countries,
            st.session_state.selected_genres,
        )


if __name__ == "__main__":
    main()
