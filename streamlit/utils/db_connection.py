"""Database connection and query utilities."""

import logging
import os
from contextlib import contextmanager

import duckdb
import polars as pl
import streamlit as st

logger = logging.getLogger(__name__)

# Get DuckDB path from environment variable or default
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "music_tracker.duckdb"),
)


@contextmanager
def get_duckdb_connection():
    """
    Context manager for DuckDB connections.

    Creates a fresh connection for each use and ensures it's properly closed,
    preventing file locks when running with Streamlit.

    Usage:
        with get_duckdb_connection() as conn:
            result = conn.execute(query).pl()
    """
    try:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        logger.info(f"Connected to DuckDB: {DUCKDB_PATH}")
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise
    finally:
        try:
            conn.close()
            logger.debug("DuckDB connection closed")
        except Exception as e:
            logger.warning(f"Error closing DuckDB connection: {e}")


def get_last_24h_tracks():
    """
    Fetch all tracks played in the last 24 hours from main_dw.recently_played view.

    Returns:
        DataFrame with columns: user_id, track_id, track_name, artist,
                                       artist_id, album, duration_ms, minutes_played,
                                       played_at, popularity, play_source
        None if query fails
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    user_id,
                    track_id,
                    track_name,
                    artist,
                    artist_id,
                    album,
                    duration_ms,
                    minutes_played,
                    played_at,
                    popularity,
                    play_source
                FROM main_dw.recently_played
                WHERE played_at >= NOW() - INTERVAL 24 HOURS
                ORDER BY played_at DESC
            """

            result = conn.execute(query).pl()
            logger.info(f"Fetched {len(result)} tracks from last 24 hours")
            return result

    except Exception as e:
        logger.error(f"Failed to fetch last 24h tracks: {e}")
        return None


def get_artist_aggregates(df):
    """
    Aggregate track data by artist, calculating total minutes and track count.

    Args:
        df: Polars DataFrame from get_last_24h_tracks()

    Returns:
        Polars DataFrame with columns: artist, artist_id, total_minutes, track_count
        Sorted by total_minutes descending
    """
    if df is None or len(df) == 0:
        return pl.DataFrame()

    try:
        result = (
            df.group_by(["artist"])
            .agg(
                pl.col("minutes_played").sum().alias("total_minutes"),
                pl.col("artist").count().alias("track_count"),
            )
            .sort("total_minutes", descending=True)
        )
        return result

    except Exception as e:
        logger.error(f"Failed to aggregate artist data: {e}")
        return pl.DataFrame()


def get_tracks_for_artist(df, artist: str):
    """
    Get all tracks for a specific artist from the last 24h data.

    Args:
        df: Polars DataFrame from get_last_24h_tracks()
        artist: Artist name to filter

    Returns:
        Polars DataFrame with columns: track_name, album, minutes_played,
                                       played_at, popularity
        Sorted by played_at descending
    """
    if df is None or len(df) == 0:
        return pl.DataFrame()

    try:
        result = (
            df.filter(pl.col("artist") == artist)
            .select(
                ["track_name", "album", "minutes_played", "played_at", "popularity"]
            )
            .sort("played_at", descending=True)
        )
        return result

    except Exception as e:
        logger.error(f"Failed to get tracks for artist {artist}: {e}")
        return pl.DataFrame()


def get_geographic_data(start_date, end_date):
    """
    Fetch tracks played within a date range with geographic and artist data.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)

    Returns:
        DataFrame with columns: artist_name, continent, country, state_province, city,
                                lat, longitude, primary_genre, track_count
        None if query fails
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    da.artist_name,
                    da.continent,
                    da.country,
                    da.state_province,
                    da.city,
                    da.lat,
                    da.longitude,
                    da.primary_genre,
                    COUNT(ftp.track_sid) as track_count
                FROM main_dw.fact_track_played ftp
                JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND da.country IS NOT NULL
                GROUP BY da.artist_sid, da.artist_name, da.continent, da.country,
                         da.state_province, da.city, da.lat, da.longitude, da.primary_genre
                ORDER BY track_count DESC
            """

            result = conn.execute(query, [start_date, end_date]).pl()
            logger.info(
                f"Fetched {len(result)} artist-location records between {start_date} and {end_date}"
            )
            return result

    except Exception as e:
        logger.error(f"Failed to fetch geographic data: {e}")
        return None


def get_continents():
    """
    Get all distinct continents from the data.

    Returns:
        List of continent names, sorted alphabetically
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT DISTINCT continent
                FROM main_dw.dim_artist
                WHERE continent IS NOT NULL AND continent != ''
                ORDER BY continent
            """
            result = conn.execute(query).pl()
            return result["continent"].to_list()

    except Exception as e:
        logger.error(f"Failed to fetch continents: {e}")
        return []


def get_countries_for_continents(continents):
    """
    Get countries filtered by selected continents.

    Args:
        continents: List of continent names

    Returns:
        List of country names, sorted alphabetically
    """
    if not continents:
        return []

    try:
        with get_duckdb_connection() as conn:
            # Create placeholder string
            placeholders = ",".join(["?" for _ in continents])
            query = f"""
                SELECT DISTINCT country
                FROM main_dw.dim_artist
                WHERE continent IN ({placeholders})
                    AND country IS NOT NULL
                    AND country != ''
                ORDER BY country
            """
            result = conn.execute(query, continents).pl()
            return result["country"].to_list()

    except Exception as e:
        logger.error(f"Failed to fetch countries: {e}")
        return []


def get_track_count_by_geography(
    start_date, end_date, continents=None, countries=None, genres=None
):
    """
    Get total track count for selected geography and date range.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        continents: List of continent names (optional)
        countries: List of country names (optional)
        genres: List of genre names (optional)

    Returns:
        Integer count of tracks
    """
    try:
        with get_duckdb_connection() as conn:
            query = 'SELECT COUNT(ftp.track_sid) as track_count FROM main_dw.fact_track_played ftp JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid WHERE dd."date" >= ? AND dd."date" <= ?'
            params = [start_date, end_date]

            if continents:
                placeholders = ",".join(["?" for _ in continents])
                query += f" AND da.continent IN ({placeholders})"
                params.extend(continents)

            if countries:
                placeholders = ",".join(["?" for _ in countries])
                query += f" AND da.country IN ({placeholders})"
                params.extend(countries)

            if genres:
                placeholders = ",".join(["?" for _ in genres])
                query += f" AND da.primary_genre IN ({placeholders})"
                params.extend(genres)

            result = conn.execute(query, params).pl()
            return result["track_count"][0]

    except Exception as e:
        logger.error(f"Failed to fetch track count: {e}")
        return 0


def get_genre_distribution(start_date, end_date, continents=None, countries=None):
    """
    Get genre distribution for selected geography and date range (top 20).

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        continents: List of continent names (optional)
        countries: List of country names (optional)

    Returns:
        DataFrame with columns: primary_genre, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    da.primary_genre,
                    COUNT(ftp.track_sid) as track_count
                FROM main_dw.fact_track_played ftp
                JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND da.primary_genre IS NOT NULL
                    AND da.primary_genre != 'no genre defined'
            """
            params = [start_date, end_date]

            if continents:
                placeholders = ",".join(["?" for _ in continents])
                query += f" AND da.continent IN ({placeholders})"
                params.extend(continents)

            if countries:
                placeholders = ",".join(["?" for _ in countries])
                query += f" AND da.country IN ({placeholders})"
                params.extend(countries)

            query += " GROUP BY da.primary_genre ORDER BY track_count DESC LIMIT 20"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch genre distribution: {e}")
        return None


def get_artists_by_geography(
    start_date, end_date, continents=None, countries=None, genres=None
):
    """
    Get artists ranked by track count for selected filters (top 25).

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        continents: List of continent names (optional)
        countries: List of country names (optional)
        genres: List of genre names (optional)

    Returns:
        DataFrame with columns: artist_name, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    da.artist_name,
                    COUNT(ftp.track_sid) as track_count
                FROM main_dw.fact_track_played ftp
                JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
            """
            params = [start_date, end_date]

            if continents:
                placeholders = ",".join(["?" for _ in continents])
                query += f" AND da.continent IN ({placeholders})"
                params.extend(continents)

            if countries:
                placeholders = ",".join(["?" for _ in countries])
                query += f" AND da.country IN ({placeholders})"
                params.extend(countries)

            if genres:
                placeholders = ",".join(["?" for _ in genres])
                query += f" AND da.primary_genre IN ({placeholders})"
                params.extend(genres)

            query += " GROUP BY da.artist_sid, da.artist_name ORDER BY track_count DESC LIMIT 25"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch artists by geography: {e}")
        return None


def get_countries():
    """
    Get all distinct country codes from the data.

    Returns:
        List of country codes, sorted alphabetically
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT DISTINCT country_code
                FROM main_dw.dim_artist
                WHERE country_code IS NOT NULL AND country_code != ''
                ORDER BY country_code
            """
            result = conn.execute(query).pl()
            return result["country_code"].to_list()

    except Exception as e:
        logger.error(f"Failed to fetch countries: {e}")
        return []


def get_tracks_by_year(start_date, end_date, country_code=None):
    """
    Get track count by year for selected date range and optional country filter.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        country_code: Optional country code filter

    Returns:
        DataFrame with columns: year_num, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    dd.year_num,
                    COUNT(ftp.track_sid) as track_count
                FROM main_dw.fact_track_played ftp
                JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND da.artist_name IS NOT NULL
            """
            params = [start_date, end_date]

            if country_code:
                query += " AND da.country_code = ?"
                params.append(country_code)

            query += " GROUP BY dd.year_num ORDER BY dd.year_num ASC"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch tracks by year: {e}")
        return None


def get_tracks_by_hour(start_date, end_date, country_code=None):
    """
    Get track count by hour of day for selected date range and optional country filter.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        country_code: Optional country code filter

    Returns:
        DataFrame with columns: hour_of_day, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    dt.hour_of_day,
                    COUNT(ftp.track_sid) as track_count
                FROM main_dw.fact_track_played ftp
                JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid
                JOIN main_dw.dim_time dt ON ftp.time_sid = dt.time_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND dt.hour_of_day IS NOT NULL
                    AND da.artist_name IS NOT NULL
            """
            params = [start_date, end_date]

            if country_code:
                query += " AND da.country_code = ?"
                params.append(country_code)

            query += " GROUP BY dt.hour_of_day ORDER BY dt.hour_of_day ASC"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch tracks by hour: {e}")
        return None


def get_tracks_by_time_of_day(start_date, end_date, country_code=None):
    """
    Get track count by time of day period for selected date range and optional country filter.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        country_code: Optional country code filter

    Returns:
        DataFrame with columns: time_of_day, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    dt.time_of_day,
                    COUNT(ftp.track_sid) as track_count
                FROM main_dw.fact_track_played ftp
                JOIN main_dw.dim_artist da ON ftp.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON ftp.date_sid = dd.date_sid
                JOIN main_dw.dim_time dt ON ftp.time_sid = dt.time_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND dt.hour_of_day IS NOT NULL
                    AND da.artist_name IS NOT NULL
            """
            params = [start_date, end_date]

            if country_code:
                query += " AND da.country_code = ?"
                params.append(country_code)

            query += " GROUP BY dt.time_of_day ORDER BY CASE WHEN dt.time_of_day = 'Morning' THEN 1 WHEN dt.time_of_day = 'Afternoon' THEN 2 WHEN dt.time_of_day = 'Evening' THEN 3 WHEN dt.time_of_day = 'Night' THEN 4 ELSE 5 END"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch tracks by time of day: {e}")
        return None


def get_genres():
    """
    Get all distinct genres from the data.

    Returns:
        List of genre names, sorted alphabetically
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT DISTINCT genre
                FROM main_dw.dim_artist_genre
                WHERE genre IS NOT NULL
                    AND genre != ''
                    AND genre != 'no genre defined'
                ORDER BY genre
            """
            result = conn.execute(query).pl()
            return result["genre"].to_list()

    except Exception as e:
        logger.error(f"Failed to fetch genres: {e}")
        return []


def get_tracks_by_year_and_genre(start_date, end_date, genres=None):
    """
    Get track count by year for selected date range and optional genre filter.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        genres: Optional list of genre names to filter

    Returns:
        DataFrame with columns: year_num, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    dd.year_num,
                    COUNT(fag.track_sid) as track_count
                FROM main_dw.fact_artist_genre fag
                JOIN main_dw.dim_artist_genre dag ON fag.genre_sid = dag.genre_sid
                JOIN main_dw.dim_date dd ON fag.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND dag.genre IS NOT NULL
                    AND dag.genre != 'no genre defined'
            """
            params = [start_date, end_date]

            if genres:
                placeholders = ",".join(["?" for _ in genres])
                query += f" AND dag.genre IN ({placeholders})"
                params.extend(genres)

            query += " GROUP BY dd.year_num ORDER BY dd.year_num ASC"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch tracks by year and genre: {e}")
        return None


def get_genre_distribution_for_analysis(start_date, end_date, genres=None):
    """
    Get genre distribution for genre analysis page (top 25).

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        genres: Optional list of genre names to filter

    Returns:
        DataFrame with columns: genre, track_count, top_artist, most_popular_artist
    """
    try:
        with get_duckdb_connection() as conn:
            # Get genre counts and pre-calculated artist info from dim_artist_genre
            query = """
                SELECT
                    dag.genre,
                    COUNT(fag.track_sid) as track_count,
                    dag.top_artist_in_genre as top_artist,
                    dag.most_popular_artist_in_genre as most_popular_artist
                FROM main_dw.fact_artist_genre fag
                JOIN main_dw.dim_artist_genre dag ON fag.genre_sid = dag.genre_sid
                JOIN main_dw.dim_date dd ON fag.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND dag.genre IS NOT NULL
                    AND dag.genre != 'no genre defined'
            """
            params = [start_date, end_date]

            if genres:
                placeholders = ",".join(["?" for _ in genres])
                query += f" AND dag.genre IN ({placeholders})"
                params.extend(genres)

            query += " GROUP BY dag.genre, dag.top_artist_in_genre, dag.most_popular_artist_in_genre ORDER BY track_count DESC LIMIT 25"

            result = conn.execute(query, params).pl()
            logger.debug(
                f"Genre distribution query result shape: {result.shape if result is not None else 'None'}"
            )
            logger.debug(
                f"Genre distribution query result columns: {result.columns if result is not None else 'None'}"
            )
            if result is not None and len(result) > 0:
                logger.debug(f"First row: {result.row(0)}")
            logger.info(
                f"Fetched {len(result) if result is not None and not result.is_empty() else 0} genres for analysis"
            )

            return result

    except Exception as e:
        logger.error(
            f"Failed to fetch genre distribution for analysis: {e}", exc_info=True
        )
        return None


def get_artists_by_genre(
    start_date, end_date, selected_genres=None, selected_years=None
):
    """
    Get artists ranked by track count within selected genres and years.

    Args:
        start_date: Start date (datetime)
        end_date: End date (datetime)
        selected_genres: Optional list of genre names to filter (top 25)
        selected_years: Optional list of year numbers to filter

    Returns:
        DataFrame with columns: artist_name, track_count
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    da.artist_name,
                    COUNT(fag.track_sid) as track_count
                FROM main_dw.fact_artist_genre fag
                JOIN main_dw.dim_artist_genre dag ON fag.genre_sid = dag.genre_sid
                JOIN main_dw.dim_artist da ON fag.artist_sid = da.artist_sid
                JOIN main_dw.dim_date dd ON fag.date_sid = dd.date_sid
                WHERE dd."date" >= ?
                    AND dd."date" <= ?
                    AND dag.genre IS NOT NULL
                    AND dag.genre != 'no genre defined'
            """
            params = [start_date, end_date]

            if selected_genres:
                placeholders = ",".join(["?" for _ in selected_genres])
                query += f" AND dag.genre IN ({placeholders})"
                params.extend(selected_genres)

            if selected_years:
                placeholders = ",".join(["?" for _ in selected_years])
                query += f" AND dd.year_num IN ({placeholders})"
                params.extend(selected_years)

            query += " GROUP BY da.artist_sid, da.artist_name ORDER BY track_count DESC LIMIT 25"

            result = conn.execute(query, params).pl()
            return result

    except Exception as e:
        logger.error(f"Failed to fetch artists by genre: {e}")
        return None
