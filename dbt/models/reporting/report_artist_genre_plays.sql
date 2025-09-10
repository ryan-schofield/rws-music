-- depends_on: {{ ref('report_track_plays') }}

{{
  config(
    materialized='external',
    location='../data/report_artist_genre_plays.parquet' 
    )
}}

SELECT
    fg.track_played_uid
    , fg.track_sid
    , fg.artist_sid
    , fg.album_sid
    , fg.genre_sid
    , fg.date_sid
    , fg.time_sid
    , fg.popularity
    , fg.duration_ms
    , fg.duration_mins
    , dt.track_name
    , dt.artist_track
    , dt.track_popularity
    , dt.track_popularity_group
    , dt.track_popularity_sort
    , dt.is_popular
    , dt.track_seconds
    , dt.track_minutes
    , dt.track_duration
    , dt.track_length
    , dt.track_length_sort
    , da.album_name
    , da.artist_album
    , da.label
    , da.total_tracks
    , da.album_popularity
    , da.release_date
    , da.release_year
    , da.release_decade
    , da.release_date_precision
    , dar.artist_name
    , dar.artist_popularity
    , dar.artist_popularity_group
    , dar.artist_popularity_sort
    , dar.is_popular AS artist_is_popular
    , dar.primary_genre
    , dar.continent_code
    , dar.continent
    , dar.country_code
    , dar.country
    , dar.state_province_code
    , dar.state_province
    , dar.county
    , dar.city
    , dar.district
    , dar.lat
    , dar.longitude
    , dag.genre
    , dag.top_artist_in_genre
    , dag.most_popular_artist_in_genre
    , dag.rank_by_tracks_played
    , dag.tracks_grouping
    , dag.group_by_tracks_played
    , dag.total_tracks_played_in_genre
    , dag.rank_by_time_played
    , dag.time_grouping
    , dag.group_by_time_played
    , dag.total_minutes_played_in_genre
    , dd.date
    , dd.day_of_week
    , dd.day_of_week_name
    , dd.week_of_year
    , dd.year_num
    , dd.year_text
    , dd.month_num
    , dd.month_name
    , dd.quarter
    , dd.day_of_month
    , dd.day_of_year
    , dd.month_start_date
    , dd.month_end_date
    , dd.year_start_date
    , dd.year_end_date
    , dtm.time
    , dtm.hour_of_day
    , dtm.minute_of_hour
    , dtm.second_of_hour
    , dtm.am_pm
    , dtm.time_of_day
    , dtm.time_of_day_sort
FROM {{ ref('fact_artist_genre') }} fg
LEFT JOIN {{ ref('dim_track') }} dt
    ON fg.track_sid = dt.track_sid
LEFT JOIN {{ ref('dim_artist') }} dar
    ON fg.artist_sid = dar.artist_sid
LEFT JOIN {{ ref('dim_album') }} da
    ON fg.album_sid = da.album_sid
LEFT JOIN {{ ref('dim_artist_genre') }} dag
    ON fg.genre_sid = dag.genre_sid
LEFT JOIN {{ ref('dim_date') }} dd
    ON fg.date_sid = dd.date_sid
LEFT JOIN {{ ref('dim_time') }} dtm
    ON fg.time_sid = dtm.time_sid
