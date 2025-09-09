{{
  config(
    materialized='external',
    location='../data/report_track_plays.parquet'
  )
}}

SELECT
    ftp.track_sid
    , ftp.artist_sid
    , ftp.album_sid
    , ftp.artist_name
    , ftp.request_cursor
    , ftp.date_sid
    , ftp.time_sid
    , ftp.popularity
    , ftp.artist_popularity
    , ftp.duration_ms
    , ftp.duration_mins
    , ftp.tracks_played
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
    , dar.artist_name AS dim_artist_name
    , dar.artist_popularity AS dim_artist_popularity
    , dar.artist_popularity_group
    , dar.artist_popularity_sort
    , CONCAT(CAST(dar.artist_popularity_sort AS VARCHAR), ' - ', dar.artist_popularity_group) AS artist_popularity_label
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
    , CONCAT(CAST(dtm.time_of_day_sort AS VARCHAR), ' - ', dtm.time_of_day) AS time_of_day_label
FROM {{ ref('fact_track_played') }} ftp
LEFT JOIN {{ ref('dim_track') }} dt
    ON ftp.track_sid = dt.track_sid
LEFT JOIN {{ ref('dim_artist') }} dar
    ON ftp.artist_sid = dar.artist_sid
LEFT JOIN {{ ref('dim_album') }} da
    ON ftp.album_sid = da.album_sid
LEFT JOIN {{ ref('dim_date') }} dd
    ON ftp.date_sid = dd.date_sid
LEFT JOIN {{ ref('dim_time') }} dtm
    ON ftp.time_sid = dtm.time_sid
