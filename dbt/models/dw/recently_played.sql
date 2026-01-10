{{
  config(
    materialized = 'view',
    )
}}

SELECT
    user_id
    , track_id
    , uri
    , track_isrc
    , track_name
    , album_id
    , album_uri
    , album
    , artist_id
    , artist_mbid
    , artist
    , duration_ms
    , ROUND(CAST(duration_ms AS FLOAT) / 60000.0, 2) AS minutes_played
    , played_at
    , popularity
    , request_after
    , play_source
FROM read_csv('{% if target.name == 'dev' %}/app/data/recently_played.csv{% else %}../data/recently_played.csv{% endif %}')
