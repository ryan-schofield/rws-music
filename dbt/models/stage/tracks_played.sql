{%- set convert_played_at = "played_at::TIMESTAMPTZ AT TIME ZONE 'America/Denver'" -%}

SELECT DISTINCT
    user_id
    , track_id
    , track_uri
    , track_isrc
    , track_name
    , album_id
    , album_uri
    , album
    , artist_id
    , artist_mbid
    , artist
    , duration_ms
    , ROUND(CAST(duration_ms AS FLOAT) / 60000, 0) AS duration_mins
    , played_at
    , {{ convert_played_at }} AS played_at_local
    , popularity
    , request_cursor
    , play_source
FROM {{ source('lh', 'tracks_played') }}
