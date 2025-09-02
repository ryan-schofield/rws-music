SELECT
    MAX(track_id) AS track_id
    , MAX(track_isrc) AS track_isrc
    , track_name
    , artist
    , MAX(popularity) AS track_popularity
    , MAX(duration_ms) AS track_duration_ms
    , ROUND(CAST(MAX(duration_ms) AS FLOAT) / 1000, 0) AS track_duration_sec
    , ROUND(CAST(MAX(duration_ms) AS FLOAT) / 60000, 0) AS track_duration_min
    , CAST(DATE '1900-01-01' + INTERVAL '1' MILLISECOND * MAX(duration_ms) AS TIME) AS track_duration
FROM {{ source('lh', 'tracks_played') }}
GROUP BY
    track_name
    , artist
