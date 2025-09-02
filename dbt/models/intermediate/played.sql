SELECT
    track_id
    , t.track_name
    , t.artist
    , t.artist_id
    , t.album_id
    , t.album
    , t.request_cursor
    , t.played_at
    , t.played_at_local
    , CAST(t.played_at_local AS DATE) AS date_sid
    , CAST(t.played_at_local AS TIME) AS time_sid
    , t.popularity
    , a.artist_popularity
    , t.duration_ms
    , t.duration_mins
    , 1 AS tracks_played
FROM {{ ref('tracks_played') }} t
LEFT JOIN {{ ref('artists') }} a
    ON t.artist_id = a.artist_id
