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
    , played_at
    , request_after
    , play_source
    , ROUND(CAST(duration_ms AS FLOAT) / 60000.0, 2) AS minutes_played
    , CASE
        WHEN COALESCE(popularity, 0) = 0 THEN '--' ELSE
            CONCAT(CAST(CAST(popularity AS INT) AS VARCHAR(3)), '/', '100')
    END AS popularity
FROM
    READ_CSV('{% if target.name == "dev" %}/home/runner/workspace/data/recently_played.csv{% else %}../data/recently_played.csv{% endif %}') -- noqa: L016
