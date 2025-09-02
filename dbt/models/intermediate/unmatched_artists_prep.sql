{{ config(materialized='view') }}

WITH artist_delimited_cte AS (
    SELECT
        artist AS artist_name
        , artist_mbid
        , COUNT(*) AS count_played
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(artist, ',', '|'), 'feat.', '|'), '[', '|'), '(', '|'), ' & ', '|')
            AS artist_delimited
    FROM {{ source('lh', 'tracks_played') }}
    WHERE artist_id IS NULL
    GROUP BY
        artist
        , artist_mbid
)

, artist_match_cte AS (
    SELECT
        a.artist_name
        , a.artist_mbid
        , TRIM(
            CASE
                WHEN POSITION('|' IN a.artist_delimited) > 0
                THEN SUBSTRING(a.artist_delimited, 1, POSITION('|' IN a.artist_delimited) - 1)
                ELSE a.artist_name
            END
        ) AS potential_match
        , count_played
    FROM artist_delimited_cte a
    WHERE TRIM(
        CASE
            WHEN POSITION('|' IN a.artist_delimited) > 0
            THEN SUBSTRING(a.artist_delimited, 1, POSITION('|' IN a.artist_delimited) - 1)
            ELSE a.artist_name
        END
    ) != ''
)

SELECT
    artist_name
    , artist_mbid
    , potential_match
    , count_played
FROM artist_match_cte

UNION

SELECT
    artist_name
    , artist_mbid
    , artist_name AS potential_match
    , count_played
FROM artist_delimited_cte
WHERE artist_delimited NOT LIKE '%|%'
