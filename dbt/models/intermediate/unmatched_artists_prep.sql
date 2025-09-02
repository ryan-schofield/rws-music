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
        , TRIM(SPLIT_PART(a.artist_delimited, '|', 1)) AS potential_match
        , count_played
    FROM artist_delimited_cte a
    WHERE SPLIT_PART(a.artist_delimited, '|', 1) != ''
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
