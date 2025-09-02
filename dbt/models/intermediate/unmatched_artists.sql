WITH unmatched_artists_cte AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY artist_name ORDER BY count_played DESC
        ) AS row_num
    FROM {{ ref('unmatched_artists_prep') }}
)

SELECT
    artist_name
    , artist_mbid
    , potential_match
    , count_played
FROM unmatched_artists_cte
WHERE row_num = 1
