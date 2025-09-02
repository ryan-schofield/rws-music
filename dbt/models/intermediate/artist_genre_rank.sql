SELECT *
    , ROW_NUMBER() OVER (
        PARTITION BY artist_id ORDER BY genre_total DESC
        ) AS artist_genre_rank
FROM (
    SELECT *
        , COUNT(*) OVER (PARTITION BY genre) AS genre_total
    FROM {{ ref('spotify_artist_genre') }} 
    ) x
