WITH tracks_agg AS (
    SELECT
        t.artist_id
        , t.artist
        , COUNT(*) AS total_tracks_played
        , SUM(t.duration_ms) AS total_ms_played
        , a.artist_popularity
    FROM {{ ref('tracks_played') }} t
    LEFT JOIN {{ ref('spotify_artists') }} a
        ON t.artist_id = a.artist_id
    GROUP BY
        t.artist_id
        , t.artist
        , a.artist_popularity
)

SELECT DISTINCT
    COALESCE(g.genre, 'no genre defined') AS genre
    , FIRST_VALUE(t.artist) OVER (
        PARTITION BY g.genre ORDER BY t.total_tracks_played DESC
    ) AS top_artist_in_genre
    , FIRST_VALUE(t.artist) OVER (
        PARTITION BY g.genre ORDER BY t.artist_popularity DESC
    ) AS most_popular_artist_in_genre
    , SUM(t.total_tracks_played) OVER (PARTITION BY g.genre) AS total_tracks_played_in_genre
    , SUM(t.total_ms_played) OVER (PARTITION BY g.genre) AS total_ms_played_in_genre
FROM tracks_agg t
LEFT JOIN {{ ref('spotify_artist_genre') }} g
    ON t.artist_id = g.artist_id
