WITH album_cte AS (
    SELECT
        MAX(artist_id) AS artist_id
        , MAX(album_id) AS album_id
        , artist
        , album
    FROM {{ ref('tracks_played') }}
    GROUP BY
        artist
        , album
)

, album_attribute_cte AS (
    SELECT
        a.artist_id
        , a.album_id
        , a.artist
        , a.album
        , a.artist || ' - ' || COALESCE(a.album, '') AS artist_album
        , s.label
        , s.total_tracks
        , MAX(s.popularity) AS album_popularity
        , CAST(LEFT(s.release_date || '-01-01', 10) AS DATE) AS release_date
        , s.release_date_precision
    FROM album_cte a
    LEFT JOIN {{ ref('spotify_albums') }} s
        ON a.album_id = s.album_id
    GROUP BY
        a.artist_id
        , a.album_id
        , a.artist
        , a.album
        , s.label
        , s.total_tracks
        , s.release_date
        , s.release_date_precision
)

SELECT
    artist_id
    , album_id
    , artist
    , album
    , artist_album
    , label
    , total_tracks
    , album_popularity
    , release_date
    , YEAR(release_date) AS release_year
    , CAST(LEFT(CAST(YEAR(release_date) AS VARCHAR), 3) || '0' AS INT) AS release_decade
    , release_date_precision
FROM album_attribute_cte
