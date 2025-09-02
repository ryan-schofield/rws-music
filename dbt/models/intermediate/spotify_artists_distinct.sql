SELECT
    artist_id
    , artist_name
    , MAX(artist_mbid) AS artist_mbid
    , MAX(artist_popularity) AS artist_popularity
FROM {{ ref('spotify_artists') }}
GROUP BY
    artist_id
    , artist_name
