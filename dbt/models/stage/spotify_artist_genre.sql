SELECT
    artist_id
    , artist_name
    , genre
FROM {{ source('lh', 'spotify_artist_genre') }}
WHERE artist_id IS NOT NULL
