SELECT
    artist_id
    , artist_name
    , artist_mbid
    , artist_popularity
FROM {{ source('lh', 'spotify_artists') }}
