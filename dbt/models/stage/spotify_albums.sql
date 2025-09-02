SELECT
    album_type
    , artist_id
    , artist_name
    , artist_type
    , album_id
    , label
    , album_name
    , popularity
    , release_date
    , release_date_precision
    , total_tracks
    , last_modified
FROM {{ source('lh', 'spotify_albums') }}
