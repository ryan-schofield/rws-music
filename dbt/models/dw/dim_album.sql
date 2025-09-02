SELECT {{ dbt_utils.generate_surrogate_key(['album_id', 'album', 'artist']) }} AS album_sid
    , album AS album_name
    , artist_album
    , label
    , total_tracks
    , album_popularity
    , release_date
    , release_year
    , release_decade
    , release_date_precision
FROM {{ ref('albums') }}
