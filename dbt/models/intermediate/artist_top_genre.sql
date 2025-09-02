SELECT artist_id
    , artist_name
    , COALESCE(genre, 'no genre defined') AS primary_genre
FROM {{ ref('artist_genre_rank') }}
WHERE artist_genre_rank = 1