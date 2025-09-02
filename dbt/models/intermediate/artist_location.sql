SELECT 
    COALESCE(area_id, end_area_id, begin_area_id) AS area_id
    , spotify_id AS artist_id
    , id AS artist_mbid
    , name AS artist_name
FROM {{ ref('mbz_artist_info') }}
