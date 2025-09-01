WITH artist_base_cte AS (
SELECT a.artist_id
    , a.artist_mbid
    , a.artist_name
    , a.artist_popularity
    , g.primary_genre
    , l.continent_code
    , l.continent
    , l.country_code
    , l.country
    , l.state_province_code
    , l.state_province
    , l.county
    , l.city
    , l.district
    , l.lat
    , l.long
    , ROW_NUMBER() OVER (PARTITION BY a.artist_id ORDER BY a.artist_popularity DESC, l.country_code DESC) AS artist_sort
FROM {{ ref('spotify_artists_distinct') }} a
LEFT JOIN {{ ref('artist_top_genre') }} g
    ON a.artist_id = g.artist_id
LEFT JOIN {{ ref('artist_location_hierarchy') }} l
    ON a.artist_id = l.artist_id

UNION ALL

SELECT NULL AS artist_id
    , u.artist_mbid
    , u.artist_name
    , NULL AS artist_popularity
    , NULL AS primary_genre
    , l.continent_code
    , l.continent
    , l.country_code
    , l.country
    , l.state_province_code
    , l.state_province
    , l.county
    , l.city
    , l.district
    , l.lat
    , l.long
    , ROW_NUMBER() OVER (PARTITION BY u.artist_name ORDER BY u.count_played DESC, l.country_code DESC) AS artist_sort
FROM {{ ref('unmatched_artists') }} u
LEFT JOIN {{ ref('artist_location_hierarchy') }} l
    ON u.artist_mbid = l.artist_mbid
)

SELECT artist_id
    , artist_mbid
    , artist_name
    , artist_popularity
    , primary_genre
    , continent_code
    , continent
    , country_code
    , country
    , state_province_code
    , state_province
    , county
    , city
    , district
    , lat
    , long
FROM artist_base_cte
WHERE artist_sort = 1
