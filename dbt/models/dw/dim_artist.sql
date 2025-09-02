SELECT {{ dbt_utils.generate_surrogate_key(['a.artist_id', 'a.artist_name']) }} AS artist_sid
    , COALESCE(m.mask_name, a.artist_name) AS artist_name
    , a.artist_popularity
    , CASE WHEN a.artist_popularity >= 90 THEN 'Extremely Popular'
          WHEN a.artist_popularity >= 70 THEN 'Very Popular'
          WHEN a.artist_popularity >= 50 THEN 'Popular'
          WHEN a.artist_popularity >= 30 THEN 'Niche Popularity'
          WHEN a.artist_popularity >= 10 THEN 'Mostly Unknown'
          WHEN a.artist_popularity < 10 THEN 'Not Popular'
          ELSE 'No Artist Popularity Data'
        END AS artist_popularity_group
    , CASE WHEN a.artist_popularity >= 90 THEN 6
          WHEN a.artist_popularity >= 70 THEN 5
          WHEN a.artist_popularity >= 50 THEN 4
          WHEN a.artist_popularity >= 30 THEN 3
          WHEN a.artist_popularity >= 10 THEN 2
          WHEN a.artist_popularity < 10 THEN 1
          ELSE 0
        END AS artist_popularity_sort
    , CASE WHEN artist_popularity IS NULL THEN 'No Artist Popularity Data'
          WHEN a.artist_popularity >= 50 THEN 'Popular'
          ELSE 'Not Popular' 
        END AS is_popular
    , COALESCE(a.primary_genre, 'no genre defined') AS primary_genre
    , a.continent_code
    , a.continent
    , a.country_code
    , a.country
    , a.state_province_code
    , a.state_province
    , a.county
    , a.city
    , a.district
    , a.lat
    , a.longitude
FROM {{ ref('artists') }} a
LEFT JOIN {{ ref('mask_artists') }} m
    ON a.artist_id = CAST(m.artist_id AS VARCHAR)
