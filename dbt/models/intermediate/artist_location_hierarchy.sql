SELECT
    al.artist_id
    , al.artist_mbid
    , al.artist_name
    , ah.continent_code
    , ah.continent
    , ah.country_code
    , ah.country_name AS country
    , cl.state_code AS state_province_code
    , ah.state_name AS state_province
    , ah.county_name AS county
    , ah.city_name AS city
    , ah.district_name AS district
    , cl.lat
    , cl.longitude
FROM {{ ref('artist_location') }} al
INNER JOIN {{ ref('area_hierarchy') }} ah
    ON al.area_id = ah.area_id
LEFT JOIN {{ ref('cities_with_lat_long') }} cl
    ON ah.params = cl.params
