SELECT
    area_id
    , area_type
    , area_name
    , area_sort_name
    , continent_code
    , continent
    , COALESCE(country_id, island_id) AS country_id
    , country_code
    , COALESCE(country_name, island_name) AS country_name
    , subdivision_id AS state_id
    , subdivision_name AS state_name
    , county_id
    , county_name
    , COALESCE(city_id, municipality_id) AS city_id
    , COALESCE(city_name, municipality_name) AS city_name
    , district_id
    , district_name
    , params
FROM {{ ref('mbz_area_hierarchy') }}
