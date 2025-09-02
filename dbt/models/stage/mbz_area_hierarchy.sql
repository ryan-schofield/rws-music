SELECT
    area_id
    , area_type
    , area_name
    , area_sort_name
    , city_id
    , city_name
    , city_sort_name
    , country_id
    , country_name
    , country_sort_name
    , county_id
    , county_name
    , county_sort_name
    , district_id
    , district_name
    , district_sort_name
    , island_id
    , island_name
    , island_sort_name
    , municipality_id
    , municipality_name
    , municipality_sort_name
    , subdivision_id
    , subdivision_name
    , subdivision_sort_name
    , unknown_id
    , unknown_name
    , unknown_sort_name
    , continent
    , country_code
    , continent_code
    , state_code
    , params
FROM {{ source('lh', 'mbz_area_hierarchy') }}
