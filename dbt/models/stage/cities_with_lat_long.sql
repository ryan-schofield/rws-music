SELECT
    city_name
    , country_code
    , params
    , NULLIF(state_code, '') AS state_code
    , TRY_CAST(lat AS DOUBLE) AS lat
    , TRY_CAST(long AS DOUBLE) AS longitude
FROM {{ source('lh', 'cities_with_lat_long') }}
