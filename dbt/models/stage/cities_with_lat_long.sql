SELECT
    city_name
    , NULLIF(state_code, '') AS state_code
    , country_code
    , params
    , TRY_CAST(lat AS FLOAT) AS lat
    , TRY_CAST(long AS FLOAT) AS long
FROM {{ source('lh', 'cities_with_lat_long') }}
