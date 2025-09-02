SELECT CAST(DATE '1900-01-01' + INTERVAL '1' SECOND * int_range AS TIME) AS time_sid
FROM (
    SELECT
        ones.int_val
        + (10 * tens.int_val)
        + (100 * hundreds.int_val)
        + (1000 * thousands.int_val)
        + (10000 * ten_thousands.int_val) AS int_range
    FROM {{ ref('integers') }} ones
    CROSS JOIN {{ ref('integers') }} tens
    CROSS JOIN {{ ref('integers') }} hundreds
    CROSS JOIN {{ ref('integers') }} thousands
    CROSS JOIN {{ ref('integers') }} ten_thousands
) x
WHERE x.int_range <= 86400
