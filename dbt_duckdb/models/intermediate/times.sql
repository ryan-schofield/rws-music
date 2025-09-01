SELECT CAST(DATEADD(SECOND, int_range, '1900-01-01') AS TIME(0)) AS time_sid
FROM (
    SELECT ones.int_val + (10 * tens.int_val) + (100 * hundreds.int_val) + (1000 * thousands.int_val) + (10000 * ten_thousands.int_val) AS int_range
    FROM {{ ref('integers') }} ones
    CROSS APPLY {{ ref('integers') }} tens
    CROSS APPLY {{ ref('integers') }} hundreds
    CROSS APPLY {{ ref('integers') }} thousands
    CROSS APPLY {{ ref('integers') }} ten_thousands
    ) x
WHERE x.int_range <= 86400