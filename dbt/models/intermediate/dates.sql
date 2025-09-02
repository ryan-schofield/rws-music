{%- set start_date = "2005-02-01" -%}
{%- set end_datetime = run_started_at + modules.datetime.timedelta(days=30) -%}
{%- set end_date = end_datetime.strftime("%Y-%m-%d") -%}

SELECT CAST(NULLIF(DATE '{{ start_date }}' + INTERVAL '1' DAY * y.int_range, DATE '1900-01-01') AS DATE) AS date_sid
FROM (
    SELECT ones.int_val + (10 * tens.int_val) + (100 * hundreds.int_val) + (1000 * thousands.int_val) AS int_range
    FROM {{ ref('integers') }} ones
    CROSS JOIN {{ ref('integers') }} tens
    CROSS JOIN {{ ref('integers') }} hundreds
    CROSS JOIN {{ ref('integers') }} thousands
    ) y
WHERE y.int_range <= DATE_DIFF('day', DATE '{{ start_date }}', DATE '{{ end_date }}')