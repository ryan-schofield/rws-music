SELECT
    COALESCE(date_sid, '1900-01-01') AS date_sid
    , date_sid AS "date"
    , EXTRACT(DOW FROM date_sid) AS day_of_week
    , STRFTIME(date_sid, '%A') AS day_of_week_name
    , EXTRACT(WEEK FROM date_sid) AS week_of_year
    , EXTRACT(YEAR FROM date_sid) AS year_num
    , CAST(EXTRACT(YEAR FROM date_sid) AS VARCHAR) AS year_text
    , EXTRACT(MONTH FROM date_sid) AS month_num
    , STRFTIME(date_sid, '%B') AS month_name
    , EXTRACT(QUARTER FROM date_sid) AS "quarter"
    , EXTRACT(DAY FROM date_sid) AS day_of_month
    , EXTRACT(DOY FROM date_sid) AS day_of_year
    , DATE_TRUNC('month', date_sid) AS month_start_date
    , LAST_DAY(date_sid) AS month_end_date
    , DATE_TRUNC('year', date_sid) AS year_start_date
    , DATE_TRUNC('year', date_sid) + INTERVAL '1 year' - INTERVAL '1 day' AS year_end_date
FROM {{ ref('dates') }}
