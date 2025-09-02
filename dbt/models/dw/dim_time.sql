SELECT DISTINCT
    time_sid
    , time_sid AS "time"
    , EXTRACT(HOUR FROM time_sid) AS hour_of_day
    , EXTRACT(MINUTE FROM time_sid) AS minute_of_hour
    , EXTRACT(SECOND FROM time_sid) AS second_of_hour
    , CASE WHEN EXTRACT(HOUR FROM time_sid) < 12 THEN 'AM' ELSE 'PM' END AS am_pm
    , CASE
        WHEN EXTRACT(HOUR FROM time_sid) < 6
            THEN 'Night'
        WHEN EXTRACT(HOUR FROM time_sid) < 12
            THEN 'Morning'
        WHEN EXTRACT(HOUR FROM time_sid) < 18
            THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_of_day
    , CASE
        WHEN EXTRACT(HOUR FROM time_sid) < 6
            THEN 1
        WHEN EXTRACT(HOUR FROM time_sid) < 12
            THEN 2
        WHEN EXTRACT(HOUR FROM time_sid) < 18
            THEN 3
        ELSE 4
    END AS time_of_day_sort
FROM {{ ref('times') }}
