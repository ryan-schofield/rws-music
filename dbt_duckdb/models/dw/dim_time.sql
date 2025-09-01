SELECT DISTINCT time_sid
    , time_sid AS [time]
    , DATEPART(HOUR, time_sid) AS hour_of_day
    , DATEPART(MINUTE, time_sid) AS minute_of_hour
    , DATEPART(SECOND, time_sid) AS second_of_hour
    , IIF(DATEPART(HOUR, time_sid) < 12, 'AM', 'PM') AS am_pm
    , CASE 
        WHEN DATEPART(HOUR, time_sid) < 6
            THEN 'Night'
        WHEN DATEPART(HOUR, time_sid) < 12
            THEN 'Morning'
        WHEN DATEPART(HOUR, time_sid) < 18
            THEN 'Afternoon'
        ELSE 'Evening'
        END AS time_of_day
    , CASE 
        WHEN DATEPART(HOUR, time_sid) < 6
            THEN 1
        WHEN DATEPART(HOUR, time_sid) < 12
            THEN 2
        WHEN DATEPART(HOUR, time_sid) < 18
            THEN 3
        ELSE 4
        END AS time_of_day_sort
FROM {{ ref('times') }}