SELECT {{ dbt_utils.generate_surrogate_key(['genre', "''"]) }} AS genre_sid 
    , genre
    , top_artist_in_genre
    , most_popular_artist_in_genre
    , rank_by_tracks_played
    , tracks_grouping
    , CASE 
        WHEN tracks_grouping = 1
            THEN 'Group 1 (Most Played Genres)'
        WHEN tracks_grouping = 5
            THEN 'Group 5 (Least Played Genres)'
        ELSE 'Group ' + CAST(tracks_grouping AS VARCHAR)
        END AS group_by_tracks_played
    , total_tracks_played_in_genre
    , rank_by_time_played
    , time_grouping
    , CASE 
        WHEN time_grouping = 1
            THEN 'Group 1 (Most Played Genres)'
        WHEN time_grouping = 5
            THEN 'Group 5 (Least Played Genres)'
        ELSE 'Group ' + CAST(time_grouping AS VARCHAR)
        END AS group_by_time_played
    , ROUND(CAST(COALESCE(total_ms_played_in_genre, 0) AS FLOAT) / 60000, 2) AS total_minutes_played_in_genre
FROM {{ ref('ranked_genre') }}
