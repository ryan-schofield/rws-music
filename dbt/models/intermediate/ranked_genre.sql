{% set groups = 5 %}

WITH group_cte
AS (
    SELECT g.*
        , ROW_NUMBER() OVER (
            ORDER BY total_tracks_played_in_genre DESC
            ) AS row_num_tracks
        , ROW_NUMBER() OVER (
            ORDER BY total_ms_played_in_genre DESC
            ) AS row_num_ms
        , COUNT(*) OVER (PARTITION BY 1) AS total_rows
        , COUNT(*) OVER (PARTITION BY 1) / {{ groups }} AS count_per_group
    FROM {{ ref('genre') }} g
    )
SELECT genre
    , top_artist_in_genre
    , most_popular_artist_in_genre
    , row_num_tracks AS rank_by_tracks_played
    , IIF(row_num_tracks = total_rows, {{ groups }}, (row_num_tracks / count_per_group) + 1) AS tracks_grouping
    , total_tracks_played_in_genre
    , row_num_ms AS rank_by_time_played
    , IIF(row_num_ms = total_rows, {{ groups }}, (row_num_ms / count_per_group) + 1) AS time_grouping
    , total_ms_played_in_genre
FROM group_cte