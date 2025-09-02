SELECT {{ dbt_utils.generate_surrogate_key(['track_id', 'track_name', 'artist']) }} AS track_sid
    , track_name
    , artist || ' - ' || track_name AS artist_track
    , track_popularity
    , CASE WHEN track_popularity >= 90 THEN 'Extremely Popular'
          WHEN track_popularity >= 70 THEN 'Very Popular'
          WHEN track_popularity >= 50 THEN 'Popular'
          WHEN track_popularity >= 30 THEN 'Niche'
          WHEN track_popularity >= 10 THEN 'Mostly Unknown'
          WHEN track_popularity < 10 THEN 'Not Popular'
          ELSE 'No Track Popularity Data'
        END AS track_popularity_group
    , CASE WHEN track_popularity >= 90 THEN 6
          WHEN track_popularity >= 70 THEN 5
          WHEN track_popularity >= 50 THEN 4
          WHEN track_popularity >= 30 THEN 3
          WHEN track_popularity >= 10 THEN 2
          WHEN track_popularity < 10 THEN 1
          ELSE 0
        END AS track_popularity_sort
    , CASE WHEN track_popularity >= 50 THEN 'Popular' ELSE 'Not Popular' END AS is_popular
    , track_duration_sec AS track_seconds
    , track_duration_min AS track_minutes
    , track_duration
    , CASE
        WHEN track_duration_sec IS NULL
            THEN 'unknown'
        WHEN track_duration_sec < 30
            THEN 'very short'
        WHEN track_duration_sec < 180
            THEN 'short'
        WHEN track_duration_sec < 300
            THEN 'average'
        WHEN track_duration_sec < 420
            THEN 'pretty long'
        WHEN track_duration_sec < 600
            THEN 'long'
        ELSE 'possibly too long'
        END AS track_length
    , CASE
        WHEN track_duration_sec IS NULL
            THEN 0
        WHEN track_duration_sec < 30
            THEN 1
        WHEN track_duration_sec < 180
            THEN 2
        WHEN track_duration_sec < 300
            THEN 3
        WHEN track_duration_sec < 420
            THEN 5
        WHEN track_duration_sec < 600
            THEN 6
        ELSE 7
        END AS track_length_sort
FROM {{ ref('tracks') }}