SELECT {{ dbt_utils.generate_surrogate_key(['track_id', 'track_name', 'artist']) }} AS track_sid
    , {{ dbt_utils.generate_surrogate_key(['artist_id', 'artist']) }} AS artist_sid
    , {{ dbt_utils.generate_surrogate_key(['album_id', 'album', 'artist']) }} AS album_sid
    , artist AS artist_name
    , date_sid
    , time_sid
    , popularity
    , artist_popularity
    , duration_ms
    , duration_mins
    , tracks_played
FROM {{ ref('played') }}