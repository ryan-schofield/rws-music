
SELECT {{ dbt_utils.generate_surrogate_key(['t.track_id', 't.track_name', 't.artist', 't.played_at']) }} AS track_played_uid 
    , {{ dbt_utils.generate_surrogate_key(['t.track_id', 't.track_name', 't.artist']) }} AS track_sid
    , {{ dbt_utils.generate_surrogate_key(['t.artist_id', 't.artist']) }} AS artist_sid
    , {{ dbt_utils.generate_surrogate_key(['t.album_id', 't.album', 't.artist']) }} AS album_sid
    , {{ dbt_utils.generate_surrogate_key(['g.genre', "''"]) }} AS genre_sid
    , date_sid
    , time_sid
    , popularity
    , duration_ms
    , duration_mins
FROM {{ ref('played') }} t
LEFT JOIN {{ ref('spotify_artist_genre') }} g
ON t.artist_id = g.artist_id