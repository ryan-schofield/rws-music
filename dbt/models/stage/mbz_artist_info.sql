SELECT
    alias_count
    , area_disambiguation
    , area_id
    , area_life_span_begin
    , area_name
    , area_sort_name
    , begin_area_disambiguation
    , begin_area_id
    , begin_area_life_span_begin
    , begin_area_name
    , begin_area_sort_name
    , country
    , disambiguation
    , end_area_id
    , end_area_name
    , end_area_sort_name
    , gender
    , id
    , ipi
    , life_span_begin
    , life_span_end
    , life_span_ended
    , "name"
    , release_group_count
    , sort_name
    , source_file
    , tag_list
    , "type"
    , end_area_disambiguation
    , spotify_id
FROM {{ source('lh', 'mbz_artist_info') }}
