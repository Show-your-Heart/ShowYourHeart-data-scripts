{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select *
    , jsonb_path_query(cu.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as valca
    , jsonb_path_query(cu.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as vales
from {{ source('dwhec', 'custom_list_item')}} cu