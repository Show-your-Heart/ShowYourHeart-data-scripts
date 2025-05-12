{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select *
    , jsonb_path_query(fbp.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as valca
    , jsonb_path_query(fbp.name::jsonb, '$.texts[*] ? (@.la == "es").text') #>> '{}' as vales
from {{ source('dwhec', 'form_blocks')}} fbp