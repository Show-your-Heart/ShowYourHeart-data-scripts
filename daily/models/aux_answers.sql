{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select *
, jsonb_path_query(cu.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'
from {{ source('dwhec', 'answers')}} cu