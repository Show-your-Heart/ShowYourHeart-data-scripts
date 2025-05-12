{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select *
, jsonb_path_query(q.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'
from {{ source('dwhec', 'answers')}} cu