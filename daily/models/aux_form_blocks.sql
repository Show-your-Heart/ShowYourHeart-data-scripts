{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select *
from {{ source('dwhec', 'form_blocks')}} fbp