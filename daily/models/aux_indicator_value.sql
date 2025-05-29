{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}


SELECT id, "version", id_indicator, id_module_info, value
FROM {{ source('dwhec', 'indicator_value')}}