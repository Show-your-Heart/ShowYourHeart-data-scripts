{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select a."ID", a.fixed_value, a.value, a."version", a.id_campaign, a.id_email, a.id_entity, a.id_question, a.id_last_answer
from {{ source('dwhec', 'answers')}} a
