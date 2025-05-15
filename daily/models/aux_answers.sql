{{ config(materialized='incremental'
, unique_key='id_campaign'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select "ID", fixed_value, value, "version", id_campaign, id_email, id_entity, id_question, id_last_answer
from {{ source('dwhec', 'answers')}} cu
--
--{% if is_incremental() %}
--
--where id_campaign = date_part('year', current_date)
--
--{% endif %}