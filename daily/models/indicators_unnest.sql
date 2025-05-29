{{ config(materialized='incremental'
, unique_key='indicator_year'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}


select emi."ID" id_module_info, emi.closed_date as module_info_closed_date , emi.start_date as module_info_start_date
, emi."STATE" as module_info_state, emi.id_entity,  m."MODULE_KEY" as indicator_module_key,c.year as indicator_year
, iv.value as indicator_value_origin
, unnest(case when i.value_type like 'Gender%' then (string_to_array(replace(replace(iv.value,'[',''),']',''),',')) end) as indicator_value
, unnest('{d,h,a}'::varchar[]) as indicator_genders
, i."INDICATOR_KEY" as indicator_key, I."FORMULA" as indicator_formula, i."UNIT" as indicator_unit, i.value_type as indicator_value_type
, jsonb_path_query(i."name"::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'as indicator_name
, jsonb_path_query(i."description"::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'as indicator_description
from {{ source('dwhec', 'entity_module_info')}} emi
join {{ source('dwhec', 'modules')}} m on emi.id_module = m."ID"
join {{ source('dwhec', 'campaigns')}} c on m.id_campaign = c."ID"
join {{ source('dwhec', 'indicator_value')}} iv on emi."ID"=iv.id_module_info
join {{ source('dwhec', 'indicators')}} i on iv.id_indicator = i."ID" and m.id_campaign = i.id_campaign
where 1=1
--and id_entity =2809
and i.value_type ilike '%gender%'

    {% if is_incremental() %}
    and c.year >= date_part('year', current_date-300)
    {% endif %}

union all
select emi."ID" id_module_info, emi.closed_date as module_info_closed_date , emi.start_date as module_info_start_date
, emi."STATE" as module_info_state, emi.id_entity,  m."MODULE_KEY",c.year
, iv.value as indicator_value
, iv.value  as indicator_value
, null as indicator_genders
, i."INDICATOR_KEY" as indicator_key, I."FORMULA" as indicator_formula, i."UNIT" as indicator_unit, i.value_type as indicator_value_type
, jsonb_path_query(i."name"::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'as indicator_name
, jsonb_path_query(i."description"::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'as indicator_description
from {{ source('dwhec', 'entity_module_info')}} emi
join {{ source('dwhec', 'modules')}} m on emi.id_module = m."ID"
join {{ source('dwhec', 'campaigns')}} c on m.id_campaign = c."ID"
join {{ source('dwhec', 'indicator_value')}} iv on emi."ID"=iv.id_module_info
join {{ source('dwhec', 'indicators')}} i on iv.id_indicator = i."ID" and m.id_campaign = i.id_campaign
where 1=1
--and id_entity =2809
and i.value_type  in ('Number', 'Decimal')
and iv.value is not null
and iv.value  ~ '^-?[0-9]+(\.[0-9]+)?$'

    {% if is_incremental() %}
    and c.year >= date_part('year', current_date-300)
    {% endif %}
