{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}



select e."ID" as id_entity, e."ADDRESS" as entity_address, e."CP" as entity_cp, e.description as entity_description
, e."EMAIL" as entity_email, e.name as entity_name, e."NIF" as entity_nif, e."PHONE" as entity_phone, e."WEB" as entity_web
, jsonb_path_query(lf.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as entity_legal_form
, jsonb_path_query(s.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'as entity_sector
, t."ID" id_town, , jsonb_path_query(t.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as entity_town
, r."ID" as id_comarca, , jsonb_path_query(r.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as entity_comarca
, p."ID" as id_provincia, , jsonb_path_query(p.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as entity_provincia
, ac."ID" as id_ccaa, , jsonb_path_query(ac.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as entity_ccaa
from {{ source('dwhec', 'entities')}} e
join {{ source('dwhec', 'user')}} u on e.id_user = u."ID"
join {{ source('dwhec', 'legal_forms')}} lf on e.id_legal_form = lf."ID"
join {{ source('dwhec', 'sectors')}} s on e.id_sector = s."ID"
join {{ source('dwhec', 'entity_state')}} es on e.id_bs_state = es."ID"
join {{ source('dwhec', 'towns')}} t on e.id_town = t."ID"
join {{ source('dwhec', 'regions')}} r on t.id_region = r."ID"
join {{ source('dwhec', 'provinces')}} p on r.id_province = p."ID"
join {{ source('dwhec', 'entity_state')}} es2 on e.id_xes_state = es2."ID"
join {{ source('dwhec', 'autonomous_community')}} ac on e.id_autonomous_community = ac."ID"
left join {{ source('dwhec', 'neighbourhood')}} n on e.id_neighbourhood = n.id
left join {{ source('dwhec', 'district')}} d on n.id_district = d.id
left join {{ source('dwhec', 'sectors')}} s2 on e.id_secondary_sector = s2."ID"
