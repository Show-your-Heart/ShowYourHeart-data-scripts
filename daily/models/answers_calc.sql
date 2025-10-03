{{ config(materialized='table'
, tags=[ "SYH"]
, docs={'node_color': '#79964D'}
) }}

with recursive method_section_hieriarchy as (
select distinct s.id, s.title, s.order, s.method_id, s.parent_id, 1 as lvl, cast(s.order as text) as path_order
from {{ source('dwhpublic', 'syh_methods_section')}} s
where parent_id is null
union all
select distinct s.id, s.title, s.order, s.method_id, s.parent_id, ms.lvl+1 as lvl, ms.path_order || '.' || lpad(s.order::varchar,2,'0') AS path_order
from {{ source('dwhpublic', 'syh_methods_section')}} s
	join method_section_hieriarchy ms on ms.id=s.parent_id
)
select c.id as id_campaign, c.name as campaign_name, c.year, c.previous_campaign_id
    , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
    , m.id as id_method, m.active, m.name as method_name, m.description as method_description
    , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
    , o.id as id_organization, o.name as organization_name, o.vat_number --TODO afegir més camps
    , h.id as id_methods_section, h.title as method_section_title, h.order as method_order, h.lvl as method_level, h.path_order
    , si.sort_value
    , i.id as id_indicator, i.project_id, i.name as indicator_name, i.description as indicator_description
    	, i.is_direct_indicator, i.category as indicator_category, i.data_type as indicator_data_type, i.unit as indicator_unit
    , ir.id as id_indicatorresult, ir.gender, ir.value
from {{ source('dwhpublic', 'syh_methods_campaign')}} c
    join {{ source('dwhpublic', 'syh_methods_survey')}} s on s.campaign_id=c.id
    join {{ source('dwhpublic', 'syh_methods_method')}} m on s.method_id=m.id
    join {{ source('dwhpublic', 'syh_organizations_organization')}} o on s.organization_id=o.id
    join {{ source('dwhpublic', 'syh_users_user')}} u on s.user_id=u.id
    join method_section_hieriarchy h on h.method_id=m.id
    left join {{ source('dwhpublic', 'syh_methods_section_indicators')}} si on si.section_id=h.id
    left join {{ source('dwhpublic', 'syh_methods_indicator')}} i on si.indicator_id=i.id
    left join {{ source('dwhpublic', 'syh_methods_indicatorresult')}} ir on ir.indicator_id = i.id and ir.survey_id=s.id
 where 1=1