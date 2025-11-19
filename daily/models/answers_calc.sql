{{ config(materialized='incremental'
, unique_key='id_campaign'
, tags=[ "SYH"]
, docs={'node_color': '#C93314'}
, post_hook=after_commit("{{ create_index_answers_calc() }}")
) }}

with recursive method_section_hieriarchy as (
select distinct s.id
    , s.title, s.title_en, s.title_ca, s.title_gl, s.title_eu, s.title_es, s.title_nl
    , s.order, s.method_id, s.parent_id, 1 as lvl, cast(s.order as text) as path_order
from {{ source('dwhpublic', 'syh_methods_section')}} s
where parent_id is null
union all
select distinct s.id
    , s.title, s.title_en, s.title_ca, s.title_gl, s.title_eu, s.title_es, s.title_nl
    , s.order, s.method_id, s.parent_id, ms.lvl+1 as lvl, ms.path_order || '.' || lpad(s.order::varchar,2,'0') AS path_order
from {{ source('dwhpublic', 'syh_methods_section')}} s
	join method_section_hieriarchy ms on ms.id=s.parent_id
)
select c.id as id_campaign
        , c.name as campaign_name
        , c.name_en as campaign_name_en, c.name_ca as campaign_name_ca, c.name_gl as campaign_name_gl
        , c.name_eu as campaign_name_eu, c.name_es as campaign_name_es, c.name_nl as campaign_name_nl
        , c.year, c.previous_campaign_id
    , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
    , m.id as id_method, m.active
        , m.name as method_name
        , m.name_en as method_name_en, m.name_ca as method_name_ca, m.name_gl as method_name_gl
        , m.name_eu as method_name_eu, m.name_es as method_name_es, m.name_nl as method_name_nl
        , m.description as method_description
        , m.description_en as method_description_en, m.description_ca as method_description_ca
        , m.description_gl as method_description_gl, m.description_eu as method_description_eu
        , m.description_es as method_description_es, m.description_nl as method_description_nl
    , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
    , o.id as id_organization, o.name as organization_name, o.vat_number --TODO afegir més camps
    , null::uuid as id_methods_section
        ,'Indirect indicator'::varchar(500) as method_section_title
        ,'Indirect indicator'::varchar(500) as method_section_title_en
        ,'Indicator indirecte'::varchar(500) as method_section_title_ca
        ,'Indirect indicator'::varchar(500) as method_section_title_gl
        ,'Indirect indicator'::varchar(500) as method_section_title_eu
        ,'Indicador indirecto'::varchar(500) as method_section_title_es
        , 'Indirect indicator'::varchar(500) as method_section_title_nl
        , 999 as method_order,1 as method_level, '9999.01' as path_order
        , 999 as sort_value
    , i.id as id_indicator, i.code as indicator_code
        , i.name as indicator_name
        , i.name_en as indicator_name_en, i.name_ca as indicator_name_ca, i.name_gl as indicator_name_gl
        , i.name_eu as indicator_name_eu, i.name_es as indicator_name_es, i.name_nl as indicator_name_nl
        , i.description as indicator_description
        , i.description_en as indicator_description_en, i.description_ca as indicator_description_ca
        , i.description_gl as indicator_description_gl, i.description_eu as indicator_description_eu
        , i.description_es as indicator_description_es, i.description_nl as indicator_description_nl
    	, i.is_direct_indicator, i.category as indicator_category, i.data_type as indicator_data_type, i.unit as indicator_unit
    , ir.id as id_indicatorresult, ir.gender
        , case when replace(ir.value, ' ', '')=',' or  replace(ir.value, ' ', '')='' then null else regexp_replace(ir.value, ',\s*\]', ']', 'g') end as value
from {{ source('dwhpublic', 'syh_methods_campaign')}} c
    join {{ source('dwhpublic', 'syh_methods_survey')}} s on s.campaign_id=c.id
    join {{ source('dwhpublic', 'syh_methods_method')}} m on s.method_id=m.id
    join {{ source('dwhpublic', 'syh_organizations_organization')}} o on s.organization_id=o.id
    join {{ source('dwhpublic', 'syh_users_user')}} u on s.user_id=u.id
    left join {{ source('dwhpublic', 'syh_methods_indicatorresult')}} ir on ir.survey_id=s.id
    left join {{ source('dwhpublic', 'syh_methods_indicator')}} i on ir.indicator_id=i.id
    --left join method_section_hieriarchy h on h.method_id=m.id
    --left join {{ source('dwhpublic', 'syh_methods_section_indicators')}} si on si.section_id=h.id
    --    and si.indicator_id=i.id
where 1=1
       {% if is_incremental() %}

      and year>=(date_part('year', current_date)-1)::varchar


      {% endif %}

union all
select distinct c.id as id_campaign
        , c.name as campaign_name
        , c.name_en as campaign_name_en, c.name_ca as campaign_name_ca, c.name_gl as campaign_name_gl
        , c.name_eu as campaign_name_eu, c.name_es as campaign_name_es, c.name_nl as campaign_name_nl
        , c.year, c.previous_campaign_id
    , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
    , m.id as id_method, m.active
        , m.name as method_name
        , m.name_en as method_name_en, m.name_ca as method_name_ca, m.name_gl as method_name_gl
        , m.name_eu as method_name_eu, m.name_es as method_name_es, m.name_nl as method_name_nl
        , m.description as method_description
        , m.description_en as method_description_en, m.description_ca as method_description_ca
        , m.description_gl as method_description_gl, m.description_eu as method_description_eu
        , m.description_es as method_description_es, m.description_nl as method_description_nl
    , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
    , o.id as id_organization, o.name as organization_name, o.vat_number --TODO afegir més camps
    , h.id as id_methods_section
        , h.title as method_section_title
        , h.title_en as method_section_title_en, h.title_ca as method_section_title_ca
        , h.title_gl as method_section_title_gl, h.title_eu as method_section_title_eu
        , h.title_es as method_section_title_es, h.title_nl as method_section_title_nl
        , h.order as method_order, h.lvl as method_level, h.path_order
    , si.sort_value
    , null::uuid as id_indicator, null as indicator_code
        , null as indicator_name
        , null as indicator_name_en, null as indicator_name_ca, null as indicator_name_gl
        , null as indicator_name_eu, null as indicator_name_es, null as indicator_name_nl
        , null as indicator_description
        , null as indicator_description_en, null as indicator_description_ca
        , null as indicator_description_gl, null as indicator_description_eu
        , null as indicator_description_es, null as indicator_description_nl
    	, true as is_direct_indicator, null as indicator_category, null as indicator_data_type, null as indicator_unit
    , null::uuid as id_indicatorresult, null::int as gender, null as value
from {{ source('dwhpublic', 'syh_methods_campaign')}} c
    join {{ source('dwhpublic', 'syh_methods_survey')}} s on s.campaign_id=c.id
    join {{ source('dwhpublic', 'syh_methods_method')}} m on s.method_id=m.id
    join {{ source('dwhpublic', 'syh_organizations_organization')}} o on s.organization_id=o.id
    join {{ source('dwhpublic', 'syh_users_user')}} u on s.user_id=u.id
    join method_section_hieriarchy h on h.method_id=m.id
    left join {{ source('dwhpublic', 'syh_methods_section_indicators')}} si on si.section_id=h.id
where 1=1
       {% if is_incremental() %}

      and year>=(date_part('year', current_date)-1)::varchar


      {% endif %}


union all
select distinct c.id as id_campaign
        , c.name as campaign_name
        , c.name_en as campaign_name_en, c.name_ca as campaign_name_ca, c.name_gl as campaign_name_gl
        , c.name_eu as campaign_name_eu, c.name_es as campaign_name_es, c.name_nl as campaign_name_nl
        , c.year, c.previous_campaign_id
    , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
    , m.id as id_method, m.active
        , m.name as method_name
        , m.name_en as method_name_en, m.name_ca as method_name_ca, m.name_gl as method_name_gl
        , m.name_eu as method_name_eu, m.name_es as method_name_es, m.name_nl as method_name_nl
        , m.description as method_description
        , m.description_en as method_description_en, m.description_ca as method_description_ca
        , m.description_gl as method_description_gl, m.description_eu as method_description_eu
        , m.description_es as method_description_es, m.description_nl as method_description_nl
    , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
    , o.id as id_organization, o.name as organization_name, o.vat_number --TODO afegir més camps
    , h.id as id_methods_section
        , h.title as method_section_title
        , h.title_en as method_section_title_en, h.title_ca as method_section_title_ca
        , h.title_gl as method_section_title_gl, h.title_eu as method_section_title_eu
        , h.title_es as method_section_title_es, h.title_nl as method_section_title_nl
        , h.order as method_order, h.lvl as method_level, h.path_order
    , si.sort_value
    , null::uuid as id_indicator, null as indicator_code
        , null as indicator_name
        , null as indicator_name_en, null as indicator_name_ca, null as indicator_name_gl
        , null as indicator_name_eu, null as indicator_name_es, null as indicator_name_nl
        , null as indicator_description
        , null as indicator_description_en, null as indicator_description_ca
        , null as indicator_description_gl, null as indicator_description_eu
        , null as indicator_description_es, null as indicator_description_nl
    	, false as is_direct_indicator, null as indicator_category, null as indicator_data_type, null as indicator_unit
    , null::uuid as id_indicatorresult, null::int as gender, null as value
from {{ source('dwhpublic', 'syh_methods_campaign')}} c
    join {{ source('dwhpublic', 'syh_methods_survey')}} s on s.campaign_id=c.id
    join {{ source('dwhpublic', 'syh_methods_method')}} m on s.method_id=m.id
    join {{ source('dwhpublic', 'syh_organizations_organization')}} o on s.organization_id=o.id
    join {{ source('dwhpublic', 'syh_users_user')}} u on s.user_id=u.id
    join method_section_hieriarchy h on h.method_id=m.id
    left join {{ source('dwhpublic', 'syh_methods_section_indicators')}} si on si.section_id=h.id
where 1=1
       {% if is_incremental() %}

      and year>=(date_part('year', current_date)-1)::varchar


      {% endif %}