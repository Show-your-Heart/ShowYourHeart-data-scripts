{{ config(materialized='incremental'
, unique_key='id_campaign'
, tags=[ "SYH"]
, docs={'node_color': '#C93314'}
, post_hook=after_commit("{{ create_index_answers_calc() }}")
) }}

    with recursive method_section_hieriarchy as (
    select distinct s.id
        , s.title
        , coalesce(s.title_en, s.title) as title_en
        , coalesce(s.title_ca, s.title) as title_ca
        , coalesce(s.title_gl, s.title) as title_gl
        , coalesce(s.title_eu, s.title) as title_eu
        , coalesce(s.title_es, s.title) as title_es
        , coalesce(s.title_nl, s.title) as title_nl
        , coalesce(s.title_fr, s.title) as title_fr
        , s.order, s.method_id, s.parent_id, 1 as lvl, cast(s.order as text) as path_order
    from {{ source('dwhpublic', 'syh_methods_section')}} s
    where parent_id is null
    union all
    select distinct s.id
        , s.title
        , coalesce(s.title_en, s.title) as title_en
        , coalesce(s.title_ca, s.title) as title_ca
        , coalesce(s.title_gl, s.title) as title_gl
        , coalesce(s.title_eu, s.title) as title_eu
        , coalesce(s.title_es, s.title) as title_es
        , coalesce(s.title_nl, s.title) as title_nl
        , coalesce(s.title_fr, s.title) as title_fr
        , s.order, s.method_id, s.parent_id, ms.lvl+1 as lvl, ms.path_order || '.' || lpad(s.order::varchar,2,'0') AS path_order
    from {{ source('dwhpublic', 'syh_methods_section')}} s
        join method_section_hieriarchy ms on ms.id=s.parent_id
    )
    select c.id as id_campaign
            ,  c.name as campaign_name
            ,  coalesce(c.name_en, c.name) as campaign_name_en
            ,  coalesce(c.name_ca, c.name) as campaign_name_ca
            ,  coalesce(c.name_gl, c.name) as campaign_name_gl
            ,  coalesce(c.name_eu, c.name) as campaign_name_eu
            ,  coalesce(c.name_es, c.name) as campaign_name_es
            ,  coalesce(c.name_nl, c.name) as campaign_name_nl
            ,  coalesce(c.name_fr, c.name) as campaign_name_fr
            , c.year, c.previous_campaign_id
        , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
        , m.id as id_method
            , m.name as method_name
            ,  coalesce(m.name_en, m.name) as method_name_en
            ,  coalesce(m.name_ca, m.name) as method_name_ca
            ,  coalesce(m.name_gl, m.name) as method_name_gl
            ,  coalesce(m.name_eu, m.name) as method_name_eu
            ,  coalesce(m.name_es, m.name) as method_name_es
            ,  coalesce(m.name_nl, m.name) as method_name_nl
            ,  coalesce(m.name_fr, m.name) as method_name_fr
            , m.description as method_description
            ,  coalesce(m.description_en, m.description) as method_description_en
            ,  coalesce(m.description_ca, m.description) as method_description_ca
            ,  coalesce(m.description_gl, m.description) as method_description_gl
            ,  coalesce(m.description_eu, m.description) as method_description_eu
            ,  coalesce(m.description_es, m.description) as method_description_es
            ,  coalesce(m.description_nl, m.description) as method_description_nl
            ,  coalesce(m.description_fr, m.description) as method_description_fr
        , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
        , o.id as id_organization, o.name as organization_name, o.vat_number, o.logo as organization_logo --TODO afegir més camps
        , null::uuid as id_methods_section
            , 'Indirect indicator'::varchar(500) as method_section_title
            , 'Indirect indicator'::varchar(500) as method_section_title_en
            , 'Indicator indirecte'::varchar(500) as method_section_title_ca
            , 'Indirect indicator'::varchar(500) as method_section_title_gl
            , 'Indirect indicator'::varchar(500) as method_section_title_eu
            , 'Indicador indirecto'::varchar(500) as method_section_title_es
            , 'Indirect indicator'::varchar(500) as method_section_title_nl
            , 'Indirect indicator'::varchar(500) as method_section_title_fr
            , 999 as method_order,1 as method_level, '9999.01' as path_order
            , 999 as sort_value
        , i.id as id_indicator, i.code as indicator_code
            , i.name as indicator_name
            ,  coalesce(i.name_en, i.name) as indicator_name_en
            ,  coalesce(i.name_ca, i.name) as indicator_name_ca
            ,  coalesce(i.name_gl, i.name) as indicator_name_gl
            ,  coalesce(i.name_eu, i.name) as indicator_name_eu
            ,  coalesce(i.name_es, i.name) as indicator_name_es
            ,  coalesce(i.name_nl, i.name) as indicator_name_nl
            ,  coalesce(i.name_fr, i.name) as indicator_name_fr
            , i.description as indicator_description
            ,  coalesce(i.description_en, i.description) as indicator_description_en
            ,  coalesce(i.description_ca, i.description) as indicator_description_ca
            ,  coalesce(i.description_gl, i.description) as indicator_description_gl
            ,  coalesce(i.description_eu, i.description) as indicator_description_eu
            ,  coalesce(i.description_es, i.description) as indicator_description_es
            ,  coalesce(i.description_nl, i.description) as indicator_description_nl
            ,  coalesce(i.description_fr, i.description) as indicator_description_fr
            , i.is_direct_indicator, i.category as indicator_category, i.data_type as indicator_data_type, i.unit as indicator_unit
        , ir.id as id_indicatorresult, ir.gender
            , case when replace(ir.value, ' ', '')=',' or  replace(ir.value, ' ', '')='' then null
                when ir.value like '[%' and ir.value not like '%]' then null
                else replace(regexp_replace(ir.value, ',\s*\]', ']', 'g'), '"', '')
              end as value
        , ir.instance_number
        , pr.id as id_project, pr.name as project_name
        ,g1.title as g1_title
        , coalesce(g1.title_en, g1.title) as g1_title_en
        , coalesce(g1.title_ca, g1.title) as g1_title_ca
        , coalesce(g1.title_gl, g1.title) as g1_title_gl
        , coalesce(g1.title_eu, g1.title) as g1_title_eu
        , coalesce(g1.title_es, g1.title) as g1_title_es
        , coalesce(g1.title_nl, g1.title) as g1_title_nl
        , coalesce(g1.title_fr, g1.title) as g1_title_fr
        ,g2.title as g2_title
        , coalesce(g2.title_en, g2.title) as g2_title_en
        , coalesce(g2.title_ca, g2.title) as g2_title_ca
        , coalesce(g2.title_gl, g2.title) as g2_title_gl
        , coalesce(g2.title_eu, g2.title) as g2_title_eu
        , coalesce(g2.title_es, g2.title) as g2_title_es
        , coalesce(g2.title_nl, g2.title) as g2_title_nl
        , coalesce(g2.title_fr, g2.title) as g2_title_fr
    from {{ source('dwhpublic', 'syh_methods_campaign')}} c
        join {{ source('dwhpublic', 'syh_methods_survey')}} s on s.campaign_id=c.id
        left join {{ source('dwhpublic', 'syh_organizations_project')}} pr on s.project_id=pr.id
        join {{ source('dwhpublic', 'syh_methods_method')}} m on s.method_id=m.id
        join {{ source('dwhpublic', 'syh_organizations_organization')}} o on s.organization_id=o.id
        join {{ source('dwhpublic', 'syh_users_user')}} u on s.user_id=u.id
        left join {{ source('dwhpublic', 'syh_methods_indicatorresult')}} ir on ir.survey_id=s.id
        left join {{ source('dwhpublic', 'syh_methods_indicator')}} i on ir.indicator_id=i.id
        left join {{ source('dwhpublic', 'syh_methods_groupitem')}} g1 on ir.group_item_id = g1.id
        left join {{ source('dwhpublic', 'syh_methods_groupitem')}} g2 on ir.group_2_item_id = g2.id
        --left join method_section_hieriarchy h on h.method_id=m.id
        --left join {{ source('dwhpublic', 'syh_methods_section_indicators')}} si on si.section_id=h.id
        --    and si.indicator_id=i.id
    where 1=1
           {% if is_incremental() %}

          and year>=(date_part('year', current_date)-1)::varchar


          {% endif %}
          and ((g1.title is not null and g2.title is not null) or coalesce(ir.value,'')<>'')

    union all
    select distinct c.id as id_campaign
            , c.name as campaign_name
            ,  coalesce(c.name_en, c.name) as campaign_name_en
            ,  coalesce(c.name_ca, c.name) as campaign_name_ca
            ,  coalesce(c.name_gl, c.name) as campaign_name_gl
            ,  coalesce(c.name_eu, c.name) as campaign_name_eu
            ,  coalesce(c.name_es, c.name) as campaign_name_es
            ,  coalesce(c.name_nl, c.name) as campaign_name_nl
            ,  coalesce(c.name_fr, c.name) as campaign_name_fr
            , c.year, c.previous_campaign_id
        , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
        , m.id as id_method
            , m.name as method_name
            ,  coalesce(m.name_en, m.name) as method_name_en
            ,  coalesce(m.name_ca, m.name) as method_name_ca
            ,  coalesce(m.name_gl, m.name) as method_name_gl
            ,  coalesce(m.name_eu, m.name) as method_name_eu
            ,  coalesce(m.name_es, m.name) as method_name_es
            ,  coalesce(m.name_nl, m.name) as method_name_nl
            ,  coalesce(m.name_fr, m.name) as method_name_fr
            , m.description as method_description
            ,  coalesce(m.description_en, m.description) as method_description_en
            ,  coalesce(m.description_ca, m.description) as method_description_ca
            ,  coalesce(m.description_gl, m.description) as method_description_gl
            ,  coalesce(m.description_eu, m.description) as method_description_eu
            ,  coalesce(m.description_es, m.description) as method_description_es
            ,  coalesce(m.description_nl, m.description) as method_description_nl
            ,  coalesce(m.description_fr, m.description) as method_description_fr
        , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
        , o.id as id_organization, o.name as organization_name, o.vat_number, o.logo as organization_logo --TODO afegir més camps
        , h.id as id_methods_section
            , h.title as method_section_title
            , coalesce(h.title_en, h.title) as method_section_title_en
            , coalesce(h.title_ca, h.title) as method_section_title_ca
            , coalesce(h.title_gl, h.title) as method_section_title_gl
            , coalesce(h.title_eu, h.title) as method_section_title_eu
            , coalesce(h.title_es, h.title) as method_section_title_es
            , coalesce(h.title_nl, h.title) as method_section_title_nl
            , coalesce(h.title_fr, h.title) as method_section_title_fr
            , h.order as method_order, h.lvl as method_level, h.path_order
        , si.sort_value
        , null::uuid as id_indicator, null as indicator_code
            , null as indicator_name
            , null as indicator_name_en, null as indicator_name_ca, null as indicator_name_gl
            , null as indicator_name_eu, null as indicator_name_es, null as indicator_name_nl
            , null as indicator_name_fr
            , null as indicator_description
            , null as indicator_description_en, null as indicator_description_ca
            , null as indicator_description_gl, null as indicator_description_eu
            , null as indicator_description_es, null as indicator_description_nl
            , null as indicator_description_fr
            , true as is_direct_indicator, null as indicator_category, null as indicator_data_type, null as indicator_unit
        , null::uuid as id_indicatorresult, null::int as gender, null as value
        , null::smallint as instance_number
        , null::uuid as id_project, null as project_name
        , null::varchar as g1_title
        , null::varchar as g1_title_en
        , null::varchar as g1_title_ca
        , null::varchar as g1_title_gl
        , null::varchar as g1_title_eu
        , null::varchar as g1_title_es
        , null::varchar as g1_title_nl
        , null::varchar as g1_title_fr
        , null::varchar as g2_title
        , null::varchar as g2_title_en
        , null::varchar as g2_title_ca
        , null::varchar as g2_title_gl
        , null::varchar as g2_title_eu
        , null::varchar as g2_title_es
        , null::varchar as g2_title_nl
        , null::varchar as g2_title_fr
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
            ,  coalesce(c.name_en, c.name) as campaign_name_en
            ,  coalesce(c.name_ca, c.name) as campaign_name_ca
            ,  coalesce(c.name_gl, c.name) as campaign_name_gl
            ,  coalesce(c.name_eu, c.name) as campaign_name_eu
            ,  coalesce(c.name_es, c.name) as campaign_name_es
            ,  coalesce(c.name_nl, c.name) as campaign_name_nl
            ,  coalesce(c.name_fr, c.name) as campaign_name_fr
            , c.year, c.previous_campaign_id
        , s.id as id_survey, s.created_at as survey_created_at, s.updated_at as survey_updated_at, s.status
        , m.id as id_method
            , m.name as method_name
            ,  coalesce(m.name_en, m.name) as method_name_en
            ,  coalesce(m.name_ca, m.name) as method_name_ca
            ,  coalesce(m.name_gl, m.name) as method_name_gl
            ,  coalesce(m.name_eu, m.name) as method_name_eu
            ,  coalesce(m.name_es, m.name) as method_name_es
            ,  coalesce(m.name_nl, m.name) as method_name_nl
            ,  coalesce(m.name_fr, m.name) as method_name_fr
            , m.description as method_description
            ,  coalesce(m.description_en, m.description) as method_description_en
            ,  coalesce(m.description_ca, m.description) as method_description_ca
            ,  coalesce(m.description_gl, m.description) as method_description_gl
            ,  coalesce(m.description_eu, m.description) as method_description_eu
            ,  coalesce(m.description_es, m.description) as method_description_es
            ,  coalesce(m.description_nl, m.description) as method_description_nl
            ,  coalesce(m.description_fr, m.description) as method_description_fr
        , u.id as id_user, u.name as user_name, u.surnames as user_surname, u.email as user_email
        , o.id as id_organization, o.name as organization_name, o.vat_number, o.logo as organization_logo --TODO afegir més camps
        , h.id as id_methods_section
            , h.title as method_section_title
            , coalesce(h.title_en, h.title) as method_section_title_en
            , coalesce(h.title_ca, h.title) as method_section_title_ca
            , coalesce(h.title_gl, h.title) as method_section_title_gl
            , coalesce(h.title_eu, h.title) as method_section_title_eu
            , coalesce(h.title_es, h.title) as method_section_title_es
            , coalesce(h.title_nl, h.title) as method_section_title_nl
            , coalesce(h.title_fr, h.title) as method_section_title_fr
            , h.order as method_order, h.lvl as method_level, h.path_order
        , si.sort_value
        , null::uuid as id_indicator, null as indicator_code
            , null as indicator_name
            , null as indicator_name_en, null as indicator_name_ca, null as indicator_name_gl
            , null as indicator_name_eu, null as indicator_name_es, null as indicator_name_nl
            , null as indicator_name_fr
            , null as indicator_description
            , null as indicator_description_en, null as indicator_description_ca
            , null as indicator_description_gl, null as indicator_description_eu
            , null as indicator_description_es, null as indicator_description_nl
            , null as indicator_description_fr
            , false as is_direct_indicator, null as indicator_category, null as indicator_data_type, null as indicator_unit
        , null::uuid as id_indicatorresult, null::int as gender, null as value
        , null::smallint as instance_number
        , null::uuid as id_project, null as project_name
        , null::varchar as g1_title
        , null::varchar as g1_title_en
        , null::varchar as g1_title_ca
        , null::varchar as g1_title_gl
        , null::varchar as g1_title_eu
        , null::varchar as g1_title_es
        , null::varchar as g1_title_nl
        , null::varchar as g1_title_fr
        , null::varchar as g2_title
        , null::varchar as g2_title_en
        , null::varchar as g2_title_ca
        , null::varchar as g2_title_gl
        , null::varchar as g2_title_eu
        , null::varchar as g2_title_es
        , null::varchar as g2_title_nl
        , null::varchar as g2_title_fr
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