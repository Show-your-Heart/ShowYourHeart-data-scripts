{{ config(materialized='table'
, tags=[ "SYH"]
, docs={'node_color': '#C93314'}
, post_hook=after_commit("{{ update_answers_calc_agg() }}")
) }}

select id_campaign
    , max(campaign_name) as campaign_name
    , max(campaign_name_en) as campaign_name_en
    , max(campaign_name_ca) as campaign_name_ca
    , max(campaign_name_es) as campaign_name_es
    , max(campaign_name_eu) as campaign_name_eu
    , max(campaign_name_gl) as campaign_name_gl
    , max(campaign_name_nl) as campaign_name_nl
    , max(campaign_name_fr) as campaign_name_fr
    , max("year") as "year", max(previous_campaign_id::varchar)::uuid as previous_campaign_id
    , id_survey, max(survey_created_at) as survey_created_at, max(survey_updated_at) as survey_updated_at
    , max(status) as status
    , id_method
    , max(method_name) as method_name
    , max(method_name_en) as method_name_en
    , max(method_name_ca) as method_name_ca
    , max(method_name_es) as method_name_es
    , max(method_name_eu) as method_name_eu
    , max(method_name_gl) as method_name_gl
    , max(method_name_nl) as method_name_nl
    , max(method_name_fr) as method_name_fr
    , max(method_description) as method_description
    , max(method_description_en) as method_description_en
    , max(method_description_ca) as method_description_ca
    , max(method_description_es) as method_description_es
    , max(method_description_eu) as method_description_eu
    , max(method_description_gl) as method_description_gl
    , max(method_description_nl) as method_description_nl
    , max(method_description_fr) as method_description_fr
    , id_user, max(user_name) as user_name, max(user_surname) as user_surname, max(user_email) as user_email
    , id_organization, max(organization_name) as organization_name, max(vat_number) as vat_number
    , id_project, max(project_name) as project_name
    , id_methods_section
    , max(method_section_title) as method_section_title
    , max(method_section_title_en) as method_section_title_en
    , max(method_section_title_ca) as method_section_title_ca
    , max(method_section_title_es) as method_section_title_es
    , max(method_section_title_eu) as method_section_title_eu
    , max(method_section_title_gl) as method_section_title_gl
    , max(method_section_title_nl) as method_section_title_nl
    , max(method_section_title_fr) as method_section_title_fr
    , max(method_order) as method_order, max(method_level) as method_level, max(path_order) as path_order
    , max(sort_value) as sort_value
    , id_indicator, indicator_code
    , max(indicator_name) as indicator_name
    , max(indicator_name_en) as indicator_name_en
    , max(indicator_name_ca) as indicator_name_ca
    , max(indicator_name_es) as indicator_name_es
    , max(indicator_name_eu) as indicator_name_eu
    , max(indicator_name_gl) as indicator_name_gl
    , max(indicator_name_nl) as indicator_name_nl
    , max(indicator_name_fr) as indicator_name_fr
    , max(indicator_description) as indicator_description
    , max(indicator_description_en) as indicator_description_en
    , max(indicator_description_ca) as indicator_description_ca
    , max(indicator_description_es) as indicator_description_es
    , max(indicator_description_eu) as indicator_description_eu
    , max(indicator_description_gl) as indicator_description_gl
    , max(indicator_description_nl) as indicator_description_nl
    , max(indicator_description_fr) as indicator_description_fr
    , is_direct_indicator as is_direct_indicator, max(indicator_category) as indicator_category
    , max(indicator_data_type) as indicator_data_type, max(indicator_unit) as indicator_unit
    , array_agg(
        case gender when 0 then 'Home'
            when 1 then 'Dona'
            when 2 then 'N/B'
            end
            ORDER BY gender
    ) as gender
    , array_agg(value order by gender) as value
    , count(distinct gender) as num_gender
    , case when count(distinct gender)>0 then '['||string_agg(case gender when 0 then '"Home"'
            when 1 then '"Dona"'
            when 2 then '"N/B"'
            end::varchar,',' order by gender)||']' end as str_gender
    , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value
        , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value_en
        , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value_ca
        , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value_es
        , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value_eu
        , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value_gl
        , case when count(distinct gender)>0 then '['||string_agg(value,',' order by gender)||']' else string_agg(value,'') end as str_value_nl
from {{ref('answers_calc')}}
group by id_campaign,  id_survey, id_method, id_user, id_organization, id_project
    , id_methods_section, id_indicator, indicator_code, is_direct_indicator
