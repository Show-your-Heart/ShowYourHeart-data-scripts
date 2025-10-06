{{ config(materialized='table'
, tags=[ "SYH"]
, docs={'node_color': '#C93314'}
, post_hook=after_commit("{{ update_answers_calc_agg() }}")
) }}

select id_campaign
, campaign_name
, campaign_name_en
, campaign_name_ca
, campaign_name_es
, campaign_name_eu
, campaign_name_gl
, campaign_name_nl
, "year", previous_campaign_id
, id_survey, survey_created_at, survey_updated_at, status
, id_method, active
, method_name
, method_name_en
, method_name_ca
, method_name_es
, method_name_eu
, method_name_gl
, method_name_nl
, method_description
, method_description_en
, method_description_ca
, method_description_es
, method_description_eu
, method_description_gl
, method_description_nl
, id_user, user_name, user_surname, user_email
, id_organization, organization_name, vat_number
, id_methods_section
, method_section_title
, method_section_title_en
, method_section_title_ca
, method_section_title_es
, method_section_title_eu
, method_section_title_gl
, method_section_title_nl
, method_order, method_level, path_order, sort_value
, id_indicator, project_id
, indicator_name
, indicator_name_en
, indicator_name_ca
, indicator_name_es
, indicator_name_eu
, indicator_name_gl
, indicator_name_nl
, indicator_description
, indicator_description_en
, indicator_description_ca
, indicator_description_es
, indicator_description_eu
, indicator_description_gl
, indicator_description_nl
, is_direct_indicator, indicator_category
, indicator_data_type, indicator_unit
, array_agg(
	case gender when 0 then 'Home'
		when 1 then 'Dona'
		when 2 then 'N/B'
		end
) as gender, array_agg(value) as value, count(distinct gender) as num_gender
, case when count(distinct gender)>0 then '['||string_agg(case gender when 0 then '"Home"'
		when 1 then '"Dona"'
		when 2 then '"N/B"'
		end::varchar,',')||']' end as str_gender
, case when count(distinct gender)>0 then '['||string_agg(value,',')||']' else string_agg(value,'') end as str_value
from {{ref('answers_calc')}}
group by id_campaign, campaign_name, "year", previous_campaign_id, id_survey, survey_created_at, survey_updated_at, status, id_method, active, method_name, method_description, id_user, user_name, user_surname, user_email, id_organization, organization_name, vat_number, id_methods_section, method_section_title, method_order, method_level, path_order, sort_value, id_indicator, project_id, indicator_name, indicator_description, is_direct_indicator, indicator_category, indicator_data_type, indicator_unit
    , campaign_name_en
    , campaign_name_ca
    , campaign_name_es
    , campaign_name_eu
    , campaign_name_gl
    , campaign_name_nl
    , method_name_en
    , method_name_ca
    , method_name_es
    , method_name_eu
    , method_name_gl
    , method_name_nl
    , method_description_en
    , method_description_ca
    , method_description_es
    , method_description_eu
    , method_description_gl
    , method_description_nl
    , method_section_title_en
    , method_section_title_ca
    , method_section_title_es
    , method_section_title_eu
    , method_section_title_gl
    , method_section_title_nl
    , indicator_name
    , indicator_name_en
    , indicator_name_ca
    , indicator_name_es
    , indicator_name_eu
    , indicator_name_gl
    , indicator_name_nl
    , indicator_description
    , indicator_description_en
    , indicator_description_ca
    , indicator_description_es
    , indicator_description_eu
    , indicator_description_gl
    , indicator_description_nl
