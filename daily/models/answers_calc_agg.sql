{{ config(materialized='table'
, tags=[ "SYH"]
, docs={'node_color': '#79964D'}
, post_hook=after_commit("{{ update_answers_calc_agg() }}")
) }}

select id_campaign, campaign_name, "year", previous_campaign_id, id_survey, survey_created_at, survey_updated_at, status, id_method, active, method_name, method_description, id_user, user_name, user_surname, user_email, id_organization, organization_name, vat_number, id_methods_section, method_section_title, method_order, method_level, path_order, sort_value, id_indicator, project_id, indicator_name, indicator_description, is_direct_indicator, indicator_category, indicator_data_type, indicator_unit
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
from {{ref('data_results')}}
group by id_campaign, campaign_name, "year", previous_campaign_id, id_survey, survey_created_at, survey_updated_at, status, id_method, active, method_name, method_description, id_user, user_name, user_surname, user_email, id_organization, organization_name, vat_number, id_methods_section, method_section_title, method_order, method_level, path_order, sort_value, id_indicator, project_id, indicator_name, indicator_description, is_direct_indicator, indicator_category, indicator_data_type, indicator_unit


