
{% macro update_answers_calc_agg() %}

create index cix_answers_calc_agg on {{ this }} (id_campaign, id_organization, id_method, id_methods_section, id_indicator, id_survey);

CLUSTER {{ this }} USING cix_answers_calc_agg;


with vals as (
	select unnest(string_to_array(trim(both '[]' from replace(str_value,'|',',')),',')) as val_id,*
	from {{ this }}
),
fin as (
select v.id_campaign, v.campaign_name, v."year", v.previous_campaign_id, v.id_survey, v.survey_created_at, v.survey_updated_at, v.status, v.id_method, v.method_name, v.method_description, v.id_user, v.user_name, v.user_surname, v.user_email, v.id_organization, v.organization_name, v.vat_number, v.id_methods_section, v.method_section_title, v.method_order, v.method_level, v.path_order, v.sort_value, v.id_indicator, v.indicator_code, v.indicator_name, v.indicator_description, v.is_direct_indicator, v.indicator_category, v.indicator_data_type, v.indicator_unit, v.gender, v.value, v.num_gender, v.str_gender
    , v.set_code, v.instance_number
, '['||string_Agg('"'||replace(l.title, '"', '')||'"',',')||']' as str_value
    , '['||string_Agg('"'||replace(l.title_en, '"', '')||'"',',')||']' as str_value_en
    , '['||string_Agg('"'||replace(l.title_ca, '"', '')||'"',',')||']' as str_value_ca
    , '['||string_Agg('"'||replace(l.title_es, '"', '')||'"',',')||']' as str_value_es
    , '['||string_Agg('"'||replace(l.title_eu, '"', '')||'"',',')||']' as str_value_eu
    , '['||string_Agg('"'||replace(l.title_gl, '"', '')||'"',',')||']' as str_value_gl
    , '['||string_Agg('"'||replace(l.title_nl, '"', '')||'"',',')||']' as str_value_nl
    , '['||string_Agg('"'||replace(l.title_fr, '"', '')||'"',',')||']' as str_value_fr
from vals v join {{ source('dwhpublic', 'syh_methods_listitem')}} l on v.val_id=l.id::text
group by v.id_campaign, v.campaign_name, v."year", v.previous_campaign_id, v.id_survey, v.survey_created_at, v.survey_updated_at, v.status, v.id_method, v.method_name, v.method_description, v.id_user, v.user_name, v.user_surname, v.user_email, v.id_organization, v.organization_name, v.vat_number, v.id_methods_section, v.method_section_title, v.method_order, v.method_level, v.path_order, v.sort_value, v.id_indicator, v.indicator_code, v.indicator_name, v.indicator_description, v.is_direct_indicator, v.indicator_category, v.indicator_data_type, v.indicator_unit, v.gender, v.value, v.num_gender, v.str_gender
    , v.set_code, v.instance_number
)
update {{ this }} set str_value=f.str_value
    , str_value_en=f.str_value_en
    , str_value_ca=f.str_value_ca
    , str_value_es=f.str_value_es
    , str_value_eu=f.str_value_eu
    , str_value_gl=f.str_value_gl
    , str_value_nl=f.str_value_nl
    , str_value_fr=f.str_value_fr
from fin f
where f.id_campaign={{ this.table }}.id_campaign
	and f.id_survey={{ this.table }}.id_survey
	and f.id_method={{ this.table }}.id_method
	and f.id_organization={{ this.table }}.id_organization
	and f.set_code={{ this.table }}.set_code
	and coalesce(f.instance_number,-1)=coalesce({{ this.table }}.instance_number, -1)
	-- uuid random per als que no tenen section
	and coalesce(f.id_methods_section,'2aa162df-864d-4d58-8924-2d6fab577017'::uuid)=coalesce({{ this.table }}.id_methods_section, null,'2aa162df-864d-4d58-8924-2d6fab577017'::uuid)
	and f.id_indicator={{ this.table }}.id_indicator;



with val as (
select  i.code, sml.title, sml.title_ca , sml.title_es , sml.title_gl , sml.title_eu , sml.title_en , sml.title_fr, sml.title_nl
from  {{ source('dwhpublic', 'syh_methods_indicator')}} i
	join {{ source('dwhpublic', 'syh_methods_list')}} l on i.list_options_id = l.id and i.data_type in ('CH', 'R', 'DR')
	join {{ source('dwhpublic', 'syh_methods_list_items')}} li on l.id = li.list_id
	join {{ source('dwhpublic', 'syh_methods_listitem')}} sml on li.listitem_id = sml.id
)
, new_list_val as (
select id_organization, id_campaign, id_method, a.id_project, a.id_survey, indicator_code
, '['||string_Agg('"'||replace(val.title, '"', '')||'"',',')||']'  as list_string
, '['||string_Agg('"'||replace(val.title_ca, '"', '')||'"',',')||']'  as list_string_ca
, '['||string_Agg('"'||replace(val.title_es, '"', '')||'"',',')||']'  as list_string_es
, '['||string_Agg('"'||replace(val.title_en, '"', '')||'"',',')||']'  as list_string_en
, '['||string_Agg('"'||replace(val.title_gl, '"', '')||'"',',')||']'  as list_string_gl
, '['||string_Agg('"'||replace(val.title_eu, '"', '')||'"',',')||']'  as list_string_eu
, '['||string_Agg('"'||replace(val.title_nl, '"', '')||'"',',')||']'  as list_string_nl
, '['||string_Agg('"'||replace(val.title_fr, '"', '')||'"',',')||']'  as list_string_fr
,'['||string_Agg('"'||case when concat('%', str_value, '%') like concat('%"', val.title, '"%') then '✅' else '❌' end||'"',',')||']'  as list_value
from {{ this }} a
join val on a.indicator_code = val.code
group by id_organization, id_campaign, id_method, a.id_project, a.id_survey, indicator_code
)
update {{ this }} set
	str_list = list_string
	, str_list_ca = list_string_ca
	, str_list_es = list_string_es
	, str_list_gl = list_string_gl
	, str_list_eu = list_string_eu
	, str_list_en = list_string_en
	, str_list_fr = list_string_fr
	, str_list_nl = list_string_nl
	, str_value=list_value
	, str_value_ca=list_value
	, str_value_es=list_value
	, str_value_gl=list_value
	, str_value_eu=list_value
	, str_value_en=list_value
	, str_value_fr=list_value
	, str_value_nl=list_value
from new_list_val n
where n.id_organization = {{this.table}}.id_organization
and n.id_campaign = {{this.table}}.id_campaign
and n.id_method = {{this.table}}.id_method
and (n.id_project = {{this.table}}.id_project or (n.id_project is null and {{this.table}}.id_project is null))
and n.id_survey = {{this.table}}.id_survey
and n.indicator_code = {{this.table}}.indicator_code;



commit;



{% endmacro %}
