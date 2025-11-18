
{% macro update_answers_calc_agg() %}


with vals as (
	select unnest(string_to_array(trim(both '[]' from str_value),',')) as val_id,*
	from {{ this }}
),
fin as (
select v.id_campaign, v.campaign_name, v."year", v.previous_campaign_id, v.id_survey, v.survey_created_at, v.survey_updated_at, v.status, v.id_method, v.active, v.method_name, v.method_description, v.id_user, v.user_name, v.user_surname, v.user_email, v.id_organization, v.organization_name, v.vat_number, v.id_methods_section, v.method_section_title, v.method_order, v.method_level, v.path_order, v.sort_value, v.id_indicator, v.indicator_code, v.indicator_name, v.indicator_description, v.is_direct_indicator, v.indicator_category, v.indicator_data_type, v.indicator_unit, v.gender, v.value, v.num_gender, v.str_gender
, '['||string_Agg('"'||l.title||'"',',')||']' as str_value
    , '['||string_Agg('"'||l.title_en||'"',',')||']' as str_value_en
    , '['||string_Agg('"'||l.title_ca||'"',',')||']' as str_value_ca
    , '['||string_Agg('"'||l.title_es||'"',',')||']' as str_value_es
    , '['||string_Agg('"'||l.title_eu||'"',',')||']' as str_value_eu
    , '['||string_Agg('"'||l.title_gl||'"',',')||']' as str_value_gl
    , '['||string_Agg('"'||l.title_nl||'"',',')||']' as str_value_nl
from vals v join {{ source('dwhpublic', 'syh_methods_listitem')}} l on v.val_id=l.id::text
group by v.id_campaign, v.campaign_name, v."year", v.previous_campaign_id, v.id_survey, v.survey_created_at, v.survey_updated_at, v.status, v.id_method, v.active, v.method_name, v.method_description, v.id_user, v.user_name, v.user_surname, v.user_email, v.id_organization, v.organization_name, v.vat_number, v.id_methods_section, v.method_section_title, v.method_order, v.method_level, v.path_order, v.sort_value, v.id_indicator, v.indicator_code, v.indicator_name, v.indicator_description, v.is_direct_indicator, v.indicator_category, v.indicator_data_type, v.indicator_unit, v.gender, v.value, v.num_gender, v.str_gender
)
update {{ this }} set str_value=f.str_value
    , str_value_en=f.str_value_en
    , str_value_ca=f.str_value_ca
    , str_value_es=f.str_value_es
    , str_value_eu=f.str_value_eu
    , str_value_gl=f.str_value_gl
    , str_value_nl=f.str_value_nl
from fin f
where f.id_campaign={{ this.table }}.id_campaign
	and f.id_survey={{ this.table }}.id_survey
	and f.id_method={{ this.table }}.id_method
	and f.id_organization={{ this.table }}.id_organization
	and f.id_methods_section={{ this.table }}.id_methods_section
	and f.id_indicator={{ this.table }}.id_indicator;

create index cix_answers_calc_agg on {{ this }} (id_campaign, id_organization);

CLUSTER {{ this }} USING cix_answers_calc_agg;



commit;



{% endmacro %}
