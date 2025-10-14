
{% macro create_index_answers_calc() %}

{% if not is_incremental() %}
create index cix_answers_calc on {{ this }} (id_campaign, id_survey, id_method, id_user, id_organization
, id_methods_section, id_indicator, indicator_code
);

CLUSTER {{ this }} USING cix_answers_calc;


commit;

{% endif %}


{% endmacro %}
