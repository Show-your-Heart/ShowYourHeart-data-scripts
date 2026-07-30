
{% macro create_index_answers_calc() %}

{% if not is_incremental() %}
create index cix_answers_calc on {{ this }} (id_campaign,  id_survey, id_method, id_user, id_organization, id_project
    , id_methods_section, id_indicator, indicator_code, is_direct_indicator);

CLUSTER {{ this }} USING cix_answers_calc;
{% endif %}


-- calculo la jerarquia de cada pregunta
with recursive method_section_hieriarchy as (
select distinct s.id
    , s.title, s.title_en, s.title_ca, s.title_gl, s.title_eu, s.title_es, s.title_nl, s.title_fr
    , s.order, s.method_id, s.parent_id, 1 as lvl, cast(s.order as text) as path_order
from {{ source('dwhpublic', 'syh_methods_section')}} s
where parent_id is null
union all
select distinct s.id
    , s.title, s.title_en, s.title_ca, s.title_gl, s.title_eu, s.title_es, s.title_nl, s.title_fr
    , s.order, s.method_id, s.parent_id, ms.lvl+1 as lvl, ms.path_order || '.' || lpad(s.order::varchar,2,'0') AS path_order
from {{ source('dwhpublic', 'syh_methods_section')}} s
	join method_section_hieriarchy ms on ms.id=s.parent_id
)
update {{ this }}  set id_methods_section=h.id
        , method_section_title=h.title
        , method_section_title_en=coalesce(h.title_en,h.title)
        , method_section_title_ca=coalesce(h.title_ca,h.title)
        , method_section_title_gl=coalesce(h.title_gl,h.title)
        , method_section_title_eu=coalesce(h.title_eu,h.title)
        , method_section_title_es=coalesce(h.title_es,h.title)
        , method_section_title_nl=coalesce(h.title_nl,h.title)
        , method_section_title_fr=coalesce(h.title_fr,h.title)
        , method_order=h."order"
        , method_level=h.lvl
        , path_order=h.path_order
        , sort_value=si.sort_value
from method_section_hieriarchy h
join syh_methods_section_indicators si on si.section_id=h.id
where {{ this.table }}.id_indicator=si.indicator_id and {{ this.table }}.id_method=h.method_id
    and id_methods_section is null;

-- TODO afegir les jerarquies sense pregunta (per cada campanya/mètode/organització

commit;




{% endmacro %}
