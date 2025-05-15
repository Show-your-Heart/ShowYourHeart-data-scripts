{{ config(materialized='incremental'
, unique_key='answer_year'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}


with ans as (
    select coalesce(mfbp.form_block_index,mfb.form_block_index) as index, mf.question_index
    , q."ID", q."QUESTION_KEY"
    , jsonb_path_query(q.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as question_name
    , jsonb_path_query(fb.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as single_block_name
    , jsonb_path_query(fbp.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as parent_block_name
    , case when fbp.valca is null
        then fbp.vales
        else fbp.valca
     end as block_name
    , a.value as value_origin
    , q."QUESTIONTYPE"
    , unnest( (string_to_array(replace(replace(a.value,'[',''),']',''),','))) as value
    , c.year
    , c."ID" as id_campaign
    , a.id_entity
    , m."MODULE_KEY"
    , m."ID" as id_module
    from {{ source('dwhec', 'questions')}} q
        join {{ source('dwhec', 'campaigns')}} c on q.id_campaign = c."ID"
        join {{ ref('aux_answers')}}  a on a.id_question = q."ID"
        join {{ source('dwhec', 'module_form_block_question')}} mf on mf.id_question=q."ID"
        join {{ source('dwhec', 'module_form_block')}} mfb on mf.id_module_form_block=mfb."id"
        join {{ source('dwhec', 'form_blocks')}} fb on mfb.id_form_block=fb."ID"
        left join {{ source('dwhec', 'module_form_block')}} mfbp on mfb.id_parent=mfbp."id"
        left join {{ ref('aux_form_blocks') }} fbp on mfbp.id_form_block=fbp."ID"
        --left join form_blocks fbp on fb."ID_PARENT"=fbp."ID"
        join {{ source('dwhec', 'modules')}} m on mfb.id_module=m."ID"
        join {{ source('dwhec', 'entity_module')}} em on em.id_module = m."ID" and em.id_entity =  a.id_entity
    where 1=1

    {% if is_incremental() %}
    and c.year  >= date_part('year', current_date-300)
    {% endif %}

--    and m."MODULE_KEY" ='BS-XES'
    --and q."QUESTIONTYPE"  in ('Gender', 'GenderDecimal')
    and q."QUESTIONTYPE" not in ('Gender', 'Number', 'Radio', 'Text', 'Boolean', 'GenderDecimal', 'Decimal', 'SingleText')
    and value is not null
    --and a.id_entity =2809
)


-- GENDER & GENDER DECIMAL
select coalesce(mfbp.form_block_index,mfb.form_block_index) as answer_form_blocindex
    , mf.question_index  as answer_question_index
    , q."ID" as answer_question_id, q."QUESTION_KEY" as answer_question_key
    , answer_question_name as answer_question_name
    , fb.valca  as answer_single_block_name
    , fbp.valca as answer_parent_block_name
    , case when fbp.valca is null
        then fbp.vales
        else fbp.valca
     end as answer_block_name
    , a.value as answer_value
    , q."QUESTIONTYPE" as answer_question_type
    , unnest(case when q."QUESTIONTYPE" like 'Gender%' then (string_to_array(replace(replace(a.value,'[',''),']',''),',')) end) as answer_value_number
    , null as answer_value_text
    , null as answer_value_boolean
    , unnest('{d,h,a}'::varchar[]) as answer_genders
    , c.year as answer_year
    , c."ID" as id_campaign
    , a.id_entity as id_entity
    , m."MODULE_KEY"  as answer_module_key
    , m."ID" as answer_id_module
from (select *
        , jsonb_path_query(q.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as answer_question_name
        from {{ source('dwhec', 'questions')}} q
        ) q
    join {{ source('dwhec', 'campaigns')}} c on q.id_campaign = c."ID"
    join {{ ref('aux_answers')}}  a on a.id_question = q."ID"
    join {{ source('dwhec', 'module_form_block_question')}} mf on mf.id_question=q."ID"
    join {{ source('dwhec', 'module_form_block')}} mfb on mf.id_module_form_block=mfb."id"
    join {{ ref('aux_form_blocks') }} fb on mfb.id_form_block=fb."ID"
    left join {{ source('dwhec', 'module_form_block')}} mfbp on mfb.id_parent=mfbp."id"
    left join {{ ref('aux_form_blocks') }} fbp on mfbp.id_form_block=fbp."ID"
    --left join form_blocks fbp on fb."ID_PARENT"=fbp."ID"
    join {{ source('dwhec', 'modules')}} m on mfb.id_module=m."ID"
    join {{ source('dwhec', 'entity_module')}} em on em.id_module = m."ID" and em.id_entity =  a.id_entity
where 1=1

    {% if is_incremental() %}
    and c.year >= date_part('year', current_date-300)
    {% endif %}

    --and m."MODULE_KEY" ='BS-XES'
    and q."QUESTIONTYPE"  in ('Gender', 'GenderDecimal')
    and value is not null
    --and a.id_entity =2809


union all
-- NUMBER RADIO DECIMAL TEXT SIGNLETEXT BOOLEAN
select coalesce(mfbp.form_block_index,mfb.form_block_index) as index, mf.question_index
    , q."ID", q."QUESTION_KEY"
    , jsonb_path_query(q.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as answer_question_name
    , jsonb_path_query(fb.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}'  as answer_single_block_name
    , jsonb_path_query(fbp.name::jsonb, '$.texts[*] ? (@.la == "ca").text') #>> '{}' as answer_parent_block_name
    , case when fbp.valca is null
        then fbp.vales
        else fbp.valca
     end as answer_block_name
    , a.value as value_origin
    , q."QUESTIONTYPE"
    , case when q."QUESTIONTYPE" in ('Number', 'Decimal') and  a.value not like '%E%' then
        nullif(nullif(nullif(a.value,'N/A'),'[,,,]'),'')
     end as value_number
    , case when q."QUESTIONTYPE" in ('Text', 'SingleText') then a.value
        when q."QUESTIONTYPE" in ('Radio') then
        case when
            cu.valca=''
            then
                cu.vales
            else
                cu.valca
            end
      end as val_text
    , case when q."QUESTIONTYPE" in ('Boolean') then a.value end as val_boolean
    , null as genders
    , c.year
    , c."ID" as id_campaign
    , a.id_entity
    , m."MODULE_KEY"
    , m."ID" as answer_id_module
from {{ source('dwhec', 'questions')}} q
    join {{ source('dwhec', 'campaigns')}} c on q.id_campaign = c."ID"
    join {{ ref('aux_answers')}}  a on a.id_question = q."ID"
    join {{ source('dwhec', 'module_form_block_question')}} mf on mf.id_question=q."ID"
    join {{ source('dwhec', 'module_form_block')}} mfb on mf.id_module_form_block=mfb."id"
    join {{ source('dwhec', 'form_blocks')}} fb on mfb.id_form_block=fb."ID"
    left join {{ source('dwhec', 'module_form_block')}} mfbp on mfb.id_parent=mfbp."id"
    left join {{ ref('aux_form_blocks') }} fbp on mfbp.id_form_block=fbp."ID"
    --left join form_blocks fbp on fb."ID_PARENT"=fbp."ID"
    join {{ source('dwhec', 'modules')}} m on mfb.id_module=m."ID"
    join {{ source('dwhec', 'entity_module')}} em on em.id_module = m."ID" and em.id_entity =  a.id_entity
    left join {{ ref('aux_custom_list_item')}} cu on case when q."QUESTIONTYPE" in ('Radio') then a.value end=cu."ID"::varchar
where 1=1

    {% if is_incremental() %}
    and c.year  >= date_part('year', current_date-300)
    {% endif %}

    --and m."MODULE_KEY" ='BS-XES'
    and q."QUESTIONTYPE"  in ('Number','Radio', 'Decimal','Text', 'SingleText','Boolean')
    and a.value is not null
    --and a.id_entity =2809


union all
-- NOT NUMBER RADIO DECIMAL TEXT SIGNLETEXT BOOLEAN GENDER*
select
    a."index" , a.question_index , a."ID" , a."QUESTION_KEY" , a.question_name , a.single_block_name , a.parent_block_name , a.block_name
    , a.value_origin
    , a."QUESTIONTYPE"
    , null
    , coalesce(c.valca, c.vales, a.value)
    , null
    , null
    , a."year"
    , a.id_campaign
    , a.id_entity
    , a."MODULE_KEY"
    , a."ID" as id_module
from ans a
    left join {{ ref('aux_custom_list_item')}} c on a.value=c."ID"::varchar
where a.value not in ('')