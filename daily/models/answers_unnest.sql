{{ config(materialized='table'
, tags=[ "EC"]
, docs={'node_color': '#f09df3'}
) }}


with ans as (
    select coalesce(mfbp.form_block_index,mfb.form_block_index) as index, mf.question_index
    , q."ID", q."QUESTION_KEY"
    , case when (((q.name::jsonb)->>'texts')::jsonb->>0)::jsonb->>'la' ='ca' then
        (((q.name::jsonb)->>'texts')::jsonb->>0)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>1)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>1)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>2)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>2)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text'
        end
     as question_name
     , (((fb.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text'  as single_block_name
    ,(((fbp.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' as parent_block_name
    ,coalesce((((fbp.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text', (((fb.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' ) as block_name
    , a.value as value_origin
    , q."QUESTIONTYPE"
    , unnest( (string_to_array(replace(replace(a.value,'[',''),']',''),','))) as value
    , c.year
    , a.id_entity
    , m."MODULE_KEY"
    from {{ source('dwhec', 'questions')}} q
        join {{ source('dwhec', 'campaigns')}} c on q.id_campaign = c."ID"
        join {{ source('dwhec', 'answers')}}  a on a.id_question = q."ID"
        join {{ source('dwhec', 'module_form_block_question')}} mf on mf.id_question=q."ID"
        join {{ source('dwhec', 'module_form_block')}} mfb on mf.id_module_form_block=mfb."id"
        join {{ source('dwhec', 'form_blocks')}} fb on mfb.id_form_block=fb."ID"
        left join {{ source('dwhec', 'module_form_block')}} mfbp on mfb.id_parent=mfbp."id"
        left join {{ source('dwhec', 'form_blocks')}} fbp on mfbp.id_form_block=fbp."ID"
        --left join form_blocks fbp on fb."ID_PARENT"=fbp."ID"
        join {{ source('dwhec', 'modules')}} m on mfb.id_module=m."ID"
        join {{ source('dwhec', 'entity_module')}} em on em.id_module = m."ID" and em.id_entity =  a.id_entity
    where 1=1
--    and c.year =2024
--    and m."MODULE_KEY" ='BS-XES'
    --and q."QUESTIONTYPE"  in ('Gender', 'GenderDecimal')
    and q."QUESTIONTYPE" not in ('Gender', 'Number', 'Radio', 'Text', 'Boolean', 'GenderDecimal', 'Decimal', 'SingleText')
    and value is not null
    --and a.id_entity =2809
)


-- GENDER & GENDER DECIMAL
select coalesce(mfbp.form_block_index,mfb.form_block_index) as index, mf.question_index
    , q."ID", q."QUESTION_KEY"
    , case when (((q.name::jsonb)->>'texts')::jsonb->>0)::jsonb->>'la' ='ca' then
        (((q.name::jsonb)->>'texts')::jsonb->>0)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>1)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>1)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>2)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>2)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text'
        end
     as question_name
     , (((fb.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text'  as single_block_name
    ,(((fbp.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' as parent_block_name
    ,coalesce((((fbp.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text', (((fb.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' ) as block_name
    , a.value
    , q."QUESTIONTYPE"
    , unnest(case when q."QUESTIONTYPE" like 'Gender%' then (string_to_array(replace(replace(a.value,'[',''),']',''),',')) end) as value_number
    , null as value_text
    , null as value_boolean
    , unnest('{d,h,nb}'::varchar[]) as genders
    , c.year
    , a.id_entity
    , m."MODULE_KEY"
from {{ source('dwhec', 'questions')}} q
    join {{ source('dwhec', 'campaigns')}} c on q.id_campaign = c."ID"
    join {{ source('dwhec', 'answers')}}  a on a.id_question = q."ID"
    join {{ source('dwhec', 'module_form_block_question')}} mf on mf.id_question=q."ID"
    join {{ source('dwhec', 'module_form_block')}} mfb on mf.id_module_form_block=mfb."id"
    join {{ source('dwhec', 'form_blocks')}} fb on mfb.id_form_block=fb."ID"
    left join {{ source('dwhec', 'module_form_block')}} mfbp on mfb.id_parent=mfbp."id"
    left join {{ source('dwhec', 'form_blocks')}} fbp on mfbp.id_form_block=fbp."ID"
    --left join form_blocks fbp on fb."ID_PARENT"=fbp."ID"
    join {{ source('dwhec', 'modules')}} m on mfb.id_module=m."ID"
    join {{ source('dwhec', 'entity_module')}} em on em.id_module = m."ID" and em.id_entity =  a.id_entity
where 1=1
    --and c.year =2024
    --and m."MODULE_KEY" ='BS-XES'
    and q."QUESTIONTYPE"  in ('Gender', 'GenderDecimal')
    and value is not null
    --and a.id_entity =2809


union all
-- NUMBER RADIO DECIMAL TEXT SIGNLETEXT BOOLEAN
select coalesce(mfbp.form_block_index,mfb.form_block_index) as index, mf.question_index
    , q."ID", q."QUESTION_KEY"
    , case when (((q.name::jsonb)->>'texts')::jsonb->>0)::jsonb->>'la' ='ca' then
        (((q.name::jsonb)->>'texts')::jsonb->>0)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>1)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>1)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>2)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>2)::jsonb->>'text'
        when (((q.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'la' ='ca' then
            (((q.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text'
        end
     as question_name
     , (((fb.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text'  as single_block_name
    ,(((fbp.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' as parent_block_name
    ,coalesce((((fbp.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text', (((fb.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' ) as block_name
    , a.value as value_origin
    , q."QUESTIONTYPE"
    , case when q."QUESTIONTYPE" in ('Number', 'Decimal') then nullif(a.value,'N/A') end as value_number
    , case when q."QUESTIONTYPE" in ('Text', 'SingleText') then a.value  when q."QUESTIONTYPE" in ('Radio') then (((cu.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text' end as val_text
    , case when q."QUESTIONTYPE" in ('Boolean') then a.value end as val_boolean
    , null as genders
    , c.year
    , a.id_entity
    , m."MODULE_KEY"
from {{ source('dwhec', 'questions')}} q
    join {{ source('dwhec', 'campaigns')}} c on q.id_campaign = c."ID"
    join {{ source('dwhec', 'answers')}}  a on a.id_question = q."ID"
    join {{ source('dwhec', 'module_form_block_question')}} mf on mf.id_question=q."ID"
    join {{ source('dwhec', 'module_form_block')}} mfb on mf.id_module_form_block=mfb."id"
    join {{ source('dwhec', 'form_blocks')}} fb on mfb.id_form_block=fb."ID"
    left join {{ source('dwhec', 'module_form_block')}} mfbp on mfb.id_parent=mfbp."id"
    left join {{ source('dwhec', 'form_blocks')}} fbp on mfbp.id_form_block=fbp."ID"
    --left join form_blocks fbp on fb."ID_PARENT"=fbp."ID"
    join {{ source('dwhec', 'modules')}} m on mfb.id_module=m."ID"
    join {{ source('dwhec', 'entity_module')}} em on em.id_module = m."ID" and em.id_entity =  a.id_entity
    left join {{ source('dwhec', 'custom_list_item')}} cu on case when q."QUESTIONTYPE" in ('Radio') then a.value end=cu."ID"::varchar
where 1=1
    --and c.year =2024
    --and m."MODULE_KEY" ='BS-XES'
    and q."QUESTIONTYPE"  in ('Number','Radio', 'Decimal','Text', 'SingleText','Boolean')
    and a.value is not null
    --and a.id_entity =2809


union all
-- NoT NUMBER RADIO DECIMAL TEXT SIGNLETEXT BOOLEAN GENDER*
select
    a."index" , a.question_index , a."ID" , a."QUESTION_KEY" , a.question_name , a.single_block_name , a.parent_block_name , a.block_name
    , a.value_origin
    , a."QUESTIONTYPE"
    , null
    , coalesce((((c.name::jsonb)->>'texts')::jsonb->>3)::jsonb->>'text', a.value)
    , null
    , null
    , a."year" , a.id_entity
    , a."MODULE_KEY"
from ans a
    left join {{ source('dwhec', 'custom_list_item')}} c on a.value=c."ID"::varchar
where a.value not in ('')