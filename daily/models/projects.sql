{{ config(materialized='table'
, tags=[ "SYH"]
, docs={'node_color': '#C93314'}
) }}


select
	 pr.id
	, pr.name, pr.description, pr.start_date, pr.main_action_scope, pr.secondary_action_scope
	, l.name as legal_structure
	, l.name_en as legal_structure_en
	, l.name_ca as legal_structure_ca
	, l.name_es as legal_structure_es
	, l.name_gl as legal_structure_gl
	, l.name_eu as legal_structure_eu
	, l.name_fr as legal_structure_fr
	, l.name_nl as legal_structure_nl
	, l2.name as secondary_legal_structure
	, l2.name_en as secondary_legal_structure_en
	, l2.name_ca as secondary_legal_structure_ca
	, l2.name_es as secondary_legal_structure_es
	, l2.name_gl as secondary_legal_structure_gl
	, l2.name_eu as secondary_legal_structure_eu
	, l2.name_fr as secondary_legal_structure_fr
	, l2.name_nl as secondary_legal_structure_nl
	, pr.contact_name
	, pr.contact_email
	, pr.contact_telephone
from  {{ source('dwhpublic', 'syh_organizations_project')}} pr
left join  {{ source('dwhpublic', 'syh_settings_legalstructure')}} l on pr.main_legal_entity_type = l.id::varchar
left join  {{ source('dwhpublic', 'syh_settings_legalstructure')}} l2 on pr.secondary_legal_entity_type = l2.id::varchar
