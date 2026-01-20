{%- set source_refs = ${source_refs} -%}
{%- set entity_name = '${entity_name}' -%}

{%- set source_models = [] -%}
{%- for ref in source_refs | map(attribute='source') | unique -%}
    {%- do source_models.append({'name': 'stg_' ~ ref}) -%}
{%- endfor -%}

{{ datavault4dbt.hub(
    hashkey=entity_name ~ '_hk',
    business_keys=[entity_name ~ '_id'],
    source_models=source_models,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
