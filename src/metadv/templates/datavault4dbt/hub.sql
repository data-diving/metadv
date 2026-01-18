{%- set source_model_names = ${source_models} -%}
{%- set entity_name = '${entity_name}' -%}

{%- set source_models = [] -%}
{%- for name in source_model_names -%}
    {%- do source_models.append({'name': name}) -%}
{%- endfor -%}

{{ datavault4dbt.hub(
    hashkey=entity_name ~ '_hk',
    business_keys=[entity_name ~ '_id'],
    source_models=source_models,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
