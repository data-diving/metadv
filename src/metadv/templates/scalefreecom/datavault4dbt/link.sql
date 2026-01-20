{%- set source_model_names = ${source_models} -%}
{%- set link_name = '${link_name}' -%}
{%- set fk_columns = ${fk_columns} -%}

{%- set source_models = [] -%}
{%- for name in source_model_names -%}
    {%- do source_models.append({'name': name}) -%}
{%- endfor -%}

{{ datavault4dbt.link(
    link_hashkey=link_name ~ '_hk',
    foreign_hashkeys=fk_columns,
    source_models=source_models,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
