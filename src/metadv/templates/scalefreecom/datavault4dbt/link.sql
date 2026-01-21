{%- set source_refs = ${source_refs} -%}
{%- set link_name = '${link_name}' -%}
{%- set fk_columns = ${fk_columns} -%}

{%- set source_models = [] -%}
{%- for ref in source_refs | map(attribute='source') | unique -%}
    {%- do source_models.append({'name': 'stg_' ~ ref}) -%}
{%- endfor -%}

{{ datavault4dbt.link(
    link_hashkey=link_name ~ '_hk',
    foreign_hashkeys=fk_columns,
    source_models=source_models,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
