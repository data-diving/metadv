{%- set source_model = '${source_model}' -%}
{%- set entity_name = '${entity_name}' -%}
{%- set attributes = ${attributes} -%}

{#- Derive payload and multiactive key columns from attributes -#}
{%- set payload_columns = [] -%}
{%- set multiactive_key_columns = [] -%}
{%- for attr in attributes -%}
    {%- if attr.multiactive_key -%}
        {%- do multiactive_key_columns.append(attr.column) -%}
    {%- else -%}
        {%- do payload_columns.append(attr.column) -%}
    {%- endif -%}
{%- endfor -%}

{{ datavault4dbt.ma_sat_v0(
    source_model=source_model,
    parent_hashkey=entity_name ~ '_hk',
    src_hashdiff=entity_name ~ '_hashdiff',
    src_payload=payload_columns,
    src_ma_key=multiactive_key_columns,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
