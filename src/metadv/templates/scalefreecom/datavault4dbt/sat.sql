{%- set source_model = '${source_model}' -%}
{%- set entity_name = '${entity_name}' -%}
{%- set payload_columns = ${payload_columns} -%}

{{ datavault4dbt.sat_v0(
    source_model=source_model,
    parent_hashkey=entity_name ~ '_hk',
    src_hashdiff=entity_name ~ '_hashdiff',
    src_payload=payload_columns,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
