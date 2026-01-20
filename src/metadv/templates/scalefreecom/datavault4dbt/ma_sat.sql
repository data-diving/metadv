{%- set source_model = '${source_model}' -%}
{%- set target_name = '${target_name}' -%}
{%- set payload_columns = ${payload_columns} -%}
{%- set ma_key_columns = ${ma_key_columns} -%}

{{ datavault4dbt.ma_sat_v0(
    source_model=source_model,
    parent_hashkey=target_name ~ '_hk',
    src_hashdiff=target_name ~ '_hashdiff',
    src_payload=payload_columns,
    src_ma_key=ma_key_columns,
    src_ldts='ldts',
    src_rsrc='rsrc'
) }}
