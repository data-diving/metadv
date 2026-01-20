{%- set source_model = '${source_model}' -%}
{%- set target_name = '${target_name}' -%}
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

{%- set yaml_metadata -%}
source_model: '{{ source_model }}'
src_pk: {{ target_name }}_hk
src_hashdiff: {{ target_name }}_hashdiff
src_payload:
{% for col in payload_columns %}
  - {{ col }}
{% endfor %}
src_cdk:
{% for col in multiactive_key_columns %}
  - {{ col }}
{% endfor %}
src_ldts: load_dt
src_source: record_source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ma_sat(
    src_pk=metadata_dict["src_pk"],
    src_cdk=metadata_dict["src_cdk"],
    src_hashdiff=metadata_dict["src_hashdiff"],
    src_payload=metadata_dict["src_payload"],
    src_ldts=metadata_dict["src_ldts"],
    src_source=metadata_dict["src_source"],
    source_model=metadata_dict["source_model"]
) }}
