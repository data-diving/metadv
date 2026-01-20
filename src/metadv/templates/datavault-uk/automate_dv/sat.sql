{%- set source_model = '${source_model}' -%}
{%- set entity_name = '${entity_name}' -%}
{%- set payload_columns = ${payload_columns} -%}

{%- set yaml_metadata -%}
source_model: '{{ source_model }}'
src_pk: {{ entity_name }}_hk
src_hashdiff: {{ entity_name }}_hashdiff
src_payload:
{% for col in payload_columns %}
  - {{ col }}
{% endfor %}
src_ldts: load_dt
src_source: record_source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(
    src_pk=metadata_dict["src_pk"],
    src_hashdiff=metadata_dict["src_hashdiff"],
    src_payload=metadata_dict["src_payload"],
    src_ldts=metadata_dict["src_ldts"],
    src_source=metadata_dict["src_source"],
    source_model=metadata_dict["source_model"]
) }}
