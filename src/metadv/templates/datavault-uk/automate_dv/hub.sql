{%- set source_refs = ${source_refs} -%}
{%- set entity_name = '${entity_name}' -%}

{%- set yaml_metadata -%}
source_model:
{% for ref in source_refs | map(attribute='source') | unique %}
  - stg_{{ ref }}
{% endfor %}
src_pk: {{ entity_name }}_hk
src_nk:
  - {{ entity_name }}_id
src_ldts: load_dt
src_source: record_source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(
    src_pk=metadata_dict["src_pk"],
    src_nk=metadata_dict["src_nk"],
    src_ldts=metadata_dict["src_ldts"],
    src_source=metadata_dict["src_source"],
    source_model=metadata_dict["source_model"]
) }}
