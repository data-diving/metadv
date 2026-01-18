{%- set source_models = ${source_models} -%}
{%- set link_name = '${link_name}' -%}
{%- set fk_columns = ${fk_columns} -%}

{%- set yaml_metadata -%}
source_model:
{% for model in source_models %}
  - {{ model }}
{% endfor %}
src_pk: {{ link_name }}_hk
src_fk:
{% for fk in fk_columns %}
  - {{ fk }}
{% endfor %}
src_ldts: load_dt
src_source: record_source
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(
    src_pk=metadata_dict["src_pk"],
    src_fk=metadata_dict["src_fk"],
    src_ldts=metadata_dict["src_ldts"],
    src_source=metadata_dict["src_source"],
    source_model=metadata_dict["source_model"]
) }}
