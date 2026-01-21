{%- set relation_name = '${relation_name}' -%}
{%- set entities = ${entities} -%}
{%- set source_refs = ${source_refs} -%}

{%- set sources = source_refs | map(attribute='source') | unique | list -%}

{%- set is_self_ref = (entities | length) != (entities | unique | list | length) -%}

{%- set source_entity_columns = {} -%}
{%- set source_attributes = {} -%}
{%- for src_ref in source_refs -%}
    {%- do source_entity_columns.update({src_ref['source']: src_ref.get('entity_columns', {})}) -%}
    {%- do source_attributes.update({src_ref['source']: src_ref.get('attributes', [])}) -%}
{%- endfor -%}

{%- set all_attrs = [] -%}
{%- for src_ref in source_refs -%}
    {%- for attr in src_ref.get('attributes', []) -%}
        {%- if attr not in all_attrs -%}
            {%- do all_attrs.append(attr) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{% if sources | length == 1 %}
{%- set source = sources[0] -%}
{%- set entity_cols = source_entity_columns[source] -%}
{%- set attrs = source_attributes[source] -%}
{%- set entity_col_idx = {} -%}
SELECT
{% for entity in entities %}
    {%- if is_self_ref -%}
        {%- set col_list = entity_cols.get(entity, []) -%}
        {%- set idx = entity_col_idx.get(entity, 0) -%}
        {%- set src_col = col_list[idx] if idx < (col_list | length) else entity ~ '_id' -%}
        {%- do entity_col_idx.update({entity: idx + 1}) -%}
        {%- set seq = entities[:loop.index] | select('equalto', entity) | list | length -%}
    {{ src_col }} AS {{ entity }}_{{ seq }}_id,
    {%- else %}
    {{ entity_cols.get(entity, [entity ~ '_id'])[0] }} AS {{ entity }}_id,
    {%- endif %}
{% endfor %}
{% for attr in attrs %}
    {{ attr }}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ ref(source) }}

{% else %}
{%- for source in sources %}
{%- set entity_cols = source_entity_columns[source] -%}
{%- set src_attrs = source_attributes[source] -%}
{%- set entity_col_idx = {} -%}
{% if not loop.first %}
UNION ALL
{% endif %}
SELECT
{% for entity in entities %}
    {%- if is_self_ref -%}
        {%- set col_list = entity_cols.get(entity, []) -%}
        {%- set idx = entity_col_idx.get(entity, 0) -%}
        {%- set src_col = col_list[idx] if idx < (col_list | length) else entity ~ '_id' -%}
        {%- do entity_col_idx.update({entity: idx + 1}) -%}
        {%- set seq = entities[:loop.index] | select('equalto', entity) | list | length -%}
    {{ src_col }} AS {{ entity }}_{{ seq }}_id,
    {%- else %}
    {{ entity_cols.get(entity, [entity ~ '_id'])[0] }} AS {{ entity }}_id,
    {%- endif %}
{% endfor %}
{% for attr in all_attrs %}
    {% if attr in src_attrs %}{{ attr }}{% else %}NULL AS {{ attr }}{% endif %}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ ref(source) }}
{%- endfor %}
{% endif %}