{%- set entity_name = '${entity_name}' -%}
{%- set source_refs = ${source_refs} -%}

{%- set sources = source_refs | map(attribute='source') | unique | list -%}

{% if sources | length == 1 %}
{%- set src_ref = source_refs[0] -%}
SELECT
    {{ src_ref['column'] }} AS {{ entity_name }}_id
{% for attr in src_ref.get('attributes', []) %}
    {{ attr }}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ ref(src_ref['source']) }}

{% else %}
{%- set all_attrs = [] -%}
{%- for src_ref in source_refs -%}
    {%- for attr in src_ref.get('attributes', []) -%}
        {%- if attr not in all_attrs -%}
            {%- do all_attrs.append(attr) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{%- for src_ref in source_refs %}
{%- set src_attrs = src_ref.get('attributes', []) -%}
{% if not loop.first %}
UNION ALL
{% endif %}
SELECT
    {{ src_ref['column'] }} AS {{ entity_name }}_id
{% for attr in all_attrs %}
    {% if loop.first %},{% endif %}
    {% if attr in src_attrs %}{{ attr }}{% else %}NULL AS {{ attr }}{% endif %}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ ref(src_ref['source']) }}
{%- endfor %}
{% endif %}
