{%- set relation_name = '${relation_name}' -%}
{%- set entities = ${entities} -%}
{%- set source_refs = ${source_refs} -%}

{#- Get unique source models -#}
{%- set sources = [] -%}
{%- for ref in source_refs -%}
    {%- if ref['source'] not in sources -%}
        {%- do sources.append(ref['source']) -%}
    {%- endif -%}
{%- endfor -%}

{#- Generate CTEs for each source -#}
{% for source in sources %}
{% if loop.first %}WITH {% else %}   , {% endif %}{{ source }}_data AS (
    SELECT * FROM {{ ref(source) }}
)
{% endfor %}

{#- Union all sources or select from single source -#}
{% if sources | length == 1 %}
SELECT
    {#- Foreign keys to dimensions -#}
{%- for entity in entities %}
    {{ entity }}_id,
{%- endfor %}
    *
FROM {{ sources[0] }}_data
{% else %}
SELECT
    {#- Foreign keys to dimensions -#}
{%- for entity in entities %}
    {{ entity }}_id,
{%- endfor %}
    *
FROM (
    {% for source in sources %}
    SELECT * FROM {{ source }}_data
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
{% endif %}
