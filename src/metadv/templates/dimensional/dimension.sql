{%- set entity_name = '${entity_name}' -%}
{%- set source_refs = ${source_refs} -%}

{#- Build source to key column and all_columns mapping -#}
{%- set source_key_columns = {} -%}
{%- set source_all_columns = {} -%}
{%- for ref in source_refs -%}
    {%- if ref['source'] not in source_key_columns -%}
        {%- do source_key_columns.update({ref['source']: ref['column']}) -%}
        {%- do source_all_columns.update({ref['source']: ref.get('all_columns', [])}) -%}
    {%- endif -%}
{%- endfor -%}

{#- Get unique source models -#}
{%- set sources = source_refs | map(attribute='source') | unique | list -%}

{#- Single source: simple select with key alias -#}
{% if sources | length == 1 %}
{%- set source = sources[0] -%}
{%- set key_column = source_key_columns[source] -%}
SELECT
    {{ key_column }} AS {{ entity_name }}_id,
    *
FROM {{ ref(source) }}

{#- Multiple sources: explicit column list with NULLs for missing columns -#}
{% else %}
{#- Collect all unique columns across all sources -#}
{%- set all_cols = [] -%}
{%- for source in sources -%}
    {%- for col in source_all_columns[source] -%}
        {%- if col not in all_cols -%}
            {%- do all_cols.append(col) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{#- Build list of non-key columns -#}
{%- set non_key_cols = [] -%}
{%- set all_keys = source_key_columns.values() | list -%}
{%- for col in all_cols -%}
    {%- if col not in all_keys -%}
        {%- do non_key_cols.append(col) -%}
    {%- endif -%}
{%- endfor -%}

{%- for source in sources %}
{%- set key_column = source_key_columns[source] -%}
{%- set src_cols = source_all_columns[source] -%}
{% if loop.first %}{% else %}UNION ALL
{% endif %}SELECT
    {{ key_column }} AS {{ entity_name }}_id{{ "," if non_key_cols | length > 0 else "" }}
{%- for col in non_key_cols %}
    {% if col in src_cols %}{{ col }}{% else %}NULL AS {{ col }}{% endif %}{{ "," if not loop.last else "" }}
{%- endfor %}

FROM {{ ref(source) }}
{%- endfor %}
{% endif %}
