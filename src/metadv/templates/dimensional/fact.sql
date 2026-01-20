{%- set relation_name = '${relation_name}' -%}
{%- set entities = ${entities} -%}
{%- set source_refs = ${source_refs} -%}

{#- Get unique source models -#}
{%- set sources = source_refs | map(attribute='source') | unique | list -%}

{#- Build source to entity column and all_columns mappings -#}
{%- set source_entity_columns = {} -%}
{%- set source_all_columns = {} -%}
{%- for ref in source_refs -%}
    {%- if ref.entity_columns is defined -%}
        {%- do source_entity_columns.update({ref['source']: ref.entity_columns}) -%}
    {%- endif -%}
    {%- if ref.all_columns is defined -%}
        {%- do source_all_columns.update({ref['source']: ref.all_columns}) -%}
    {%- endif -%}
{%- endfor -%}

{#- Single source: simple select with FK aliases -#}
{% if sources | length == 1 %}
{%- set source = sources[0] -%}
{%- set entity_cols = source_entity_columns[source] if source in source_entity_columns else {} -%}
SELECT
{%- for entity in entities %}
    {{ entity_cols[entity][0] if entity in entity_cols else entity ~ '_id' }} AS {{ entity }}_id,
{%- endfor %}
    *
FROM {{ ref(source) }}

{#- Multiple sources: explicit column list with NULLs for missing columns -#}
{% else %}
{#- Collect all unique columns across all sources -#}
{%- set all_cols = [] -%}
{%- for source in sources -%}
    {%- for col in source_all_columns.get(source, []) -%}
        {%- if col not in all_cols -%}
            {%- do all_cols.append(col) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{#- Build list of FK source columns to exclude from non-key columns -#}
{%- set fk_source_cols = [] -%}
{%- for source in sources -%}
    {%- set entity_cols = source_entity_columns.get(source, {}) -%}
    {%- for entity in entities -%}
        {%- if entity in entity_cols -%}
            {%- for col in entity_cols[entity] -%}
                {%- if col not in fk_source_cols -%}
                    {%- do fk_source_cols.append(col) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{#- Build list of non-FK columns -#}
{%- set non_fk_cols = [] -%}
{%- for col in all_cols -%}
    {%- if col not in fk_source_cols -%}
        {%- do non_fk_cols.append(col) -%}
    {%- endif -%}
{%- endfor -%}

{%- for source in sources %}
{%- set entity_cols = source_entity_columns.get(source, {}) -%}
{%- set src_cols = source_all_columns.get(source, []) -%}
{% if loop.first %}{% else %}UNION ALL
{% endif %}SELECT
{%- for entity in entities %}
    {{ entity_cols[entity][0] if entity in entity_cols else entity ~ '_id' }} AS {{ entity }}_id,
{%- endfor %}
{%- for col in non_fk_cols %}
    {% if col in src_cols %}{{ col }}{% else %}NULL AS {{ col }}{% endif %}{{ "," if not loop.last else "" }}
{%- endfor %}

FROM {{ ref(source) }}
{%- endfor %}
{% endif %}
