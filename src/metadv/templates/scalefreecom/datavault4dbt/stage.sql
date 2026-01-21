{%- set source_model = '${source_name}' -%}
{%- set columns = ${columns} -%}
{%- set targets = ${targets} -%}

{#- Build derived columns -#}
{%- set derived_columns = {} -%}

{#- Build hashed columns from column targets -#}
{%- set hashed_columns = {} -%}
{%- set hashdiff_columns = {} -%}
{%- set link_columns = {} -%}

{#- Track entity occurrences per relation for self-link handling -#}
{%- set relation_entity_counts = {} -%}

{%- for col in columns -%}
    {%- if col.target -%}
        {%- for target_conn in col.target -%}
            {%- set target_name = target_conn.target_name -%}
            {%- set entity_name = target_conn.entity_name -%}
            {%- set attribute_of = target_conn.attribute_of -%}

            {#- Key column for entity (hub hash key + natural key) -#}
            {%- if target_name and not entity_name and not attribute_of -%}
                {%- set hk_name = target_name ~ '_hk' -%}
                {%- if hk_name not in hashed_columns -%}
                    {%- do hashed_columns.update({hk_name: []}) -%}
                {%- endif -%}
                {%- do hashed_columns[hk_name].append(col.column) -%}

                {#- Add natural key as derived column (entity_id) -#}
                {%- set nk_name = target_name ~ '_id' -%}
                {%- do derived_columns.update({nk_name: col.column}) -%}
            {%- endif -%}

            {#- Key column for relation (link foreign key + collect for link hash key) -#}
            {%- if target_name and entity_name -%}
                {#- Check if this is a self-link (same entity appears multiple times) -#}
                {%- set relation_info = targets.get(target_name, {}) -%}
                {%- set relation_entities = relation_info.get('entities', []) -%}
                {%- set is_self_link = (relation_entities | length) != (relation_entities | unique | list | length) -%}

                {#- Track entity occurrence for self-links -#}
                {%- set relation_key = target_name ~ '_' ~ entity_name -%}
                {%- if relation_key not in relation_entity_counts -%}
                    {%- do relation_entity_counts.update({relation_key: 0}) -%}
                {%- endif -%}
                {%- do relation_entity_counts.update({relation_key: relation_entity_counts[relation_key] + 1}) -%}

                {#- Build FK name with sequence number for self-links -#}
                {%- if is_self_link -%}
                    {%- set fk_name = target_name ~ '_' ~ entity_name ~ '_' ~ relation_entity_counts[relation_key] ~ '_hk' -%}
                {%- else -%}
                    {%- set fk_name = target_name ~ '_' ~ entity_name ~ '_hk' -%}
                {%- endif -%}

                {%- if fk_name not in hashed_columns -%}
                    {%- do hashed_columns.update({fk_name: []}) -%}
                {%- endif -%}
                {%- do hashed_columns[fk_name].append(col.column) -%}

                {#- Collect columns for link hash key (composite of all entity keys) -#}
                {%- if target_name not in link_columns -%}
                    {%- do link_columns.update({target_name: []}) -%}
                {%- endif -%}
                {%- do link_columns[target_name].append(col.column) -%}
            {%- endif -%}

            {#- Attribute column (for satellite hashdiff) -#}
            {%- if attribute_of -%}
                {%- if attribute_of not in hashdiff_columns -%}
                    {%- do hashdiff_columns.update({attribute_of: []}) -%}
                {%- endif -%}
                {%- do hashdiff_columns[attribute_of].append(col.column) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
{%- endfor -%}

{#- Add link hash keys (composite of all entity columns for each link) -#}
{%- for link_name, cols in link_columns.items() -%}
    {%- do hashed_columns.update({link_name ~ '_hk': cols}) -%}
{%- endfor -%}

{#- Convert hashdiff columns to hashed_columns format -#}
{%- for target, cols in hashdiff_columns.items() -%}
    {%- do hashed_columns.update({target ~ '_hashdiff': {'is_hashdiff': true, 'columns': cols}}) -%}
{%- endfor -%}

{%- set rsrc_static = '!' ~ source_model -%}

{{ datavault4dbt.stage(
    source_model=source_model,
    ldts=dbt.current_timestamp(),
    rsrc=rsrc_static,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns
) }}
