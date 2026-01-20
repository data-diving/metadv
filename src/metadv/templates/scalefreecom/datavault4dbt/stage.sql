{%- set source_model = '${source_name}' -%}
{%- set columns = ${columns} -%}

{#- Build derived columns -#}
{%- set derived_columns = {} -%}

{#- Build hashed columns from column targets -#}
{%- set hashed_columns = {} -%}
{%- set hashdiff_columns = {} -%}
{%- set link_columns = {} -%}

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
                {#- Foreign key hash for the entity within the link -#}
                {%- set fk_name = target_name ~ '_' ~ entity_name ~ '_hk' -%}
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
