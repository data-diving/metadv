{%- set source_model = '${source_name}' -%}
{%- set derived_cols = ${derived_columns} -%}
{%- set hashed_cols = ${hashed_columns} -%}
{%- set hashdiff_cols = ${hashdiff_columns} -%}

{%- set derived_columns = {} -%}
{%- for key, value in derived_cols.items() -%}
    {%- do derived_columns.update({key: value}) -%}
{%- endfor -%}

{%- set hashed_columns = {} -%}
{%- for key, columns in hashed_cols.items() -%}
    {%- do hashed_columns.update({key: columns}) -%}
{%- endfor -%}
{%- for target, columns in hashdiff_cols.items() -%}
    {%- do hashed_columns.update({target ~ '_hashdiff': {'is_hashdiff': true, 'columns': columns}}) -%}
{%- endfor -%}

{%- set rsrc_static = '!' ~ source_model -%}

{{ datavault4dbt.stage(
    source_model=source_model,
    ldts=dbt.current_timestamp(),
    rsrc=rsrc_static,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns
) }}
