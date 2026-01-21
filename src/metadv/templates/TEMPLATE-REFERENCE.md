# MetaDV Template Reference

This document describes how the generator system works and what context variables are available in templates.

## Overview

MetaDV uses three generator types to produce different kinds of output files:

| Generator | Scope | Output | Use Case |
|-----------|-------|--------|----------|
| **SourceGenerator** | `source` | One file per source | Staging models |
| **TargetGenerator** | `entity` or `relation` | One file per target | Hubs, Links, Dimensions, Facts |
| **SourceTargetGenerator** | `source` | One file per source-target pair | Satellites |

## Template Configuration (templates.yml)

Each template package contains a `templates.yml` file that defines available templates:

```yaml
entity:
  hub:
    template: hub.sql
    filename: "hub/hub_{entity_name}.sql"
    scope: entity
  sat:
    template: sat.sql
    filename: "sat/sat_{entity_name}__{source_name}.sql"
    scope: source
    condition: has_attributes

relation:
  link:
    template: link.sql
    filename: "link/link_{relation_name}.sql"
    scope: relation
```

### Configuration Fields

| Field | Description |
|-------|-------------|
| `template` | Template filename in the package directory |
| `filename` | Output filename pattern (supports variable substitution) |
| `scope` | Determines which generator processes this template |
| `condition` | Optional condition for template generation |

### Scope Values

- `source` - Processed by **SourceGenerator** (one file per source)
- `entity` - Processed by **TargetGenerator** (one file per entity)
- `relation` - Processed by **TargetGenerator** (one file per relation)
- `source` (under entity/relation) - Processed by **SourceTargetGenerator** (one file per source-target pair)

### Conditions

- `has_attributes` - Only generate if the target has attributes from this source
- `is_multiactive` - Only generate if any attribute has `multiactive_key: true`

---

## SourceGenerator

Generates one file per source model. Used for staging/prep layers.

### Scope

```yaml
template: stage.sql
scope: source
```

### Context Variables

| Variable | Type | Description |
|----------|------|-------------|
| `source_name` | string | Name of the source model |
| `columns` | list[dict] | All columns from the source |
| `targets` | dict | All targets (entities and relations) keyed by name |

### Column Structure

Each item in `columns`:

```python
{
    "column": "customer_id",
    "target": [
        {
            "target_name": "customer",      # Entity/relation this column keys
            "entity_name": "customer",      # For relation FKs: which entity
            "attribute_of": "customer",     # Entity/relation this is an attribute of
            "target_attribute": "name",     # Optional: renamed attribute
            "multiactive_key": True         # Optional: for multi-active satellites
        }
    ]
}
```

### Target Connection Types

1. **Entity Key** - `target_name` only (no `entity_name`, no `attribute_of`)
   ```yaml
   target:
     - target_name: customer
   ```

2. **Relation FK** - `target_name` + `entity_name`
   ```yaml
   target:
     - target_name: order
       entity_name: customer
   ```

3. **Attribute** - `attribute_of` only
   ```yaml
   target:
     - attribute_of: customer
   ```

### Example Template Usage

```jinja
{%- set source_model = '${source_name}' -%}
{%- set columns = ${columns} -%}
{%- set targets = ${targets} -%}

{%- for col in columns -%}
    {%- if col.target -%}
        {%- for target_conn in col.target -%}
            {# Process target connections #}
        {%- endfor -%}
    {%- endif -%}
{%- endfor -%}
```

---

## TargetGenerator

Generates one file per target (entity or relation). Used for hubs, links, dimensions, facts.

### Scope

```yaml
# For entities (hubs, dimensions)
template: hub.sql
scope: entity

# For relations (links, facts)
template: link.sql
scope: relation
```

### Entity Context Variables

| Variable | Type | Description |
|----------|------|-------------|
| `entity_name` | string | Name of the entity |
| `source_refs` | list[dict] | Sources connected to this entity |

### Entity source_refs Structure

Each item in `source_refs`:

```python
{
    "source": "orders",           # Source model name
    "column": "customer_id",      # Key column for this entity
    "attributes": ["name", "email"]  # Attribute columns for this entity
}
```

### Relation Context Variables

| Variable | Type | Description |
|----------|------|-------------|
| `relation_name` | string | Name of the relation |
| `link_name` | string | Alias for relation_name |
| `entities` | list[string] | Ordered list of entity names in the relation |
| `entities_joined` | string | Entities joined with underscore |
| `source_refs` | list[dict] | Sources connected to this relation |
| `fk_columns` | list[string] | Generated FK column names |

### Relation source_refs Structure

Each item in `source_refs`:

```python
{
    "source": "orders",
    "entity_columns": {
        "customer": ["customer_id"],
        "product": ["product_id"]
    },
    "attributes": ["amount", "quantity"]  # Measures for fact tables
}
```

### Self-Links/Self-References

When an entity appears multiple times in a relation (e.g., `entities: [customer, customer]`), the generator automatically handles sequencing:

- `fk_columns` becomes: `["relation_customer_1_hk", "relation_customer_2_hk"]`
- `entity_columns` maps each occurrence to its source column

### Example Template Usage (Entity)

```jinja
{%- set entity_name = '${entity_name}' -%}
{%- set source_refs = ${source_refs} -%}

{% for src_ref in source_refs %}
SELECT
    {{ src_ref['column'] }} AS {{ entity_name }}_id,
{% for attr in src_ref.get('attributes', []) %}
    {{ attr }}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ ref(src_ref['source']) }}
{% endfor %}
```

### Example Template Usage (Relation)

```jinja
{%- set relation_name = '${relation_name}' -%}
{%- set entities = ${entities} -%}
{%- set source_refs = ${source_refs} -%}

{% for src_ref in source_refs %}
{%- set entity_cols = src_ref.get('entity_columns', {}) -%}
SELECT
{% for entity in entities %}
    {{ entity_cols.get(entity, [entity ~ '_id'])[0] }} AS {{ entity }}_id,
{% endfor %}
{% for attr in src_ref.get('attributes', []) %}
    {{ attr }}{{ "," if not loop.last else "" }}
{% endfor %}
FROM {{ ref(src_ref['source']) }}
{% endfor %}
```

---

## SourceTargetGenerator

Generates one file per source-target pair. Used for satellites.

### Scope

```yaml
entity:
  sat:
    template: sat.sql
    scope: source           # Note: scope is "source" but under entity section
    condition: has_attributes
```

### Context Variables

| Variable | Type | Description |
|----------|------|-------------|
| `target_name` | string | Name of the target (entity or relation) |
| `source_name` | string | Name of the source model |
| `source_model` | string | Formatted source model name (e.g., `stg_orders`) |
| `columns` | list[dict] | All columns from the source |
| `attributes` | list[dict] | Attribute columns for this target from this source |
| `key_column` | string | Key column for this target |
| `entity_name` | string | (Entity only) Same as target_name |
| `entity_key_column` | string | (Entity only) Same as key_column |
| `relation_name` | string | (Relation only) Same as target_name |
| `entities` | list[string] | (Relation only) Entities in the relation |
| `entities_joined` | string | (Relation only) Entities joined with underscore |

### Attributes Structure

Each item in `attributes`:

```python
{
    "column": "customer_name",
    "target_attribute": "name",      # Optional: renamed attribute
    "multiactive_key": True          # Optional: for multi-active satellites
}
```

### Example Template Usage

```jinja
{%- set source_model = '${source_model}' -%}
{%- set target_name = '${target_name}' -%}
{%- set attributes = ${attributes} -%}

{%- set payload_columns = [] -%}
{%- for attr in attributes -%}
    {%- if not attr.multiactive_key -%}
        {%- do payload_columns.append(attr.column) -%}
    {%- endif -%}
{%- endfor -%}

{{ datavault4dbt.sat_v0(
    source_model=source_model,
    parent_hashkey=target_name ~ '_hk',
    src_hashdiff=target_name ~ '_hashdiff',
    src_payload=payload_columns
) }}
```

---

## Template Syntax

Templates use a two-phase rendering:

1. **Python string.Template** (`${var}`) - Initial substitution by MetaDV
2. **Jinja2** (`{{ var }}`) - Rendered by dbt at compile time

### Phase 1: Python Substitution

Variables like `${entity_name}` are replaced with actual values:

```jinja
{%- set entity_name = '${entity_name}' -%}
```

Becomes:

```jinja
{%- set entity_name = 'customer' -%}
```

### Phase 2: dbt/Jinja2 Rendering

Standard Jinja2 syntax for dbt:

```jinja
{{ ref('stg_orders') }}
{{ config(materialized='table') }}
```

### Important Notes

1. **Avoid shadowing dbt functions** - Don't use `ref` as a variable name (use `src_ref` instead)

2. **Whitespace control** - Use `{%-` and `-%}` carefully:
   - `{%-` strips preceding whitespace
   - `-%}` strips following whitespace
   - Use `{% for` (not `{%- for`) when you need the newline before output

3. **List/dict operations** - Use Jinja2's `do` extension:
   ```jinja
   {%- do my_list.append(item) -%}
   {%- do my_dict.update({key: value}) -%}
   ```

---

## Common Patterns

### Single vs Multiple Sources (UNION ALL)

```jinja
{%- set sources = source_refs | map(attribute='source') | unique | list -%}

{% if sources | length == 1 %}
{# Simple single-source query #}
SELECT * FROM {{ ref(sources[0]) }}

{% else %}
{# UNION ALL for multiple sources #}
{% for src_ref in source_refs %}
{% if not loop.first %}UNION ALL{% endif %}
SELECT
    {{ src_ref['column'] }} AS id,
    {% for attr in all_attrs %}
    {% if attr in src_ref.get('attributes', []) %}{{ attr }}{% else %}NULL AS {{ attr }}{% endif %}{{ "," if not loop.last }}
    {% endfor %}
FROM {{ ref(src_ref['source']) }}
{% endfor %}
{% endif %}
```

### Self-Link Handling

```jinja
{%- set is_self_ref = (entities | length) != (entities | unique | list | length) -%}

{% for entity in entities %}
    {%- if is_self_ref -%}
        {%- set seq = entities[:loop.index] | select('equalto', entity) | list | length -%}
    {{ col }} AS {{ entity }}_{{ seq }}_id,
    {%- else %}
    {{ col }} AS {{ entity }}_id,
    {%- endif %}
{% endfor %}
```

### Conditional Template Generation

In `templates.yml`:
```yaml
sat:
  template: sat.sql
  scope: source
  condition: has_attributes
```

Only generates if the source has attributes for the target entity.
