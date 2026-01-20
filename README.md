# metadv - Metadata-Driven Model Generator

metadv is a Python library for generating SQL models from a declarative YAML configuration. It supports multiple data modeling approaches including Data Vault 2.0 and Dimensional Modeling, with template packages for popular dbt libraries.

## Features

- **Declarative Configuration**: Define your data model structure in a single YAML file
- **Multiple Modeling Approaches**: Support for Data Vault 2.0 and Dimensional Modeling
- **Template Packages**: Works with automate_dv, datavault4dbt, and dimensional templates
- **Custom Templates**: Add your own template packages for different frameworks
- **Validation**: Validates your configuration before generating models
- **CLI & Library**: Use as a command-line tool or import as a Python library

## Installation

```bash
pip install metadv
```

## Quick Start

1. Create a `metadv.yml` file in your dbt project's `models/metadv/` folder
2. Define your targets (entities and relations) and source mappings
3. Run the generator to create SQL models

See [sample_metadv.yml](sample_metadv.yml) for a complete example configuration.

## Usage

### Command Line

```bash
# Generate Data Vault models using automate_dv
metadv /path/to/dbt/project --package datavault-uk/automate_dv

# Generate Data Vault models using datavault4dbt
metadv /path/to/dbt/project --package scalefreecom/datavault4dbt

# Generate Dimensional models
metadv /path/to/dbt/project --package dimensional

# Validate only (don't generate)
metadv /path/to/dbt/project --package datavault-uk/automate_dv --validate-only

# Generate to custom output directory
metadv /path/to/dbt/project --package datavault-uk/automate_dv --output ./output

# Show detailed output including warnings
metadv /path/to/dbt/project --package datavault-uk/automate_dv --verbose

# Output results as JSON
metadv /path/to/dbt/project --package datavault-uk/automate_dv --json
```

### Python Library

```python
from metadv import MetaDVGenerator

# Initialize generator with package name
generator = MetaDVGenerator('/path/to/dbt/project', 'datavault-uk/automate_dv')

# Validate configuration
result = generator.validate()
if result.errors:
    print("Validation errors:", [e.message for e in result.errors])

# Generate SQL models
success, error, files = generator.generate()
if success:
    print(f"Generated {len(files)} files")
else:
    print(f"Error: {error}")
```

## Supported Packages

| Package | Description | Generated Models |
|---------|-------------|------------------|
| `datavault-uk/automate_dv` | Data Vault 2.0 using [automate_dv](https://github.com/Datavault-UK/automate-dv) | Stage, Hub, Link, Satellite |
| `scalefreecom/datavault4dbt` | Data Vault 2.0 using [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt) | Stage, Hub, Link, Satellite |
| `dimensional` | Dimensional Modeling | Dimension, Fact |

## Configuration Reference

### metadv.yml Structure

```yaml
metadv:
  # Define your targets (entities and relations)
  targets:
    - name: customer
      type: entity
      description: Customer business entity

    - name: order
      type: entity
      description: Order business entity

    - name: customer_order
      type: relation
      description: Customer to order relationship
      entities:
        - customer
        - order

  # Define source models and their column mappings
  sources:
    - name: raw_customers
      columns:
        - name: customer_id
          target:
            - target_name: customer  # Entity key connection

        - name: customer_name
          target:
            - attribute_of: customer  # Attribute connection

    - name: raw_orders
      columns:
        - name: order_id
          target:
            - target_name: order

        - name: customer_id
          target:
            - target_name: customer_order
              entity_name: customer  # Which entity in the relation

        - name: order_date
          target:
            - attribute_of: order
              multiactive_key: true  # Mark as multiactive key
```

### Target Types

| Type | Description | Data Vault Output | Dimensional Output |
|------|-------------|-------------------|-------------------|
| `entity` | A business entity (e.g., Customer, Product) | Hub + Satellite | Dimension |
| `relation` | A relationship between entities | Link + Satellite | Fact |

### Column Target Array

Each column has a `target` array that can contain multiple connections:

| Field | Description |
|-------|-------------|
| `target_name` | Target entity/relation this column identifies (creates key) |
| `entity_name` | For relation connections: which entity within the relation |
| `entity_index` | For self-referencing relations: entity position (0-indexed) |
| `attribute_of` | Target this column is an attribute of (satellite/dimension payload) |
| `target_attribute` | Custom display name for the attribute |
| `multiactive_key` | Mark as multiactive key column (useful for Data Vault) |

### Connection Types

1. **Entity/Relation Key Connections** (`target_name`): Link a source column to a target. The column becomes a business key.

2. **Attribute Connections** (`attribute_of`): Link a source column as an attribute of a target. The column becomes part of the satellite or dimension payload.

### Multiactive Satellites (Data Vault)

For satellites with multiple active records per business key, mark one or more columns as multiactive keys:

```yaml
- name: phone_number
  target:
    - attribute_of: customer
      multiactive_key: true  # This column distinguishes active records
```

Multiactive key columns are:
- Used to identify unique records (can be used as child key within the satellite)
- Excluded from the payload columns
- Generate `ma_sat_` models instead of `sat_` models using condition in templates.yml

## Validation

metadv validates your configuration and reports:

**Errors** (must be fixed before generating):
- Relations missing entity connections from sources

**Warnings** (recommendations):
- Entities without source connections
- Targets without descriptions
- Columns without any connections

Run with `--validate-only` to check your configuration without generating files.

## Custom Template Packages

You can create custom template packages by adding a folder under `src/metadv/templates/` with:

1. A `templates.yml` file defining template configurations
2. SQL template files using Jinja2 and Python string.Template syntax

### templates.yml Structure

The `templates.yml` file defines which templates to generate for each domain (entity, relation, source):

```yaml
# Templates for entity targets (e.g., Hub, Dimension)
entity:
  hub:                                    # Template key (arbitrary name)
    template: hub.sql                     # Template file to use
    filename: "hub/hub_{entity_name}.sql" # Output filename pattern
    scope: entity                         # Generator scope (see below)
  sat:
    template: sat.sql
    filename: "sat/sat_{entity_name}__{source_name}.sql"
    scope: source                         # One file per source-target pair
    condition: has_attributes             # Only generate if condition is true
  ma_sat:
    template: ma_sat.sql
    filename: "sat/ma_sat_{entity_name}__{source_name}.sql"
    scope: source
    condition: is_multiactive             # Only for multiactive satellites

# Templates for relation targets (e.g., Link, Fact)
relation:
  link:
    template: link.sql
    filename: "link/link_{relation_name}.sql"
    scope: relation
  sat:
    template: sat.sql
    filename: "sat/sat_{relation_name}__{source_name}.sql"
    scope: source
    condition: has_attributes

# Templates for source models (e.g., Stage)
source:
  stage:
    template: stage.sql
    filename: "stage/stg_{source_name}.sql"
    scope: source
```

### Template Configuration Fields

| Field | Description |
|-------|-------------|
| `template` | SQL template filename in the package folder |
| `filename` | Output path pattern with placeholders like `{entity_name}`, `{source_name}`, `{relation_name}` |
| `scope` | Determines generator type and context passed to template |
| `condition` | Optional condition that must be true to generate this template |

### Scope Types

| Scope | Generator | Description |
|-------|-----------|-------------|
| `entity` | TargetGenerator | One file per entity target |
| `relation` | TargetGenerator | One file per relation target |
| `source` | SourceTargetGenerator | One file per source-target pair (for satellites) |
| `source` (in source domain) | SourceGenerator | One file per source model (for staging) |

### Built-in Conditions

| Condition | True when |
|-----------|-----------|
| `has_attributes` | Source has attribute columns for this target |
| `is_multiactive` | Source has multiactive key columns for this target |

### Template Context Variables

Templates receive context variables based on their scope. Use Python `${variable}` syntax for initial substitution, then Jinja2 `{{ variable }}` for dbt rendering:

**Entity scope:** `entity_name`, `source_refs`

**Relation scope:** `relation_name`, `entities`, `source_refs`, `fk_columns`

**Source scope (source-target):** `source_name`, `source_model`, `entity_name`/`relation_name`, `attributes`, `key_column`, `columns`

**Source scope (source):** `source_name`, `columns`

## License

MIT License
