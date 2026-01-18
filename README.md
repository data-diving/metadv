# metadv - Metadata-Driven Data Vault Generator

metadv is a Python library for generating Data Vault 2.0 SQL models from a declarative YAML configuration. It supports popular dbt Data Vault packages like [automate_dv](https://github.com/Datavault-UK/automate-dv) and [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt).

## Features

- **Declarative Configuration**: Define your Data Vault structure in a single YAML file
- **Automatic SQL Generation**: Generate stage, hub, link, and satellite models
- **Multiple Package Support**: Works with automate_dv and datavault4dbt
- **Validation**: Validates your configuration before generating models
- **Multiactive Satellites**: Support for multiactive satellites with configurable key columns
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
# Generate SQL models using automate_dv
metadv /path/to/dbt/project --package datavault-uk/automate_dv

# Generate SQL models using datavault4dbt
metadv /path/to/dbt/project --package scalefreecom/datavault4dbt

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

## Configuration Reference

### metadv.yml Structure

```yaml
metadv:
  # Define your Data Vault targets (entities and relations)
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
    - name: stg_customers
      columns:
        - name: customer_id
          target:
            - target_name: customer  # Entity key connection

        - name: customer_name
          target:
            - attribute_of: customer  # Attribute connection

    - name: stg_orders
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

| Type | Description | Generated Models |
|------|-------------|------------------|
| `entity` | A business entity (e.g., Customer, Product) | Hub + Satellite |
| `relation` | A relationship between entities | Link + Satellite |

### Column Target Array

Each column has a `target` array that can contain multiple connections:

| Field | Description |
|-------|-------------|
| `target_name` | Target entity/relation this column identifies (creates hash key) |
| `entity_name` | For relation connections: which entity within the relation |
| `entity_index` | For self-referencing relations: entity position (0-indexed) |
| `attribute_of` | Target this column is an attribute of (satellite payload) |
| `target_attribute` | Custom display name for the attribute |
| `multiactive_key` | Mark as multiactive key column (excluded from payload) |

### Connection Types

1. **Entity/Relation Key Connections** (`target_name`): Link a source column to a target. The column becomes a business key and generates a hash key in the stage model.

2. **Attribute Connections** (`attribute_of`): Link a source column as an attribute of a target. The column becomes part of the satellite payload.

### Multiactive Satellites

For satellites with multiple active records per business key, mark one or more columns as multiactive keys:

```yaml
- name: phone_number
  target:
    - attribute_of: customer
      multiactive_key: true  # This column distinguishes active records
```

Multiactive key columns are:
- Used to identify unique records within the satellite
- Excluded from the payload columns
- Generate `ma_sat_` models instead of `sat_` models

## Generated Output

metadv generates the following folder structure:

```
models/metadv/
├── metadv.yml
├── stage/
│   └── stg_<source>.sql
├── hub/
│   └── hub_<entity>.sql
├── link/
│   └── link_<entity1>_<entity2>.sql
└── sat/
    ├── sat_<target>__<source>.sql
    └── ma_sat_<target>__<source>.sql  # Multiactive satellites
```

## Materializations

Configure materializations at the folder level in your `dbt_project.yml`:

```yaml
models:
  your_project:
    metadv:
      stage:
        +materialized: view
      hub:
        +materialized: incremental
      link:
        +materialized: incremental
      sat:
        +materialized: incremental
```

**Recommended materializations:**
- `stage/` - `view` (staging models refresh with each run)
- `hub/` - `incremental` (hubs track historical business keys)
- `link/` - `incremental` (links track historical relationships)
- `sat/` - `incremental` (satellites track attribute history)

## Supported Packages

metadv supports the following Data Vault packages:

| Package | Package Name |
|---------|--------------|
| [automate_dv](https://github.com/Datavault-UK/automate-dv) | `datavault-uk/automate_dv` |
| [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt) | `scalefreecom/datavault4dbt` |

Specify the package when running the generator using the `--package` flag (CLI) or the second parameter (Python library).

## Validation

metadv validates your configuration and reports:

**Errors** (must be fixed before generating):
- Relations missing entity connections from sources

**Warnings** (recommendations):
- Entities without source connections
- Targets without descriptions
- Columns without any connections

Run with `--validate-only` to check your configuration without generating files.

## License

MIT License
