"""Stage model generator."""

from pathlib import Path
from typing import Dict, List, Any

from .base import BaseGenerator


class StageGenerator(BaseGenerator):
    """Generator for stage models."""

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]]
    ) -> List[str]:
        """Generate stage models - one per source model with target connections."""
        stage_dir = output_dir / "stage"
        stage_dir.mkdir(parents=True, exist_ok=True)

        generated_files: List[str] = []

        for source_name, source_info in source_models.items():
            if not source_info['connected_targets']:
                continue  # Skip sources with no connections

            filename = f"stg_{source_name}.sql"
            filepath = stage_dir / filename

            columns = source_info['columns']
            sql_content = self.render_sql(
                source_name=source_name,
                columns=columns,
                targets_by_name=targets_by_name
            )

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(sql_content)

            generated_files.append(str(filepath))

        return generated_files

    def render_sql(
        self,
        source_name: str,
        columns: List[Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]]
    ) -> str:
        """Render SQL content for a stage model using template."""
        # Build derived_columns: maps output column name -> source column
        # Build hashed_columns: maps hash key name -> list of source columns
        derived_columns: Dict[str, str] = {}
        hashed_columns: Dict[str, List[str]] = {}

        # Track relation entity columns for building relation hash keys
        # relation_name -> list of (entity_key_name, source_column) tuples
        relation_entity_columns: Dict[str, List[str]] = {}

        for col in columns:
            col_name = col['column']

            # New structure: target is an array of dicts with target_name, entity_name (for relations), entity_index
            if col.get('target'):
                for target_conn in col['target']:
                    target_name = target_conn.get('target_name')
                    entity_name = target_conn.get('entity_name')  # Only for relation connections
                    entity_index = target_conn.get('entity_index')

                    if not target_name:
                        continue

                    target_info = targets_by_name.get(target_name, {})
                    target_type = target_info.get('type', 'entity')

                    if target_type == 'entity':
                        # Entity target: {entity}_id, {entity}_hk
                        derived_columns[f"{target_name}_id"] = col_name
                        if f"{target_name}_hk" not in hashed_columns:
                            hashed_columns[f"{target_name}_hk"] = []
                        hashed_columns[f"{target_name}_hk"].append(col_name)

                    elif target_type == 'relation':
                        # Relation target: target_name is the relation, entity_name is the entity
                        entities = target_info.get('entities', [])

                        # entity_name identifies which entity this column provides for the relation
                        if entity_name and entity_name in entities:
                            # Check if self-link (entity appears multiple times)
                            is_self_link = entities.count(entity_name) > 1

                            if is_self_link and entity_index is not None:
                                # Self-link: {relation}_{entity}_{seq}_id
                                seq = entity_index + 1
                                key_name = f"{target_name}_{entity_name}_{seq}"
                            else:
                                # Regular relation: {relation}_{entity}_id
                                key_name = f"{target_name}_{entity_name}"

                            derived_columns[f"{key_name}_id"] = col_name
                            if f"{key_name}_hk" not in hashed_columns:
                                hashed_columns[f"{key_name}_hk"] = []
                            hashed_columns[f"{key_name}_hk"].append(col_name)

                            # Track for relation hash key
                            if target_name not in relation_entity_columns:
                                relation_entity_columns[target_name] = []
                            relation_entity_columns[target_name].append(col_name)

        # Build relation hash keys: {relation}_hk combines all entity columns for the relation
        for relation_name, entity_cols in relation_entity_columns.items():
            relation_hk = f"{relation_name}_hk"
            if relation_hk not in hashed_columns:
                hashed_columns[relation_hk] = []
            # Add all entity columns to the relation hash key
            for col in entity_cols:
                if col not in hashed_columns[relation_hk]:
                    hashed_columns[relation_hk].append(col)

        # Build attribute hashdiff columns from unified target array
        # Attribute connections are entries with 'attribute_of' key
        hashdiff_columns: Dict[str, List[str]] = {}
        for col in columns:
            if col.get('target'):
                for target_conn in col['target']:
                    attr_target = target_conn.get('attribute_of')
                    if attr_target:
                        if attr_target not in hashdiff_columns:
                            hashdiff_columns[attr_target] = []
                        hashdiff_columns[attr_target].append(col['column'])

        return self.render_template(
            'stage.sql',
            source_name=source_name,
            derived_columns=derived_columns,
            hashed_columns=hashed_columns,
            hashdiff_columns=hashdiff_columns
        )
