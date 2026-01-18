"""Satellite model generator."""

from pathlib import Path
from typing import Dict, List, Any, Optional

from .base import BaseGenerator


class SatGenerator(BaseGenerator):
    """Generator for satellite models."""

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]]
    ) -> List[str]:
        """Generate satellite models - one per source model-target pair."""
        sat_dir = output_dir / "sat"
        sat_dir.mkdir(parents=True, exist_ok=True)

        generated_files: List[str] = []

        # For each source model, find all targets it connects to
        for source_name, source_info in source_models.items():
            # Group columns by their target (attribute_of)
            target_columns: Dict[str, List[Dict[str, Any]]] = {}
            entity_columns: Dict[str, List[Dict[str, Any]]] = {}

            for col in source_info['columns']:
                # Process unified target array
                if col.get('target'):
                    for target_conn in col['target']:
                        # Check if this is an attribute connection
                        attr_target = target_conn.get('attribute_of')
                        if attr_target:
                            if attr_target not in target_columns:
                                target_columns[attr_target] = []
                            # Store the column with its attribute metadata
                            col_with_meta = {
                                **col,
                                'target_attribute': target_conn.get('target_attribute'),
                                'multiactive_key': target_conn.get('multiactive_key')
                            }
                            target_columns[attr_target].append(col_with_meta)
                        else:
                            # Entity/relation key connection
                            target_name = target_conn.get('target_name')
                            entity_name = target_conn.get('entity_name')  # Only for relation connections

                            # Use target_name for entity targets, entity_name for relation connections
                            entity = entity_name if entity_name else target_name
                            if entity:
                                if entity not in entity_columns:
                                    entity_columns[entity] = []
                                entity_columns[entity].append(col)

            # Generate a satellite for each target that has attributes from this source
            for target_name, attrs in target_columns.items():
                # Check if any attribute has multiactive_key = true
                multiactive_key_columns = [
                    attr['column'] for attr in attrs if attr.get('multiactive_key') is True
                ]
                is_multiactive = len(multiactive_key_columns) > 0

                # Use different filename prefix for multiactive satellites
                if is_multiactive:
                    filename = f"ma_sat_{target_name}__{source_name}.sql"
                else:
                    filename = f"sat_{target_name}__{source_name}.sql"
                filepath = sat_dir / filename

                # Get the entity key column for this target
                entity_key_col = None
                if target_name in entity_columns:
                    entity_key_col = entity_columns[target_name][0]['column']

                sql_content = self.render_sql(
                    target_name=target_name,
                    source_name=source_name,
                    attributes=attrs,
                    entity_key_column=entity_key_col,
                    multiactive_key_columns=multiactive_key_columns
                )

                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(sql_content)

                generated_files.append(str(filepath))

        return generated_files

    def render_sql(
        self,
        target_name: str,
        source_name: str,
        attributes: List[Dict[str, Any]],
        entity_key_column: Optional[str] = None,
        multiactive_key_columns: Optional[List[str]] = None
    ) -> str:
        """Render SQL content for a satellite model using template."""
        source_model = self._get_stage_ref(source_name)

        # If there are multiactive key columns, exclude them from payload
        # (they are used as the multiactive key, not as payload data)
        if multiactive_key_columns and len(multiactive_key_columns) > 0:
            multiactive_key_set = set(multiactive_key_columns)
            payload_columns = [
                attr['column'] for attr in attributes
                if attr['column'] not in multiactive_key_set
            ]
        else:
            payload_columns = [attr['column'] for attr in attributes]

        # If there are multiactive key columns, use the ma_sat template
        if multiactive_key_columns and len(multiactive_key_columns) > 0:
            # Different parameter names for different packages
            if self.package_prefix == 'datavault4dbt':
                return self.render_template(
                    'ma_sat.sql',
                    target_name=target_name,
                    source_model=source_model,
                    payload_columns=payload_columns,
                    ma_key_columns=multiactive_key_columns
                )
            else:
                # automate_dv uses src_cdk
                return self.render_template(
                    'ma_sat.sql',
                    target_name=target_name,
                    source_model=source_model,
                    payload_columns=payload_columns,
                    cdk_columns=multiactive_key_columns
                )

        return self.render_template(
            'sat.sql',
            target_name=target_name,
            source_model=source_model,
            payload_columns=payload_columns
        )
