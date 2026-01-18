"""Hub model generator."""

from pathlib import Path
from typing import Dict, List, Any

from .base import BaseGenerator


class HubGenerator(BaseGenerator):
    """Generator for hub models."""

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]]
    ) -> List[str]:
        """Generate hub models - one per entity target."""
        hub_dir = output_dir / "hub"
        hub_dir.mkdir(parents=True, exist_ok=True)

        generated_files: List[str] = []

        for target_name, target_info in targets_by_name.items():
            if target_info.get('type', 'entity') != 'entity':
                continue

            filename = f"hub_{target_name}.sql"
            filepath = hub_dir / filename

            # Find source models that have target connections to this entity
            source_refs = []
            for source_name, source_info in source_models.items():
                for col in source_info['columns']:
                    # New structure: target is an array of dicts with target_name
                    if col.get('target'):
                        for target_conn in col['target']:
                            # For entity targets, target_name is the entity
                            if target_conn.get('target_name') == target_name:
                                source_refs.append({
                                    'source': source_name,
                                    'column': col['column']
                                })
                                break  # Only add once per column

            # Skip if no sources found for this entity
            if not source_refs:
                continue

            sql_content = self.render_sql(
                entity_name=target_name,
                source_refs=source_refs
            )

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(sql_content)

            generated_files.append(str(filepath))

        return generated_files

    def render_sql(
        self,
        entity_name: str,
        source_refs: List[Dict[str, str]]
    ) -> str:
        """Render SQL content for a hub model using template."""
        source_models = self._get_unique_stage_models(source_refs)

        return self.render_template(
            'hub.sql',
            entity_name=entity_name,
            source_models=source_models
        )
