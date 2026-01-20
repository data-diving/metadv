"""Target generator - generates models for entity and relation targets."""

from pathlib import Path
from typing import Any, Dict, List

from .base import BaseGenerator


class TargetGenerator(BaseGenerator):
    """Generator for target-level templates."""

    def __init__(self, package_name: str, target_type: str):
        """Initialize generator for a specific target type."""
        super().__init__(package_name)
        self.target_type = target_type

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Generate models for all targets of this type."""
        generated_files: List[str] = []

        # Filter targets by type
        if self.target_type == "entity":
            targets = {
                name: info
                for name, info in targets_by_name.items()
                if info.get("type", "entity") == "entity"
            }
        else:
            targets = {
                name: info
                for name, info in targets_by_name.items()
                if info.get("type") == "relation"
            }

        # Get template configs for this domain, filtered to target-level scope only
        templates = self.get_domain_templates(self.target_type)
        target_templates = {
            key: config
            for key, config in templates.items()
            if config.get("scope", self.target_type) == self.target_type
        }

        for template_key, template_config in target_templates.items():
            condition = template_config.get("condition")

            for target_name, target_info in targets.items():
                # Relations require entities list
                if self.target_type == "relation":
                    entities = target_info.get("entities", [])
                    if not entities:
                        continue

                context = self._build_context(target_name, target_info, source_models)
                if not context.get("source_refs"):
                    continue
                if self.check_condition(condition, context):
                    filepath = self._render_and_write(template_config, context, output_dir)
                    if filepath:
                        generated_files.append(filepath)

        return generated_files

    def _build_context(
        self,
        target_name: str,
        target_info: Dict[str, Any],
        source_models: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build template context for a target."""
        if self.target_type == "entity":
            return self._build_entity_context(target_name, source_models)
        else:
            return self._build_relation_context(target_name, target_info, source_models)

    def _build_entity_context(
        self,
        entity_name: str,
        source_models: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build template context for an entity (hub/dimension)."""
        source_refs = []
        for source_name, source_info in source_models.items():
            for col in source_info["columns"]:
                if col.get("target"):
                    for target_conn in col["target"]:
                        if target_conn.get("target_name") == entity_name:
                            # Include all column names from this source for UNION ALL handling
                            all_columns = [c["column"] for c in source_info["columns"]]
                            source_refs.append(
                                {
                                    "source": source_name,
                                    "column": col["column"],
                                    "all_columns": all_columns,
                                }
                            )
                            break

        return {
            "entity_name": entity_name,
            "source_refs": source_refs,
        }

    def _build_relation_context(
        self,
        relation_name: str,
        relation_info: Dict[str, Any],
        source_models: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build template context for a relation (link/fact)."""
        entities = relation_info.get("entities", [])
        source_refs = self._find_link_sources(relation_name, source_models)
        fk_columns = self._build_fk_columns(relation_name, entities)

        return {
            "relation_name": relation_name,
            "link_name": relation_name,
            "entities": entities,
            "entities_joined": "_".join(entities),
            "source_refs": source_refs,
            "fk_columns": fk_columns,
        }

    def _find_link_sources(
        self,
        relation_name: str,
        source_models: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Find source models explicitly connected to a relation."""
        sources = []

        for source_name, source_info in source_models.items():
            entity_columns: Dict[str, List[str]] = {}
            is_connected = False

            for col in source_info["columns"]:
                if col.get("target"):
                    for target_conn in col["target"]:
                        target_name = target_conn.get("target_name")
                        entity_name = target_conn.get("entity_name")

                        if target_name == relation_name and entity_name:
                            is_connected = True
                            if entity_name not in entity_columns:
                                entity_columns[entity_name] = []
                            entity_columns[entity_name].append(col["column"])

            if is_connected:
                # Include all column names from this source for UNION ALL handling
                all_columns = [c["column"] for c in source_info["columns"]]
                sources.append(
                    {
                        "source": source_name,
                        "entity_columns": entity_columns,
                        "all_columns": all_columns,
                    }
                )

        return sources

    def _build_fk_columns(self, relation_name: str, entities: List[str]) -> List[str]:
        """Build foreign key column list for a relation."""
        fk_columns = []
        is_self_link = len(entities) != len(set(entities))

        for i, entity in enumerate(entities):
            if is_self_link:
                seq = entities[: i + 1].count(entity)
                fk_columns.append(f"{relation_name}_{entity}_{seq}_hk")
            else:
                fk_columns.append(f"{relation_name}_{entity}_hk")

        return fk_columns
