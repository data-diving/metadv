"""Source-target generator - generates models for source-target pairs."""

from pathlib import Path
from typing import Dict, List, Any

from .base import BaseGenerator


class SourceTargetGenerator(BaseGenerator):
    """Generator for source-target pair templates (scope: source).

    Generates one file per source-target combination, e.g., satellites in Data Vault
    or SCD Type 2 dimensions in dimensional modeling.
    """

    def __init__(self, package_name: str, target_type: str):
        """Initialize generator for a specific target type.

        Args:
            package_name: Template package/folder name
            target_type: Either "entity" or "relation"
        """
        super().__init__(package_name)
        self.target_type = target_type

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Generate models for all source-target pairs using scope:source templates."""
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

        # Get template configs for this domain, filtered to scope: source
        templates = self.get_domain_templates(self.target_type)
        source_templates = {
            key: config for key, config in templates.items() if config.get("scope") == "source"
        }

        for template_key, template_config in source_templates.items():
            condition = template_config.get("condition")

            for target_name, target_info in targets.items():
                sources = self._find_connected_sources(target_name, source_models)

                for source_name, source_data in sources.items():
                    context = self._build_context(
                        target_name,
                        target_info,
                        source_name,
                        source_data,
                    )
                    if self.check_condition(condition, context):
                        filepath = self._render_and_write(template_config, context, output_dir)
                        if filepath:
                            generated_files.append(filepath)

        return generated_files

    def _find_connected_sources(
        self,
        target_name: str,
        source_models: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Find all sources connected to a target via key or attribute."""
        connected = {}
        for source_name, source_info in source_models.items():
            has_connection = False
            for col in source_info["columns"]:
                if col.get("target"):
                    for target_conn in col["target"]:
                        if target_conn.get("target_name") == target_name:
                            has_connection = True
                            break
                        if target_conn.get("attribute_of") == target_name:
                            has_connection = True
                            break
                if has_connection:
                    break
            if has_connection:
                connected[source_name] = source_info
        return connected

    def _build_context(
        self,
        target_name: str,
        target_info: Dict[str, Any],
        source_name: str,
        source_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build template context for a source-target pair."""
        columns = source_data["columns"]

        # Find key column and attributes for this target from this source
        attributes = []
        key_column = None

        for col in columns:
            if col.get("target"):
                for target_conn in col["target"]:
                    # Key column
                    if target_conn.get("target_name") == target_name:
                        key_column = col["column"]
                    # Attribute
                    if target_conn.get("attribute_of") == target_name:
                        attributes.append(
                            {
                                "column": col["column"],
                                "target_attribute": target_conn.get("target_attribute"),
                                "multiactive_key": target_conn.get("multiactive_key"),
                            }
                        )

        context = {
            "target_name": target_name,
            "source_name": source_name,
            "source_model": f"stg_{source_name}",
            "columns": columns,
            "attributes": attributes,
            "key_column": key_column,
        }

        # Add type-specific fields
        if self.target_type == "entity":
            context["entity_name"] = target_name
            context["entity_key_column"] = key_column
        else:
            entities = target_info.get("entities", [])
            context["relation_name"] = target_name
            context["entities"] = entities
            context["entities_joined"] = "_".join(entities)

        return context
