"""Attribute generator - generates models for individual attributes."""

from pathlib import Path
from typing import Any, Dict, List

from .base import BaseGenerator


class AttributeGenerator(BaseGenerator):
    """Generator for individual attribute templates."""

    def __init__(self, package_name: str, target_type: str, **kwargs):
        """Initialize generator for a specific target type.

        Args:
            package_name: Template package/folder name
            target_type: Either "entity" or "relation"
        """
        super().__init__(package_name, **kwargs)
        self.target_type = target_type

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Generate models for all individual attributes using scope:attribute templates."""
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

        # Get template configs for this domain, filtered to scope: attribute
        templates = self.get_domain_templates(self.target_type)
        attribute_templates = {
            key: config for key, config in templates.items() if config.get("scope") == "attribute"
        }

        for template_key, template_config in attribute_templates.items():
            condition = template_config.get("condition")

            for target_name, target_info in targets.items():
                # Find all attributes for this target from all sources
                attributes = self._find_target_attributes(target_name, source_models)

                for attr_info in attributes:
                    context = self._build_context(
                        target_name,
                        target_info,
                        attr_info,
                    )
                    if self.check_condition(condition, context):
                        filepath = self._render_and_write(template_config, context, output_dir)
                        if filepath:
                            generated_files.append(filepath)

        return generated_files

    def _find_target_attributes(
        self,
        target_name: str,
        source_models: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Find all individual attributes connected to a target across all sources.

        Returns a list of attribute info dicts, each containing:
        - source_name: The source model name
        - column: The source column name
        - attribute_name: The target attribute name (or column name if not specified)
        - key_column: The key column for this target in this source
        - multiactive_key: Whether this is a multiactive key
        """
        attributes = []

        for source_name, source_info in source_models.items():
            key_column = None

            # First pass: find the key column for this target in this source
            for col in source_info["columns"]:
                if col.get("target"):
                    for target_conn in col["target"]:
                        if target_conn.get("target_name") == target_name:
                            key_column = col["column"]
                            break
                if key_column:
                    break

            # Second pass: find all attributes for this target
            for col in source_info["columns"]:
                if col.get("target"):
                    for target_conn in col["target"]:
                        if target_conn.get("attribute_of") == target_name:
                            attribute_name = target_conn.get("target_attribute") or col["column"]
                            attributes.append(
                                {
                                    "source_name": source_name,
                                    "column": col["column"],
                                    "attribute_name": attribute_name,
                                    "key_column": key_column,
                                    "multiactive_key": target_conn.get("multiactive_key"),
                                }
                            )

        return attributes

    def _build_context(
        self,
        target_name: str,
        target_info: Dict[str, Any],
        attr_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build template context for an individual attribute."""
        context = {
            "target_name": target_name,
            "source_name": attr_info["source_name"],
            "source_model": f"stg_{attr_info['source_name']}",
            "column": attr_info["column"],
            "attribute_name": attr_info["attribute_name"],
            "key_column": attr_info["key_column"],
            "multiactive_key": attr_info.get("multiactive_key"),
        }

        # Add type-specific fields
        if self.target_type == "entity":
            context["entity_name"] = target_name
            context["entity_key_column"] = attr_info["key_column"]
        else:
            entities = target_info.get("entities", [])
            context["relation_name"] = target_name
            context["entities"] = entities
            context["entities_joined"] = "_".join(entities)

        return context
