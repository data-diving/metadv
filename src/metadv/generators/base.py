"""Base generator class and shared utilities."""

import json
from pathlib import Path
from string import Template
from typing import Dict, List, Any
from abc import ABC, abstractmethod


class BaseGenerator(ABC):
    """Base class for all Data Vault model generators."""

    TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

    def __init__(self, package_name: str, package_prefix: str):
        """
        Initialize the generator.

        Args:
            package_name: Template package name (e.g., 'datavault-uk/automate_dv')
            package_prefix: The dbt package prefix to use (e.g., 'automate_dv', 'datavault4dbt')
        """
        self.package_name = package_name
        self.package_prefix = package_prefix
        self.template_path = self.TEMPLATES_DIR / package_name

    def render_template(self, template_name: str, **kwargs) -> str:
        """
        Load and render a template with placeholder substitution.

        Args:
            template_name: Name of the template file
            **kwargs: Variables to substitute (use ${var_name} placeholders in template)

        Returns:
            Rendered template string
        """
        template_file = self.template_path / template_name
        with open(template_file, "r", encoding="utf-8") as f:
            template = Template(f.read())
        # Convert dicts/lists to JSON strings
        substitutions = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in kwargs.items()
        }
        return template.substitute(substitutions)

    @abstractmethod
    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """
        Generate SQL model files.

        Args:
            output_dir: Directory to write generated files
            source_models: Dictionary of source models with column info
            targets_by_name: Dictionary of targets by name

        Returns:
            List of generated file paths
        """
        pass

    @abstractmethod
    def render_sql(self, **kwargs) -> str:
        """
        Render SQL content for a model.

        Returns:
            SQL content as string
        """
        pass

    def _get_stage_ref(self, source: str) -> str:
        """Get the stage model reference name."""
        return f"stg_{source}"

    def _get_unique_stage_models(self, source_refs: List[Dict[str, Any]]) -> List[str]:
        """Get unique stage model references from source refs."""
        stage_models = []
        seen_stages = set()
        for ref in source_refs:
            stage_ref = self._get_stage_ref(ref["source"])
            if stage_ref not in seen_stages:
                stage_models.append(stage_ref)
                seen_stages.add(stage_ref)
        return stage_models
