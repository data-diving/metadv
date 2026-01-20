"""Base generator class and shared utilities."""

import json
from pathlib import Path
from string import Template
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod

import yaml


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
        self._templates_config = self._load_templates_config()

    def _load_templates_config(self) -> Dict[str, Any]:
        """Load templates.yml configuration."""
        config_path = self.template_path / "templates.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_domain_templates(self, domain: str) -> Dict[str, Dict[str, Any]]:
        """Get all template configs for a domain (entity/relation/source)."""
        return self._templates_config.get(domain, {})

    def check_condition(self, condition: Optional[str], context: Dict[str, Any]) -> bool:
        """Check if condition is met for rendering."""
        if not condition:
            return True
        if condition == "has_attributes":
            return bool(context.get("attributes"))
        if condition == "is_multiactive":
            return bool(context.get("multiactive_key_columns"))
        return True

    def format_filename(self, pattern: str, context: Dict[str, Any]) -> str:
        """Format filename pattern with context variables."""
        return pattern.format(**context)

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

    def _write_file(self, output_dir: Path, filepath: str, content: str) -> str:
        """Write content to a file and return the path."""
        full_path = output_dir / filepath
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
        return str(full_path)
