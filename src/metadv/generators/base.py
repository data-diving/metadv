"""Base generator class and shared utilities."""

import json
from abc import ABC, abstractmethod
from pathlib import Path
from string import Template
from typing import Any, Dict, List, Optional

import yaml


class BaseGenerator(ABC):
    """Base class for all model generators."""

    TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

    def __init__(
        self, package_name: str, *args, custom_templates_dir: Optional[Path] = None, **kwargs
    ):
        """
        Initialize the generator.

        Args:
            package_name: Template package name (e.g., 'datavault-uk/automate_dv')
            custom_templates_dir: Optional custom templates directory to check first
        """
        self.package_name = package_name
        self.custom_templates_dir = custom_templates_dir

        # Determine template path: check custom dir first, then fall back to built-in
        self.template_path = self._resolve_template_path(package_name)
        with open(self.template_path / "templates.yml", "r", encoding="utf-8") as f:
            self._templates_config = yaml.safe_load(f)

    def _resolve_template_path(self, package_name: str) -> Path:
        """
        Resolve the template path, checking custom directory first.

        Args:
            package_name: Template package name

        Returns:
            Path to the template directory
        """
        package_path = package_name.lower()

        # Check custom templates directory first
        if self.custom_templates_dir:
            custom_path = self.custom_templates_dir / package_path
            if custom_path.exists() and (custom_path / "templates.yml").exists():
                return custom_path

        # Fall back to built-in templates
        return self.TEMPLATES_DIR / package_path

    def get_domain_templates(self, domain: str) -> Dict[str, Dict[str, Any]]:
        """Get template configs for a domain (entity/relation/source)."""
        return self._templates_config.get(domain, {})

    def check_condition(self, condition: Optional[str], context: Dict[str, Any]) -> bool:
        """Check if condition is met for rendering."""
        if not condition:
            return True
        if condition == "has_attributes":
            return bool(context.get("attributes"))
        if condition == "is_multiactive":
            # Extract multiactive status from attributes
            attributes = context.get("attributes", [])
            return any(attr.get("multiactive_key") for attr in attributes)
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

    def _render_and_write(
        self,
        template_config: Dict[str, Any],
        context: Dict[str, Any],
        output_dir: Path,
    ) -> Optional[str]:
        """Render template and write to file."""
        template_name = template_config["template"]
        filename_pattern = template_config["filename"]

        filepath = self.format_filename(filename_pattern, context)
        sql_content = self.render_template(template_name, **context)

        full_path = output_dir / filepath
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(sql_content)
        return str(full_path)
