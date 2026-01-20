"""Source generator - generates models for sources."""

from pathlib import Path
from typing import Dict, List, Any, Optional

from .base import BaseGenerator


class SourceGenerator(BaseGenerator):
    """Generator for source-level templates."""

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Generate source domain models for all sources with target connections."""
        generated_files: List[str] = []

        # Get template configs for source domain
        templates = self.get_domain_templates("source")

        for template_key, template_config in templates.items():
            condition = template_config.get("condition")

            for source_name, source_info in source_models.items():
                # Skip sources with no connections
                if not source_info.get("connected_targets"):
                    continue

                context = self._build_source_context(source_name, source_info, targets_by_name)

                if self.check_condition(condition, context):
                    filepath = self._render_and_write(template_config, context, output_dir)
                    if filepath:
                        generated_files.append(filepath)

        return generated_files

    def _build_source_context(
        self,
        source_name: str,
        source_info: Dict[str, Any],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build template context for a source."""
        columns = source_info["columns"]

        return {
            "source_name": source_name,
            "columns": columns,
            "targets": targets_by_name,
        }

    def _render_and_write(
        self,
        template_config: Dict[str, Any],
        context: Dict[str, Any],
        output_dir: Path,
    ) -> Optional[str]:
        """Render template and write to file."""
        template_name = template_config["template"]
        filename_pattern = template_config["filename"]

        # Format filename with context (includes path)
        filepath = self.format_filename(filename_pattern, context)

        # Render template
        sql_content = self.render_template(template_name, **context)

        # Write file
        return self._write_file(output_dir, filepath, sql_content)
