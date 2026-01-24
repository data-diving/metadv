#!/usr/bin/env python3
"""
MetaDV Generator - SQL model generation.

This module can be used in two modes:
1. As part of the backend server (imported by routes.py)
2. As a standalone CLI tool for isolated execution

Usage (standalone):
    python -m metadv.generator /path/to/dbt/project
    python -m metadv.generator /path/to/dbt/project --validate-only
    python -m metadv.generator /path/to/dbt/project --output /path/to/output

Or if running from the metadv folder directly:
    python generator.py /path/to/dbt/project
"""

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from .generators import SourceGenerator, SourceTargetGenerator, TargetGenerator
from .validations import ValidationContext, ValidationMessage, run_validations


@dataclass
class ValidationResult:
    """Result of metadv.yml validation."""

    success: bool
    error: Optional[str] = None
    errors: List[ValidationMessage] = field(default_factory=list)
    warnings: List[ValidationMessage] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "error": self.error,
            "errors": [e.to_dict() for e in self.errors],
            "warnings": [w.to_dict() for w in self.warnings],
            "summary": self.summary,
        }


@dataclass
class MetaDVData:
    """Parsed metadv.yml data."""

    targets: List[Dict[str, Any]]
    source_columns: List[Dict[str, Any]]
    raw: Dict[str, Any]


class MetaDVGenerator:
    """
    MetaDV Generator for SQL model generation.

    Can be used as a library or standalone CLI tool.
    """

    # Templates directory for package discovery
    TEMPLATES_DIR = Path(__file__).parent / "templates"

    @classmethod
    def get_available_packages(cls) -> List[str]:
        """Discover available template packages from the templates directory."""
        packages = []
        for item in cls.TEMPLATES_DIR.iterdir():
            if item.is_dir() and item.name != "__pycache__":
                templates_yml = item / "templates.yml"
                if templates_yml.exists():
                    packages.append(item.name)
                else:
                    for subitem in item.iterdir():
                        if subitem.is_dir() and (subitem / "templates.yml").exists():
                            packages.append(f"{item.name}/{subitem.name}")
        return sorted(packages)

    def __init__(self, project_path: str, package_name: str):
        """
        Initialize the generator with a dbt project path.

        Args:
            project_path: Path to the dbt project root directory
            package_name: Name of the template package to use (e.g., 'datavault-uk/automate_dv')
        """
        self.project_path = Path(project_path).expanduser().resolve()
        self.metadv_path = self.project_path / "models" / "metadv"
        self.metadv_yml_path = self.metadv_path / "metadv.yml"
        self._data: Optional[MetaDVData] = None
        self._raw_content: Optional[Dict[str, Any]] = None
        self.package_name = package_name

        # Read custom templates directory from metadv.yml if it exists
        custom_templates_dir = self._read_custom_templates_dir()

        # Initialize domain-based generators with optional custom templates directory
        kwargs = {"custom_templates_dir": custom_templates_dir} if custom_templates_dir else {}

        # Target-level: one file per target (hub, link, dim, fact)
        self._entity_generator = TargetGenerator(self.package_name, "entity", **kwargs)
        self._relation_generator = TargetGenerator(self.package_name, "relation", **kwargs)
        # Source-target level: one file per source-target pair (sat, SCD targets)
        self._entity_source_target_generator = SourceTargetGenerator(
            self.package_name, "entity", **kwargs
        )
        self._relation_source_target_generator = SourceTargetGenerator(
            self.package_name, "relation", **kwargs
        )
        # Source-level: one file per source (stage)
        self._source_generator = SourceGenerator(self.package_name, **kwargs)

    def _read_custom_templates_dir(self) -> Optional[Path]:
        """
        Read templates-dir from metadv.yml if it exists.

        Returns:
            Path to custom templates directory, or None if not configured
        """
        if not self.metadv_yml_path.exists():
            return None

        try:
            with open(self.metadv_yml_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)

            metadv_section = content.get("metadv", {}) or {}
            templates_dir = metadv_section.get("templates-dir")

            if templates_dir:
                # Resolve relative to project path
                templates_path = Path(templates_dir)
                if not templates_path.is_absolute():
                    templates_path = self.project_path / templates_path
                return templates_path.resolve()

            return None
        except Exception:
            return None

    def _read_custom_validations_dir(self) -> Optional[Path]:
        """
        Read validations-dir from metadv.yml if it exists.

        Returns:
            Path to custom validations directory, or None if not configured
        """
        if not self.metadv_yml_path.exists():
            return None

        try:
            with open(self.metadv_yml_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)

            metadv_section = content.get("metadv", {}) or {}
            validations_dir = metadv_section.get("validations-dir")

            if validations_dir:
                # Resolve relative to project path
                validations_path = Path(validations_dir)
                if not validations_path.is_absolute():
                    validations_path = self.project_path / validations_path
                return validations_path.resolve()

            return None
        except Exception:
            return None

    def read(self) -> Tuple[bool, Optional[str], Optional[MetaDVData]]:
        """
        Read and parse metadv.yml file.

        Returns:
            Tuple of (success, error_message, data)
        """
        if not self.project_path.exists():
            return False, "Project path does not exist", None

        if not self.metadv_yml_path.exists():
            return False, "metadv.yml not found. Please initialize MetaDV first.", None

        try:
            with open(self.metadv_yml_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)

            self._raw_content = content

            targets = []
            metadv_section = content.get("metadv", {})
            if metadv_section and "targets" in metadv_section:
                targets = metadv_section.get("targets", [])

            source_columns = []
            # Sources are under metadv key (same as targets)
            # Each source has a name (model name) and columns directly
            sources = metadv_section.get("sources", []) if metadv_section else []
            for source in sources:
                source_name = source.get("name", "")
                columns = source.get("columns", [])
                for column in columns:
                    col_name = column.get("name", "")

                    target = column.get("target")

                    col_data = {
                        "source": source_name,
                        "column": col_name,
                        "target": target if target else None,
                    }

                    source_columns.append(col_data)

            self._data = MetaDVData(targets=targets, source_columns=source_columns, raw=content)

            return True, None, self._data

        except Exception as e:
            return False, str(e), None

    def validate(self) -> ValidationResult:
        """
        Validate metadv.yml configuration using auto-discovered validators.

        Validators are automatically discovered from the validations folder
        and optionally from a custom validations-dir specified in metadv.yml.
        Custom validators with the same class name as built-in ones will
        override the built-in validators.

        Returns a ValidationResult with errors and warnings.
        """
        if not self.project_path.exists():
            return ValidationResult(success=False, error="Project path does not exist")

        if not self.metadv_yml_path.exists():
            return ValidationResult(
                success=False, error="metadv.yml not found. Please initialize MetaDV first."
            )

        try:
            with open(self.metadv_yml_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)

            # Build validation context
            ctx = self._build_validation_context(content)

            # Read custom validations directory from metadv.yml
            custom_validations_dir = self._read_custom_validations_dir()

            # Run all auto-discovered validators (built-in + custom)
            messages = run_validations(ctx, custom_validations_dir)

            # Separate errors and warnings
            errors = [m for m in messages if m.type == "error"]
            warnings = [m for m in messages if m.type == "warning"]

            return ValidationResult(
                success=True,
                errors=errors,
                warnings=warnings,
                summary={
                    "total_targets": len(ctx.target_map),
                    "total_columns": ctx.total_columns,
                    "columns_with_connections": ctx.columns_with_connections,
                    "error_count": len(errors),
                    "warning_count": len(warnings),
                },
            )

        except Exception as e:
            return ValidationResult(success=False, error=str(e))

    def _build_validation_context(self, content: Dict[str, Any]) -> ValidationContext:
        """Build ValidationContext from metadv.yml content."""
        metadv_section = content.get("metadv", {}) or {}
        targets = metadv_section.get("targets", []) or []
        sources = metadv_section.get("sources", []) or []

        # Build target map
        target_map: Dict[str, Dict[str, Any]] = {}
        for target in targets:
            target_name = target.get("name", "")
            target_type = target.get("type", "entity")
            target_map[target_name] = {
                "type": target_type,
                "description": target.get("description"),
                "entities": target.get("entities", []),
            }

        # Track connections
        entity_sources: set = set()
        source_entity_connections: Dict[str, set] = {}
        source_relation_connections: Dict[str, set] = {}
        source_relation_entity_positions: Dict[str, Dict[str, set]] = {}
        total_columns = 0
        columns_with_connections = 0

        for source in sources:
            source_name = source.get("name", "")
            columns = source.get("columns", [])

            if source_name not in source_entity_connections:
                source_entity_connections[source_name] = set()
            if source_name not in source_relation_connections:
                source_relation_connections[source_name] = set()
            if source_name not in source_relation_entity_positions:
                source_relation_entity_positions[source_name] = {}

            for column in columns:
                total_columns += 1
                has_connection = False

                target = column.get("target")

                if target:
                    for target_conn in target:
                        # Check if this is an attribute connection
                        if target_conn.get("attribute_of"):
                            has_connection = True
                        # Or an entity/relation key connection
                        elif target_conn.get("target_name"):
                            target_name = target_conn.get("target_name")
                            entity_name = target_conn.get("entity_name")
                            entity_index = target_conn.get("entity_index")

                            target_info = target_map.get(target_name, {})
                            target_type = target_info.get("type", "entity")

                            if target_type == "relation":
                                source_relation_connections[source_name].add(target_name)
                                if entity_name:
                                    entity_sources.add(entity_name)
                                    source_entity_connections[source_name].add(entity_name)
                                    if (
                                        target_name
                                        not in source_relation_entity_positions[source_name]
                                    ):
                                        source_relation_entity_positions[source_name][
                                            target_name
                                        ] = set()
                                    source_relation_entity_positions[source_name][target_name].add(
                                        (entity_name, entity_index)
                                    )
                            else:
                                entity_sources.add(target_name)
                                source_entity_connections[source_name].add(target_name)

                            has_connection = True

                if has_connection:
                    columns_with_connections += 1

        return ValidationContext(
            content=content,
            target_map=target_map,
            sources=sources,
            entity_sources=entity_sources,
            source_entity_connections=source_entity_connections,
            source_relation_connections=source_relation_connections,
            source_relation_entity_positions=source_relation_entity_positions,
            total_columns=total_columns,
            columns_with_connections=columns_with_connections,
        )

    def generate(self, output_path: Optional[str] = None) -> Tuple[bool, Optional[str], List[str]]:
        """
        Generate SQL models from metadv.yml configuration.

        Args:
            output_path: Optional custom output directory. If None, uses metadv folder.

        Returns:
            Tuple of (success, error_message, list_of_generated_files)
        """
        # Read the data first
        success, error, data = self.read()
        if not success:
            return False, error, []

        # Validate before generating
        validation = self.validate()
        if validation.errors:
            error_messages = [e.message for e in validation.errors]
            return False, f"Validation errors: {'; '.join(error_messages)}", []

        # Determine output directory
        output_dir = Path(output_path) if output_path else self.metadv_path
        output_dir.mkdir(parents=True, exist_ok=True)

        generated_files: List[str] = []

        try:
            # Clean up existing generated folders before generating new files
            self._cleanup_generated_folders(output_dir)

            # Build data structures for generation
            targets_by_name = {t["name"]: t for t in data.targets}

            # Group source columns by source (model name)
            source_models: Dict[str, Dict[str, Any]] = {}
            for col in data.source_columns:
                source_name = col["source"]
                if source_name not in source_models:
                    source_models[source_name] = {
                        "source": source_name,
                        "columns": [],
                        "connected_targets": set(),
                    }
                source_models[source_name]["columns"].append(col)

                # Track connected targets from the unified target array structure
                if col.get("target"):
                    for target_conn in col["target"]:
                        # Entity/relation key connection
                        target_name = target_conn.get("target_name")
                        entity_name = target_conn.get(
                            "entity_name"
                        )  # Only for relation connections
                        if target_name:
                            source_models[source_name]["connected_targets"].add(target_name)
                        # Also track the entity for relation connections
                        if entity_name:
                            source_models[source_name]["connected_targets"].add(entity_name)
                        # Attribute connection
                        attr_target = target_conn.get("attribute_of")
                        if attr_target:
                            source_models[source_name]["connected_targets"].add(attr_target)

            # 1. Generate entity target models (hub, dim)
            entity_files = self._entity_generator.generate(
                output_dir, source_models, targets_by_name
            )
            generated_files.extend(entity_files)

            # 2. Generate relation target models (link, fact)
            relation_files = self._relation_generator.generate(
                output_dir, source_models, targets_by_name
            )
            generated_files.extend(relation_files)

            # 3. Generate entity source-target models
            entity_source_target_files = self._entity_source_target_generator.generate(
                output_dir, source_models, targets_by_name
            )
            generated_files.extend(entity_source_target_files)

            # 4. Generate relation source-target models
            relation_source_target_files = self._relation_source_target_generator.generate(
                output_dir, source_models, targets_by_name
            )
            generated_files.extend(relation_source_target_files)

            # 5. Generate source models (stage)
            source_files = self._source_generator.generate(
                output_dir, source_models, targets_by_name
            )
            generated_files.extend(source_files)

            return True, None, generated_files

        except Exception as e:
            return False, f"Generation error: {str(e)}", generated_files

    def _cleanup_generated_folders(self, output_dir: Path) -> None:
        """Delete everything in output folder except metadv.yml."""
        import shutil

        for item in output_dir.iterdir():
            if item.name == "metadv.yml":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


def validate_metadv(
    project_path: str, package_name: str = "datavault-uk/automate_dv"
) -> Dict[str, Any]:
    """
    Convenience function for validating metadv.yml.

    Args:
        project_path: Path to the dbt project
        package_name: Name of the package or folder

    Returns:
        Dictionary with validation results
    """
    generator = MetaDVGenerator(project_path, package_name)
    result = generator.validate()
    return result.to_dict()


def read_metadv(
    project_path: str, package_name: str = "datavault-uk/automate_dv"
) -> Dict[str, Any]:
    """
    Convenience function for reading metadv.yml.

    Args:
        project_path: Path to the dbt project
        package_name: Name of the package or folder

    Returns:
        Dictionary with read results
    """
    generator = MetaDVGenerator(project_path, package_name)
    success, error, data = generator.read()

    if not success:
        return {"success": False, "error": error, "data": None}

    return {
        "success": True,
        "error": None,
        "data": {"targets": data.targets, "source_columns": data.source_columns, "raw": data.raw},
        "path": str(generator.metadv_yml_path),
    }


def main():
    """CLI entry point for standalone execution."""
    parser = argparse.ArgumentParser(
        description="MetaDV Generator - Generate SQL models from metadv.yml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/dbt/project --package datavault-uk/automate_dv
  %(prog)s /path/to/dbt/project --package scalefreecom/datavault4dbt
  %(prog)s /path/to/dbt/project --package datavault-uk/automate_dv --validate-only
  %(prog)s /path/to/dbt/project --package datavault-uk/automate_dv --output ./output
        """,
    )

    parser.add_argument("project_path", help="Path to the dbt project root directory")

    parser.add_argument(
        "--package",
        "-p",
        required=True,
        choices=MetaDVGenerator.get_available_packages(),
        help="Template package/folder to use for SQL generation",
    )

    parser.add_argument(
        "--validate-only",
        "-v",
        action="store_true",
        help="Only validate metadv.yml without generating SQL models",
    )

    parser.add_argument("--output", "-o", help="Custom output directory for generated SQL files")

    parser.add_argument(
        "--verbose", action="store_true", help="Show detailed output including warnings"
    )

    parser.add_argument("--json", action="store_true", help="Output results in JSON format")

    args = parser.parse_args()

    # Create generator
    generator = MetaDVGenerator(args.project_path, args.package)

    # Validate
    validation = generator.validate()

    if args.validate_only:
        if args.json:
            import json

            print(json.dumps(validation.to_dict(), indent=2))
        else:
            print(f"\nValidation Results for: {generator.metadv_yml_path}")
            print("=" * 60)

            if validation.error:
                print(f"Error: {validation.error}")
                sys.exit(1)

            summary = validation.summary
            print(f"Targets: {summary.get('total_targets', 0)}")
            print(f"Source columns: {summary.get('total_columns', 0)}")
            print(f"Columns with connections: {summary.get('columns_with_connections', 0)}")
            print()

            if validation.errors:
                print(f"Errors ({len(validation.errors)}):")
                for err in validation.errors:
                    print(f"  - {err.message}")
                print()

            if args.verbose and validation.warnings:
                print(f"Warnings ({len(validation.warnings)}):")
                for warn in validation.warnings:
                    print(f"  - {warn.message}")
                print()

            if validation.errors:
                print("Validation FAILED - please fix errors before generating")
                sys.exit(1)
            else:
                print("Validation PASSED")
                if validation.warnings and not args.verbose:
                    print(f"  ({len(validation.warnings)} warnings - use --verbose to see)")

        sys.exit(0 if not validation.errors else 1)

    # Generate SQL models
    success, error, files = generator.generate(args.output)

    if args.json:
        import json

        print(
            json.dumps(
                {
                    "success": success,
                    "error": error,
                    "generated_files": files,
                    "validation": validation.to_dict(),
                },
                indent=2,
            )
        )
    else:
        if not success:
            print(f"Error: {error}")
            sys.exit(1)

        print(f"\nGenerated {len(files)} SQL model(s):")
        for f in files:
            print(f"  - {f}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
