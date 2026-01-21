"""
MetaDV - Metadata-Driven Model Generator

A Python library for generating SQL models from a declarative YAML configuration.
Supports multiple data modeling approaches including Data Vault 2.0 and Dimensional
Modeling, with template packages for popular dbt libraries like automate_dv,
datavault4dbt, and dimensional.

Example usage:
    from metadv import MetaDVGenerator

    generator = MetaDVGenerator('/path/to/dbt/project', 'datavault-uk/automate_dv')

    # Validate configuration
    result = generator.validate()
    if result.errors:
        print("Errors:", [e.message for e in result.errors])

    # Generate SQL models
    success, error, files = generator.generate()
"""

__version__ = "0.1.0"

from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import yaml
else:
    try:
        import yaml
    except ImportError:
        yaml = None


# Supported template packages (in order of preference for auto-detection)
SUPPORTED_DV_PACKAGES = [
    "datavault-uk/automate_dv",
    "scalefreecom/datavault4dbt",
    "dimensional",
]


def detect_installed_dv_package(project_path: Path) -> Optional[str]:
    """Detect the first installed template package from packages.yml or dependencies.yml.

    Args:
        project_path: Path to the dbt project root directory

    Returns:
        The package name if found, None otherwise.
    """
    if yaml is None:
        return None

    if isinstance(project_path, str):
        project_path = Path(project_path)

    for deps_filename in ["packages.yml", "dependencies.yml"]:
        deps_file = project_path / deps_filename
        if deps_file.exists():
            try:
                with open(deps_file, "r", encoding="utf-8") as f:
                    deps_content = yaml.safe_load(f)

                if not deps_content:
                    continue

                packages = deps_content.get("packages", [])
                for pkg in packages:
                    if isinstance(pkg, dict):
                        pkg_name = pkg.get("package", "")
                        if pkg_name.lower() in [p.lower() for p in SUPPORTED_DV_PACKAGES]:
                            return pkg_name
            except Exception:
                continue

    return None


# Import generator components
from .generator import MetaDVGenerator, read_metadv, validate_metadv

__all__ = [
    "__version__",
    "detect_installed_dv_package",
    "SUPPORTED_DV_PACKAGES",
    "MetaDVGenerator",
    "validate_metadv",
    "read_metadv",
]
