"""MetaDV SQL Templates.

This module provides SQL templates for different Data Vault packages.
"""

from pathlib import Path

TEMPLATES_DIR = Path(__file__).parent


def get_template_path(package: str, template_name: str) -> Path:
    """Get the path to a template file.

    Args:
        package: Package name ('automate_dv' or 'datavault4dbt')
        template_name: Template name ('stage', 'hub', 'link', 'sat')

    Returns:
        Path to the template file
    """
    return TEMPLATES_DIR / package / f"{template_name}.sql"


def load_template(package: str, template_name: str) -> str:
    """Load a template file content.

    Args:
        package: Package name ('automate_dv' or 'datavault4dbt')
        template_name: Template name ('stage', 'hub', 'link', 'sat')

    Returns:
        Template content as string
    """
    template_path = get_template_path(package, template_name)
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()
