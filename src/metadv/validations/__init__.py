"""MetaDV Validations - Auto-discovery of validation rules.

To add a new validation:
1. Create a new .py file in this folder (or in a custom validations-dir)
2. Create a class that inherits from BaseValidator
3. Implement the validate(ctx: ValidationContext) method
4. The validator will be automatically discovered and run

Example:
    # my_validation.py
    from metadv.validations.base import BaseValidator, ValidationContext, ValidationMessage

    class MyValidator(BaseValidator):
        def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
            messages = []
            if some_condition:
                messages.append(ValidationMessage(
                    type='error',
                    code='my_error',
                    message='Something is wrong'
                ))
            return messages

Custom validations directory:
    Set `validations-dir` in your metadv.yml to point to a directory containing
    custom validators. Custom validators with the same class name as built-in
    validators will override the built-in ones.
"""

import importlib.util
import inspect
import pkgutil
import sys
from pathlib import Path
from typing import List, Optional, Type

from .base import BaseValidator, ValidationContext, ValidationMessage


def _load_validators_from_package() -> List[Type[BaseValidator]]:
    """Load validators from the built-in validations package.

    Returns:
        List of validator classes from the built-in package
    """
    validators = []
    package_path = Path(__file__).parent

    # Iterate through all modules in the validations package
    for module_info in pkgutil.iter_modules([str(package_path)]):
        if module_info.name.startswith("_") or module_info.name == "base":
            continue

        # Import the module
        module = importlib.import_module(f".{module_info.name}", package=__name__)

        # Find all classes that inherit from BaseValidator
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, BaseValidator)
                and obj is not BaseValidator
                and obj.__module__ == module.__name__
            ):
                validators.append(obj)

    return validators


def _load_validators_from_path(path: Path) -> List[Type[BaseValidator]]:
    """Load validators from a custom directory path.

    Args:
        path: Directory containing validator Python files

    Returns:
        List of validator classes from the custom directory
    """
    validators = []

    if not path.exists() or not path.is_dir():
        return validators

    # Find all .py files in the directory
    for py_file in path.glob("*.py"):
        if py_file.name.startswith("_"):
            continue

        module_name = f"metadv_custom_validators_{py_file.stem}"

        try:
            # Load the module from the file path
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                continue

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Find all classes that inherit from BaseValidator
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, BaseValidator)
                    and obj is not BaseValidator
                    and obj.__module__ == module_name
                ):
                    validators.append(obj)

        except Exception:
            # Skip files that fail to load
            continue

    return validators


def discover_validators(
    custom_validations_dir: Optional[Path] = None,
) -> List[Type[BaseValidator]]:
    """Discover all validator classes from built-in and custom directories.

    Custom validators with the same class name as built-in validators
    will override the built-in ones.

    Args:
        custom_validations_dir: Optional path to custom validations directory

    Returns:
        List of validator classes (not instances)
    """
    # Use dict to track validators by class name (allows overriding)
    validators_by_name: dict[str, Type[BaseValidator]] = {}

    # First load built-in validators
    for validator_class in _load_validators_from_package():
        validators_by_name[validator_class.__name__] = validator_class

    # Then load from custom directory if provided (can override built-in)
    if custom_validations_dir:
        for validator_class in _load_validators_from_path(custom_validations_dir):
            validators_by_name[validator_class.__name__] = validator_class

    return list(validators_by_name.values())


def run_validations(
    ctx: ValidationContext,
    custom_validations_dir: Optional[Path] = None,
) -> List[ValidationMessage]:
    """Run all discovered validators and collect messages.

    Args:
        ctx: ValidationContext with parsed data
        custom_validations_dir: Optional path to custom validations directory

    Returns:
        List of all ValidationMessages from all validators
    """
    messages = []
    validator_classes = discover_validators(custom_validations_dir)

    for validator_class in validator_classes:
        validator = validator_class()
        validator_messages = validator.validate(ctx)
        messages.extend(validator_messages)

    return messages


__all__ = [
    "BaseValidator",
    "ValidationContext",
    "ValidationMessage",
    "discover_validators",
    "run_validations",
]
