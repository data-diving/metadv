"""MetaDV Validations - Auto-discovery of validation rules.

To add a new validation:
1. Create a new .py file in this folder
2. Create a class that inherits from BaseValidator
3. Implement the validate(ctx: ValidationContext) method
4. The validator will be automatically discovered and run

Example:
    # my_validation.py
    from .base import BaseValidator, ValidationContext, ValidationMessage

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
"""

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import List, Type

from .base import BaseValidator, ValidationContext, ValidationMessage


def discover_validators() -> List[Type[BaseValidator]]:
    """Discover all validator classes in the validations folder.

    Returns:
        List of validator classes (not instances)
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


def run_validations(ctx: ValidationContext) -> List[ValidationMessage]:
    """Run all discovered validators and collect messages.

    Args:
        ctx: ValidationContext with parsed data

    Returns:
        List of all ValidationMessages from all validators
    """
    messages = []
    validator_classes = discover_validators()

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
