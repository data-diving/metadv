"""Validation: Check that all source columns have connections."""

from typing import List
from .base import BaseValidator, ValidationContext, ValidationMessage


class ColumnNoConnectionValidator(BaseValidator):
    """Warns when a source column has no connection to any target."""

    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        messages = []

        for source in ctx.sources:
            source_name = source.get('name', '')
            columns = source.get('columns', [])

            for column in columns:
                # Unified structure: target array directly on column (no meta wrapper)
                target = column.get('target')

                # Backwards compatibility: check for old meta-wrapped format
                if target is None:
                    meta = column.get('meta', {}) or {}
                    target = meta.get('target')

                    # Even older formats: entity_name/attribute_of as separate fields
                    if target is None:
                        old_entity_name = meta.get('entity_name')
                        old_attribute_of = meta.get('attribute_of')
                        has_connection = bool(old_entity_name) or bool(old_attribute_of)
                    else:
                        # Check that target array has entries
                        has_connection = bool(target) and len(target) > 0
                else:
                    # Check unified target array for any connections (entity or attribute)
                    has_connection = bool(target) and len(target) > 0

                if not has_connection:
                    col_name = column.get('name', '')
                    messages.append(ValidationMessage(
                        type='warning',
                        code='column_no_connection',
                        message=f"Column '{source_name}.{col_name}' has no connection to any target"
                    ))

        return messages
