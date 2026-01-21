"""Validation: Check that all source columns have connections."""

from typing import List

from .base import BaseValidator, ValidationContext, ValidationMessage


class ColumnNoConnectionValidator(BaseValidator):
    """Warns when a source column has no connection to any target."""

    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        messages = []

        for source in ctx.sources:
            source_name = source.get("name", "")
            columns = source.get("columns", [])

            for column in columns:
                target = column.get("target")
                has_connection = bool(target) and len(target) > 0

                if not has_connection:
                    col_name = column.get("name", "")
                    messages.append(
                        ValidationMessage(
                            type="warning",
                            code="column_no_connection",
                            message=f"Column '{source_name}.{col_name}' has no connection to any target",
                        )
                    )

        return messages
