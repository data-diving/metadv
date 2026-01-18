"""Validation: Check that entity targets have source connections."""

from typing import List
from .base import BaseValidator, ValidationContext, ValidationMessage


class EntityNoSourceValidator(BaseValidator):
    """Warns when an entity target has no source column with entity_name connection."""

    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        messages = []

        for target_name, target_info in ctx.target_map.items():
            target_type = target_info.get("type", "entity")

            # Only check entity targets
            if target_type == "entity":
                if target_name not in ctx.entity_sources:
                    messages.append(
                        ValidationMessage(
                            type="warning",
                            code="entity_no_source",
                            message=f"Entity target '{target_name}' has no source column with entity_name connection",
                        )
                    )

        return messages
