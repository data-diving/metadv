"""Validation: Check that entity targets have descriptions."""

from typing import List

from .base import BaseValidator, ValidationContext, ValidationMessage


class EntityNoDescriptionValidator(BaseValidator):
    """Warns when an entity target has no description.

    Only applies to entity targets, not relations.
    """

    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        messages = []

        for target_name, target_info in ctx.target_map.items():
            target_type = target_info.get("type", "entity")
            description = target_info.get("description")

            # Only check entities, not relations
            if target_type == "entity" and not description:
                messages.append(
                    ValidationMessage(
                        type="warning",
                        code="entity_no_description",
                        message=f"Entity '{target_name}' has no description",
                    )
                )

        return messages
