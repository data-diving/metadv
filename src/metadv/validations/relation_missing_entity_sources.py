"""Validation: Check that relation targets have source columns connected."""

from typing import List

from .base import BaseValidator, ValidationContext, ValidationMessage


class RelationMissingEntitySourcesValidator(BaseValidator):
    """Warns when relation targets have no source columns connected.

    Only produces warnings - entity-level errors are handled by SourceMissingRelationEntitiesValidator.

    - Warning if relation has no explicit connections at all
    """

    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        messages = []

        # Get all relations that have at least one source connected
        relations_with_sources = set()
        for source_name, connected_relations in ctx.source_relation_connections.items():
            relations_with_sources.update(connected_relations)

        # Check each relation target
        for target_name, target_info in ctx.target_map.items():
            target_type = target_info.get("type", "entity")

            # Only check relation targets
            if target_type != "relation":
                continue

            # Warn if relation has no sources connected at all
            if target_name not in relations_with_sources:
                messages.append(
                    ValidationMessage(
                        type="warning",
                        code="relation_no_sources",
                        message=f"Relation target '{target_name}' has no source columns connected to it",
                    )
                )

        return messages
