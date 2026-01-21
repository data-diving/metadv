"""Validation: Check that sources connected to relations have all required entities."""

from typing import List

from .base import BaseValidator, ValidationContext, ValidationMessage


class SourceMissingRelationEntitiesValidator(BaseValidator):
    """Error when a source is explicitly connected to a relation but missing some entity columns.

    Only checks sources that have explicit relation connections (via target_name pointing to a relation).
    Connecting to an entity does NOT imply connection to relations that use that entity.

    For self-links (where same entity appears multiple times), each position must be connected separately.
    """

    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        messages = []

        # Only check sources that are explicitly connected to relations
        for source_name, connected_relations in ctx.source_relation_connections.items():
            source_positions = ctx.source_relation_entity_positions.get(source_name, {})

            for relation_name in connected_relations:
                relation_info = ctx.target_map.get(relation_name, {})
                required_entities = relation_info.get("entities", [])

                # Get the positions connected for this source and relation
                connected_positions = source_positions.get(relation_name, set())

                # Check if this is a self-link (same entity appears multiple times)
                is_self_link = len(required_entities) != len(set(required_entities))

                if is_self_link:
                    # For self-links, we need to check that each position is connected
                    # required_entities might be ['order', 'order'] for a self-link
                    missing_positions = []
                    for i, entity in enumerate(required_entities):
                        # Check if this specific position (entity, index) is connected
                        position_connected = any(
                            pos[0] == entity and pos[1] == i for pos in connected_positions
                        )
                        if not position_connected:
                            missing_positions.append(f"{entity} (position {i + 1})")

                    if missing_positions:
                        messages.append(
                            ValidationMessage(
                                type="error",
                                code="source_missing_relation_entities",
                                message=f"Source '{source_name}' is connected to relation '{relation_name}' but is missing entity columns for: {', '.join(missing_positions)}",
                            )
                        )
                else:
                    # For regular relations, just check entity names
                    connected_entity_names = {pos[0] for pos in connected_positions}
                    missing_entities = [
                        e for e in required_entities if e not in connected_entity_names
                    ]

                    if missing_entities:
                        messages.append(
                            ValidationMessage(
                                type="error",
                                code="source_missing_relation_entities",
                                message=f"Source '{source_name}' is connected to relation '{relation_name}' but is missing entity columns for: {', '.join(missing_entities)}",
                            )
                        )

        return messages
