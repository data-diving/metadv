"""Base classes and types for MetaDV validations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set


@dataclass
class ValidationMessage:
    """A single validation message (error or warning)."""

    type: str  # 'error' or 'warning'
    code: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return {"type": self.type, "code": self.code, "message": self.message}


@dataclass
class ValidationContext:
    """Context data available to all validators."""

    # Raw content from metadv.yml
    content: Dict[str, Any]

    # Parsed data structures
    target_map: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    sources: List[Dict[str, Any]] = field(default_factory=list)

    # Tracked connections
    entity_sources: Set[str] = field(default_factory=set)
    source_entity_connections: Dict[str, Set[str]] = field(default_factory=dict)
    source_relation_connections: Dict[str, Set[str]] = field(default_factory=dict)
    # For self-links: tracks which entity positions are connected per source per relation
    # Key: source_name, Value: dict of relation_name -> set of (entity_name, entity_index) tuples
    source_relation_entity_positions: Dict[str, Dict[str, Set[tuple]]] = field(default_factory=dict)

    # Statistics
    total_columns: int = 0
    columns_with_connections: int = 0


class BaseValidator(ABC):
    """Base class for all MetaDV validators.

    To create a new validator:
    1. Create a new file in the validations folder
    2. Create a class that inherits from BaseValidator
    3. Implement the validate() method
    4. The validator will be auto-discovered and run

    Example:
        class MyValidator(BaseValidator):
            def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
                messages = []
                # Your validation logic here
                if some_condition:
                    messages.append(ValidationMessage(
                        type='error',
                        code='my_error_code',
                        message='Something is wrong'
                    ))
                return messages
    """

    @abstractmethod
    def validate(self, ctx: ValidationContext) -> List[ValidationMessage]:
        """Run validation and return any messages.

        Args:
            ctx: ValidationContext with all parsed data

        Returns:
            List of ValidationMessage (errors and warnings)
        """
        pass
