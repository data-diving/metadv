"""MetaDV SQL Generators.

This module provides domain-based SQL generators:
- TargetGenerator: One file per target (hub, link, dim, fact, anchor, tie)
- SourceTargetGenerator: One file per source-target pair (sat, SCD)
- AttributeGenerator: One file per attribute
- SourceGenerator: One file per source (stage)
"""

from .attribute import AttributeGenerator
from .source import SourceGenerator
from .source_target import SourceTargetGenerator
from .target import TargetGenerator

__all__ = ["TargetGenerator", "SourceTargetGenerator", "AttributeGenerator", "SourceGenerator"]
