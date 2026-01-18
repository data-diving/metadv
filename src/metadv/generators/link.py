"""Link model generator."""

from pathlib import Path
from typing import Dict, List, Any

from .base import BaseGenerator


class LinkGenerator(BaseGenerator):
    """Generator for link models."""

    def generate(
        self,
        output_dir: Path,
        source_models: Dict[str, Dict[str, Any]],
        targets_by_name: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Generate link models - one per relation target."""
        link_dir = output_dir / "link"
        link_dir.mkdir(parents=True, exist_ok=True)

        generated_files: List[str] = []

        for target_name, target_info in targets_by_name.items():
            if target_info.get("type") != "relation":
                continue

            entities = target_info.get("entities", [])
            if not entities:
                continue

            # Create filename from entities
            entities_suffix = "_".join(entities)
            filename = f"link_{entities_suffix}.sql"
            filepath = link_dir / filename

            # Find source models that are explicitly connected to this relation
            source_refs = self._find_link_sources(target_name, entities, source_models)

            # Skip if no sources found for this relation
            if not source_refs:
                continue

            sql_content = self.render_sql(
                link_name=target_name, entities=entities, source_refs=source_refs
            )

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(sql_content)

            generated_files.append(str(filepath))

        return generated_files

    def _find_link_sources(
        self, relation_name: str, entities: List[str], source_models: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Find source models that are explicitly connected to this relation."""
        sources = []

        for source_name, source_info in source_models.items():
            entity_columns = {}
            is_connected_to_relation = False

            for col in source_info["columns"]:
                # New structure: target is an array of dicts with target_name, entity_name
                if col.get("target"):
                    for target_conn in col["target"]:
                        target_name = target_conn.get("target_name")
                        entity_name = target_conn.get(
                            "entity_name"
                        )  # Only for relation connections

                        # Only count connections that explicitly target THIS relation
                        if target_name == relation_name and entity_name:
                            is_connected_to_relation = True
                            if entity_name not in entity_columns:
                                entity_columns[entity_name] = []
                            entity_columns[entity_name].append(col["column"])

            # Only include if this source is explicitly connected to the relation
            if is_connected_to_relation:
                sources.append({"source": source_name, "entity_columns": entity_columns})

        return sources

    def render_sql(
        self, link_name: str, entities: List[str], source_refs: List[Dict[str, Any]]
    ) -> str:
        """Render SQL content for a link model using template."""
        source_models = self._get_unique_stage_models(source_refs)

        # Build foreign key list (entity hash keys)
        # Must match naming pattern from stage generator:
        # - Self-links: {link_name}_{entity}_{seq}_hk (e.g., order_self_link_order_1_hk)
        # - Regular links: {link_name}_{entity}_hk (e.g., order_customer_link_order_hk)
        fk_columns = []
        is_self_link = len(entities) != len(set(entities))

        for i, entity in enumerate(entities):
            if is_self_link:
                # Self-link: {link_name}_{entity}_{seq}_hk
                seq = entities[: i + 1].count(entity)
                fk_columns.append(f"{link_name}_{entity}_{seq}_hk")
            else:
                # Regular link: {link_name}_{entity}_hk
                fk_columns.append(f"{link_name}_{entity}_hk")

        return self.render_template(
            "link.sql", link_name=link_name, source_models=source_models, fk_columns=fk_columns
        )
