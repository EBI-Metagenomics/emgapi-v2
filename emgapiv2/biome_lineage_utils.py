from __future__ import annotations

import re


def normalize_lineage(lineage: str | None) -> str:
    """Remove empty lineage components and surrounding whitespace."""
    return ":".join(part.strip() for part in (lineage or "").split(":") if part.strip())


def lineage_to_path(lineage: str | None) -> str:
    """Convert a biome lineage to a PostgreSQL ltree-compatible path."""
    normalized = normalize_lineage(lineage)
    ascii_lower = normalized.encode("ascii", "ignore").decode("ascii").lower()
    dot_separated = ascii_lower.replace(":", ".")
    underscore_punctuated = (
        dot_separated.replace(" ", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("-", "_")
        .replace("__", "_")
        .strip("_.")
    )
    return re.sub(r"[^a-zA-Z0-9._]", "", underscore_punctuated)
