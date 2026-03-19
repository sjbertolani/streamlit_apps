from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple


# ── Data structures (unchanged — consumed by app.py, graph_counts.py, graph_filter.py) ──

@dataclass
class ConceptInfo:
    name: str
    id_columns: List[str] = field(default_factory=list)
    base_table: Optional[str] = None


@dataclass
class RelationshipInfo:
    name: str
    source: str
    target: str
    rel_table: Optional[str] = None
    source_join: List[Tuple[str, str]] = field(default_factory=list)
    target_join: List[Tuple[str, str]] = field(default_factory=list)


@dataclass
class SemanticGraph:
    concepts: Dict[str, ConceptInfo]
    relationships: List[RelationshipInfo]
    tables: List[str]


# ── Shared helpers ────────────────────────────────────────────────────────────

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_BUILTIN_NAMES = frozenset({
    "=", "!=", "<", "<=", ">", ">=",
    "+", "-", "*", "/", "//", "%", "^",
    "cast", "unique", "output_value_is_key",
})


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def _is_valid_identifier(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


def _match_columns(
    id_cols: Iterable[str], rel_cols: Iterable[str]
) -> List[Tuple[str, str]]:
    """Fuzzy-match concept ID columns against relationship table column names."""
    id_list = list(id_cols)
    rel_list = list(rel_cols)
    matches: List[Tuple[str, str]] = []
    for rel_col in rel_list:
        rel_norm = _normalize(rel_col)
        for id_col in id_list:
            id_norm = _normalize(id_col)
            if not id_norm:
                continue
            if rel_norm == id_norm or rel_norm in id_norm or id_norm in rel_norm:
                matches.append((rel_col, id_col))
    return matches


# ── Metamodel-based parser ────────────────────────────────────────────────────

def _collect_block_updates(define_block):
    """Yield all Update nodes from a compiled define block."""
    from relationalai.semantics.metamodel.metamodel import Update

    def _walk(body):
        for item in body:
            if isinstance(item, Update):
                yield item
            if hasattr(item, "body"):
                sub = item.body
                if isinstance(sub, (list, tuple)):
                    yield from _walk(sub)

    for stmt in define_block.to_metamodel().body:
        body = getattr(stmt, "body", None)
        if isinstance(body, (list, tuple)):
            yield from _walk(body)


def _parse_from_model(model) -> SemanticGraph:
    """
    Extract a SemanticGraph from a live PyRel Model object using the
    metamodel IR (model.to_metamodel()) rather than text parsing.
    """
    from relationalai.semantics.metamodel.builtins import (
        is_abstract, is_entity_type, is_primitive, is_value_type,
    )
    from relationalai.semantics.metamodel.metamodel import Table, Update

    mm = model.to_metamodel()

    # ── 1. Classify types ─────────────────────────────────────────────────────
    entity_names: set[str] = set()
    value_names: set[str] = set()
    primitive_names: set[str] = set()

    for t in mm.types:
        if isinstance(t, Table):
            continue
        if is_entity_type(t):
            entity_names.add(t.name)
        elif is_value_type(t):
            value_names.add(t.name)
        elif is_primitive(t):
            primitive_names.add(t.name)

    # ── 2. Walk define blocks for data-source mappings ────────────────────────
    # For each concept: base_table and id_columns
    # For each semantic relation: rel_table and the column names used in joins
    concept_table_map: Dict[str, str] = {}       # entity_name → Snowflake table
    concept_id_cols: Dict[str, List[str]] = {}   # entity_name → [id_col_key, ...]
    rel_table_map: Dict[str, str] = {}           # rel_name → Snowflake table
    rel_table_cols_map: Dict[str, List[str]] = {}  # rel_name → [col, ...]

    # Also collect concrete type resolution from define block Update nodes
    concrete_types: Dict[Tuple, str] = {}

    for d in model.defines:
        block_updates = list(_collect_block_updates(d))

        for upd in block_updates:
            # Concrete type resolution (for abstract/polymorphic fields)
            types = [a.type.name for a in upd.args]
            ent_ctx = next((t for t in types if t in entity_names), "")
            for i, t in enumerate(types):
                concrete_types.setdefault((upd.relation.name, ent_ctx, i), t)

            # Partition args by whether they're entity-typed or Table-typed
            entity_args = [
                a for a in upd.args
                if hasattr(a, "type") and is_entity_type(a.type)
            ]
            table_args = [
                a for a in upd.args
                if hasattr(a, "type") and isinstance(a.type, Table)
            ]

            if not table_args:
                continue

            table_name = table_args[0].type.name
            # Column names live on arg.name for Table-typed args
            col_names = [c for c in (getattr(a, "name", None) for a in table_args) if c]

            rel_name = upd.relation.name
            if rel_name in _BUILTIN_NAMES or "_row_id_" in rel_name:
                continue

            if len(entity_args) == 1 and entity_args[0].type.name in entity_names:
                # Identity define (Concept.new): maps one entity to a source table
                concept = entity_args[0].type.name
                concept_table_map.setdefault(concept, table_name)
                # The relation name is the keyword used in new() — i.e. the id column key
                existing = concept_id_cols.setdefault(concept, [])
                if rel_name and not rel_name.startswith("_") and rel_name not in existing:
                    existing.append(rel_name)

            elif len(entity_args) >= 2:
                # Relationship define: maps a semantic relation through a table
                rel_table_map.setdefault(rel_name, table_name)
                existing_cols = rel_table_cols_map.setdefault(rel_name, [])
                for c in col_names:
                    if c not in existing_cols:
                        existing_cols.append(c)

    # ── 3. Build relationships from user semantic relations ────────────────────
    def _is_user_relation(rel) -> bool:
        if rel.name in _BUILTIN_NAMES or not rel.fields:
            return False
        if "_row_id_" in rel.name:
            return False
        return not any(isinstance(f.type, Table) for f in rel.fields)

    known_names = entity_names | value_names | primitive_names
    relationships: List[RelationshipInfo] = []

    for rel in mm.relations:
        if not _is_user_relation(rel):
            continue

        # Resolve each field's concrete type (handle abstract/primitive)
        ent_ctx = next(
            (f.type.name for f in rel.fields if is_entity_type(f.type)), ""
        )
        roles: List[str] = []
        try:
            for i, f in enumerate(rel.fields):
                type_name = f.type.name
                if is_abstract(f.type) or is_primitive(f.type):
                    type_name = concrete_types.get((rel.name, ent_ctx, i), type_name)
                    if is_abstract(f.type) and type_name == f.type.name:
                        raise ValueError("unresolved abstract type")
                roles.append(type_name)
        except ValueError:
            continue

        if not any(r in known_names for r in roles):
            continue

        # Build the human-readable reading label from the relation's reading
        label = rel.name
        if rel.readings:
            parts = rel.readings[0].parts
            label = "".join(
                (roles[p] if isinstance(p, int) and p < len(roles) else str(p))
                for p in parts
            ).strip()

        # Only emit edges for entity-to-entity relationships (not attribute bundles)
        entity_role_names = [r for r in roles if r in entity_names]
        if len(entity_role_names) < 2:
            continue

        source = entity_role_names[0]
        target = entity_role_names[1]

        rel_table = rel_table_map.get(rel.name)
        table_cols = rel_table_cols_map.get(rel.name, [])
        source_join = _match_columns(concept_id_cols.get(source, []), table_cols)
        target_join = _match_columns(concept_id_cols.get(target, []), table_cols)

        relationships.append(RelationshipInfo(
            name=label,
            source=source,
            target=target,
            rel_table=rel_table,
            source_join=source_join,
            target_join=target_join,
        ))

    # ── 4. Assemble SemanticGraph ──────────────────────────────────────────────
    concepts: Dict[str, ConceptInfo] = {
        name: ConceptInfo(
            name=name,
            id_columns=concept_id_cols.get(name, []),
            base_table=concept_table_map.get(name),
        )
        for name in sorted(entity_names)
    }
    tables = sorted({t.name for t in mm.types if isinstance(t, Table)})

    return SemanticGraph(concepts=concepts, relationships=relationships, tables=tables)


def _exec_model(text: str):
    """Execute PyRel model source and return the Model instance."""
    namespace: Dict[str, object] = {}
    exec(compile(text, "<semantic_model>", "exec"), namespace)  # noqa: S102
    for val in namespace.values():
        if hasattr(val, "to_metamodel") and hasattr(val, "defines") and hasattr(val, "concepts"):
            return val
    raise ValueError(
        "No Model object found. Ensure the file creates a relationalai.semantics.Model instance."
    )


# ── Regex fallback parser ─────────────────────────────────────────────────────
# Used when exec fails (e.g. syntax errors from space-containing keyword names,
# or when relationalai is not installed in the current environment).

def _extract_sources_refs(text: str) -> List[str]:
    refs: List[str] = []
    idx = 0
    while True:
        start = text.find("Sources.", idx)
        if start == -1:
            break
        end = start + len("Sources.")
        while end < len(text) and text[end] not in [",", ")"]:
            end += 1
        refs.append(text[start:end].strip())
        idx = end
    return refs


def _split_args(arg_str: str) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in arg_str:
        if ch in ("(", "[", "{"):
            depth += 1
        elif ch in (")", "]", "}"):
            depth = max(depth - 1, 0)
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_column_ref(ref: str) -> Optional[Tuple[str, str]]:
    parts = ref.split(".")
    if len(parts) < 3:
        return None
    column = parts[-1].strip()
    table_var = parts[-2].strip()
    if not _is_valid_identifier(column):
        return None
    return table_var, column


def _parse_semantic_text_regex(text: str) -> SemanticGraph:
    """Regex-based parser — fallback when exec of the model code is not possible."""
    table_map: Dict[str, str] = {}
    concepts: Dict[str, ConceptInfo] = {}
    relationships: List[RelationshipInfo] = []

    table_re = re.compile(r"^(?P<var>\w+)\s*=\s*model\.Table\(\"(?P<table>[^\"]+)\"\)")
    concept_re = re.compile(r"^(?P<concept>\w+)\s*=\s*model\.Concept\(\"(?P<label>[^\"]+)\"")
    define_new_re = re.compile(r"^model\.define\((?P<concept>\w+)\.new\((?P<args>.*)\)\)\s*$")
    define_re = re.compile(r"^model\.define\((?P<body>.+)\)\s*$")

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    for line in lines:
        m = table_re.match(line)
        if m:
            table_map[m.group("var")] = m.group("table")
            continue
        m = concept_re.match(line)
        if m:
            concepts[m.group("concept")] = ConceptInfo(name=m.group("concept"))

    for line in lines:
        m = define_new_re.match(line)
        if not m:
            continue
        concept_name = m.group("concept")
        id_cols: List[str] = []
        base_table: Optional[str] = None
        for part in _split_args(m.group("args")):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            key = key.strip()
            ref = None
            for src_ref in _extract_sources_refs(value.strip()):
                parsed = _parse_column_ref(src_ref)
                if parsed:
                    ref = parsed
                    break
            if ref:
                table_var, _ = ref
                if base_table is None:
                    base_table = table_map.get(table_var)
                id_cols.append(key)
        if concept_name in concepts:
            concepts[concept_name].id_columns = id_cols
            concepts[concept_name].base_table = base_table

    for line in lines:
        m = define_re.match(line)
        if not m:
            continue
        body = m.group("body")
        src_match = re.match(r"^(?P<src>\w+)\.filter_by", body)
        if not src_match:
            continue
        source = src_match.group("src")
        rel_match = re.search(r"\)\.(?P<rel>\w+)\((?P<args>.*)\)\s*$", body)
        if not rel_match:
            continue
        rel_name = rel_match.group("rel")
        args_str = rel_match.group("args")

        target_concepts = re.findall(r"(\w+)\.filter_by\(", args_str)
        col_refs: List[Tuple[str, str]] = []
        for ref in _extract_sources_refs(args_str):
            parsed = _parse_column_ref(ref)
            if parsed:
                col_refs.append(parsed)

        rel_table: Optional[str] = None
        if col_refs:
            counts: Dict[str, int] = {}
            for table_var, _ in col_refs:
                tname = table_map.get(table_var)
                if tname:
                    counts[tname] = counts.get(tname, 0) + 1
            if counts:
                rel_table = max(counts.items(), key=lambda kv: kv[1])[0]

        rel_table_cols: List[str] = []
        if rel_table:
            for table_var, col in col_refs:
                if table_map.get(table_var) == rel_table:
                    rel_table_cols.append(col)

        for target in target_concepts:
            if target == source:
                continue
            source_info = concepts.get(source)
            target_info = concepts.get(target)
            relationships.append(RelationshipInfo(
                name=rel_name,
                source=source,
                target=target,
                rel_table=rel_table,
                source_join=_match_columns(
                    source_info.id_columns if source_info else [], rel_table_cols
                ),
                target_join=_match_columns(
                    target_info.id_columns if target_info else [], rel_table_cols
                ),
            ))

    tables = sorted({t for t in table_map.values() if t})
    return SemanticGraph(concepts=concepts, relationships=relationships, tables=tables)


# ── Public API ────────────────────────────────────────────────────────────────

def parse_semantic_text(text: str) -> SemanticGraph:
    """
    Parse a PyRel semantic layer Python file.

    Primary path: exec the file to get the live Model object, then introspect
    it via model.to_metamodel() for accurate, structure-aware extraction.

    Fallback: regex text parsing for files with non-standard syntax (e.g.
    space-containing keyword names in filter_by calls) or environments where
    relationalai is not installed.
    """
    try:
        model = _exec_model(text)
        return _parse_from_model(model)
    except Exception:
        return _parse_semantic_text_regex(text)


def parse_semantic_file(path: str) -> SemanticGraph:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    return parse_semantic_text(text)
