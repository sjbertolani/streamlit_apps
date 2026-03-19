from __future__ import annotations

import re
import traceback
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple


# ── Data structures ───────────────────────────────────────────────────────────

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


# ── Shared helpers ─────────────────────────────────────────────────────────────

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


def _parse_from_model(model, diags: List[str]) -> SemanticGraph:
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
    table_names: set[str] = set()

    for t in mm.types:
        if isinstance(t, Table):
            table_names.add(t.name)
            continue
        if is_entity_type(t):
            entity_names.add(t.name)
        elif is_value_type(t):
            value_names.add(t.name)
        elif is_primitive(t):
            primitive_names.add(t.name)

    diags.append(
        f"[MM] Types — total: {len(mm.types)}  "
        f"entity: {len(entity_names)}  value: {len(value_names)}  "
        f"primitive: {len(primitive_names)}  table: {len(table_names)}"
    )
    diags.append(f"[MM] Entity types: {sorted(entity_names) or '(none)'}")
    diags.append(f"[MM] Snowflake tables in IR: {sorted(table_names) or '(none)'}")
    diags.append(f"[MM] Relations in IR: {len(mm.relations)}")
    diags.append(f"[MM] Define blocks: {len(model.defines)}")

    # ── 2. Walk define blocks for data-source mappings ────────────────────────
    concept_table_map: Dict[str, str] = {}
    concept_id_cols: Dict[str, List[str]] = {}
    rel_table_map: Dict[str, str] = {}
    rel_table_cols_map: Dict[str, List[str]] = {}
    concrete_types: Dict[Tuple, str] = {}

    for d_idx, d in enumerate(model.defines):
        try:
            block_updates = list(_collect_block_updates(d))
        except Exception as exc:
            diags.append(f"[MM] Define block {d_idx}: ERROR collecting updates — {exc}")
            continue

        diags.append(f"[MM] Define block {d_idx}: {len(block_updates)} Update nodes")

        for upd in block_updates:
            try:
                types = [a.type.name for a in upd.args]
            except Exception as exc:
                diags.append(f"  upd '{getattr(upd.relation, 'name', '?')}': arg type error — {exc}")
                continue

            ent_ctx = next((t for t in types if t in entity_names), "")
            for i, t in enumerate(types):
                concrete_types.setdefault((upd.relation.name, ent_ctx, i), t)

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
            col_names = [c for c in (getattr(a, "name", None) for a in table_args) if c]
            rel_name = upd.relation.name

            if rel_name in _BUILTIN_NAMES or "_row_id_" in rel_name:
                continue

            if len(entity_args) == 1 and entity_args[0].type.name in entity_names:
                concept = entity_args[0].type.name
                first_time = concept not in concept_table_map
                concept_table_map.setdefault(concept, table_name)
                existing = concept_id_cols.setdefault(concept, [])
                if rel_name and not rel_name.startswith("_") and rel_name not in existing:
                    existing.append(rel_name)
                if first_time:
                    diags.append(
                        f"  concept '{concept}' → table '{table_name}'  "
                        f"id_key='{rel_name}'"
                    )

            elif len(entity_args) >= 2:
                rel_table_map.setdefault(rel_name, table_name)
                existing_cols = rel_table_cols_map.setdefault(rel_name, [])
                for c in col_names:
                    if c not in existing_cols:
                        existing_cols.append(c)
                diags.append(
                    f"  rel-table '{rel_name}' → '{table_name}'  cols={col_names}"
                )

    diags.append(
        f"[MM] Concepts with table binding: "
        f"{sorted(concept_table_map.keys()) or '(none)'}"
    )
    diags.append(
        f"[MM] Concepts without table binding: "
        f"{sorted(entity_names - concept_table_map.keys()) or '(none)'}"
    )

    # ── 3. Build relationships from user semantic relations ────────────────────
    def _is_user_relation(rel) -> Tuple[bool, str]:
        if rel.name in _BUILTIN_NAMES:
            return False, "builtin name"
        if not rel.fields:
            return False, "no fields"
        if "_row_id_" in rel.name:
            return False, "row_id relation"
        if any(isinstance(f.type, Table) for f in rel.fields):
            return False, "has Table-typed field"
        return True, ""

    known_names = entity_names | value_names | primitive_names
    relationships: List[RelationshipInfo] = []
    skipped_counts: Dict[str, int] = {}

    for rel in mm.relations:
        is_user, skip_reason = _is_user_relation(rel)
        if not is_user:
            skipped_counts[skip_reason] = skipped_counts.get(skip_reason, 0) + 1
            continue

        ent_ctx = next(
            (f.type.name for f in rel.fields if is_entity_type(f.type)), ""
        )
        roles: List[str] = []
        skip_msg: Optional[str] = None
        try:
            for i, f in enumerate(rel.fields):
                type_name = f.type.name
                if is_abstract(f.type) or is_primitive(f.type):
                    type_name = concrete_types.get((rel.name, ent_ctx, i), type_name)
                    if is_abstract(f.type) and type_name == f.type.name:
                        raise ValueError("unresolved abstract type")
                roles.append(type_name)
        except ValueError as exc:
            skip_msg = str(exc)

        if skip_msg:
            diags.append(f"[MM] Relation '{rel.name}' SKIPPED — {skip_msg}  roles={roles}")
            skipped_counts["unresolved abstract"] = skipped_counts.get("unresolved abstract", 0) + 1
            continue

        if not any(r in known_names for r in roles):
            diags.append(f"[MM] Relation '{rel.name}' SKIPPED — no known type in roles {roles}")
            skipped_counts["no known type"] = skipped_counts.get("no known type", 0) + 1
            continue

        entity_role_names = [r for r in roles if r in entity_names]
        if len(entity_role_names) < 2:
            diags.append(
                f"[MM] Relation '{rel.name}' SKIPPED — only {len(entity_role_names)} "
                f"entity role(s) {entity_role_names} (attribute, not edge)"
            )
            skipped_counts["attribute (< 2 entity roles)"] = (
                skipped_counts.get("attribute (< 2 entity roles)", 0) + 1
            )
            continue

        label = rel.name
        if rel.readings:
            parts = rel.readings[0].parts
            label = "".join(
                (roles[p] if isinstance(p, int) and p < len(roles) else str(p))
                for p in parts
            ).strip()

        source = entity_role_names[0]
        target = entity_role_names[1]
        rel_table = rel_table_map.get(rel.name)
        table_cols = rel_table_cols_map.get(rel.name, [])
        source_join = _match_columns(concept_id_cols.get(source, []), table_cols)
        target_join = _match_columns(concept_id_cols.get(target, []), table_cols)

        diags.append(
            f"[MM] Relation '{rel.name}' → EDGE  label='{label}'  "
            f"{source} → {target}  rel_table={rel_table}  "
            f"src_join={source_join}  tgt_join={target_join}"
        )

        relationships.append(RelationshipInfo(
            name=label,
            source=source,
            target=target,
            rel_table=rel_table,
            source_join=source_join,
            target_join=target_join,
        ))

    diags.append(f"[MM] Relations skipped breakdown: {skipped_counts}")
    diags.append(f"[MM] Edges emitted: {len(relationships)}")

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


def _exec_and_find_model(namespace: Dict[str, object], text: str, diags: List[str]):
    """Exec compiled source into namespace and return the first Model found."""
    exec(compile(text, "<semantic_model>", "exec"), namespace)  # noqa: S102
    candidates = [
        (k, v) for k, v in namespace.items()
        if hasattr(v, "to_metamodel") and hasattr(v, "defines") and hasattr(v, "concepts")
    ]
    diags.append(f"[EXEC] Model candidates in namespace: {[k for k, _ in candidates]}")
    if not candidates:
        all_names = [k for k in namespace if not k.startswith("__")]
        diags.append(f"[EXEC] All namespace names: {all_names}")
        raise ValueError(
            "No Model object found. Ensure the file creates a relationalai.semantics.Model instance."
        )
    name, val = candidates[0]
    diags.append(f"[EXEC] Using model: '{name}'  type={type(val).__name__}")
    return val


def _exec_model(text: str, diags: List[str]):
    """
    Execute PyRel model source and return the Model instance.

    First tries a plain exec. If that fails (e.g. because the file calls
    rai.Config() / rai.Model() at module level and the raiconfig file is not
    in a default location), retries with relationalai.Config mocked out so
    that config-file reads are skipped. The Model's schema definitions
    (concepts, defines) are purely in-memory and do not require a real config.
    """
    diags.append("[EXEC] Compiling source…")
    compiled = compile(text, "<semantic_model>", "exec")

    # ── Attempt 1: plain exec ─────────────────────────────────────────────────
    try:
        namespace: Dict[str, object] = {}
        return _exec_and_find_model(namespace, text, diags)
    except Exception as exc1:
        diags.append(f"[EXEC] Plain exec failed — {type(exc1).__name__}: {exc1}")
        for line in traceback.format_exc().splitlines():
            diags.append(f"  {line}")

    # ── Attempt 2: retry with relationalai.Config mocked ─────────────────────
    diags.append("[EXEC] Retrying with relationalai.Config mocked (skipping config-file read)…")
    try:
        import relationalai as _rai_mod
        from unittest.mock import MagicMock, patch

        # Patch Config at the module level so any import style works:
        #   import relationalai as rai; rai.Config(...)
        #   from relationalai import Config; Config(...)
        with patch.object(_rai_mod, "Config", MagicMock(return_value=MagicMock())):
            namespace = {}
            return _exec_and_find_model(namespace, text, diags)
    except Exception as exc2:
        diags.append(f"[EXEC] Mocked-config exec also failed — {type(exc2).__name__}: {exc2}")
        for line in traceback.format_exc().splitlines():
            diags.append(f"  {line}")
        raise exc2


# ── Regex fallback parser ─────────────────────────────────────────────────────

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


def _parse_column_ref(ref: str, table_map: Optional[Dict[str, str]] = None) -> Optional[Tuple[str, str]]:
    parts = [p.strip() for p in ref.split(".")]
    if len(parts) < 3:
        return None
    column = parts[-1]
    if not _is_valid_identifier(column):
        return None
    if table_map:
        for i in range(len(parts) - 2, 0, -1):
            if parts[i] in table_map:
                return parts[i], column
    return parts[-2], column


def _extract_define_bodies(text: str) -> List[str]:
    """
    Extract the inner content of every model.define(...) call from the full
    source text, tracking parenthesis depth so multi-line defines are handled
    correctly. Returns each body as a single whitespace-collapsed string.
    """
    bodies: List[str] = []
    marker = "model.define("
    search_from = 0
    while True:
        start = text.find(marker, search_from)
        if start == -1:
            break
        i = start + len(marker) - 1
        depth = 0
        body_start = i + 1
        while i < len(text):
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if depth == 0:
                    raw = text[body_start:i]
                    bodies.append(re.sub(r"\s+", " ", raw).strip())
                    break
            i += 1
        search_from = i + 1
    return bodies


def _parse_semantic_text_regex(text: str, diags: List[str]) -> SemanticGraph:
    """Regex-based parser — fallback when exec of the model code is not possible."""
    diags.append("[REGEX] Using regex fallback parser")
    table_map: Dict[str, str] = {}
    concepts: Dict[str, ConceptInfo] = {}
    relationships: List[RelationshipInfo] = []

    _table_full_re = re.compile(
        r"^\s*(\w+)\s*=\s*model\.Table\(\s*[\"']([^\"']+)[\"']\s*\)",
        re.MULTILINE,
    )
    for m in _table_full_re.finditer(text):
        table_map[m.group(1)] = m.group(2)
    diags.append(f"[REGEX] Tables found: {table_map or '(none)'}")

    concept_re = re.compile(r"^(?P<concept>\w+)\s*=\s*model\.Concept\(\"(?P<label>[^\"]+)\"")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = concept_re.match(line)
        if m:
            concepts[m.group("concept")] = ConceptInfo(name=m.group("concept"))
    diags.append(f"[REGEX] Concepts found: {list(concepts.keys()) or '(none)'}")

    define_bodies = _extract_define_bodies(text)
    diags.append(f"[REGEX] model.define() bodies extracted: {len(define_bodies)}")
    for i, body in enumerate(define_bodies):
        diags.append(f"  body[{i}]: {body[:120]}{'…' if len(body) > 120 else ''}")

    define_new_re = re.compile(r"^(?P<concept>\w+)\.new\((?P<args>.+)\)\s*$")

    for body in define_bodies:
        m = define_new_re.match(body)
        if m:
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
                    parsed = _parse_column_ref(src_ref, table_map)
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
                diags.append(
                    f"[REGEX] Concept.new '{concept_name}' → table='{base_table}'  "
                    f"id_cols={id_cols}"
                )
            else:
                diags.append(
                    f"[REGEX] Concept.new '{concept_name}' not in known concepts — SKIPPED"
                )
            continue

        src_match = re.match(r"^(?P<src>\w+)\.filter_by", body)
        if not src_match:
            diags.append(f"[REGEX] Body not matched (no .new or .filter_by): {body[:80]}")
            continue
        source = src_match.group("src")
        rel_match = re.search(r"\)\.(?P<rel>\w+)\((?P<args>.*)\)\s*$", body)
        if not rel_match:
            diags.append(f"[REGEX] Rel body: no trailing .rel(...) pattern — {body[:80]}")
            continue
        rel_name = rel_match.group("rel")
        args_str = rel_match.group("args")

        target_concepts = re.findall(r"(\w+)\.filter_by\(", args_str)
        col_refs: List[Tuple[str, str]] = []
        for ref in _extract_sources_refs(args_str):
            parsed = _parse_column_ref(ref, table_map)
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

        diags.append(
            f"[REGEX] Rel '{rel_name}'  {source} → {target_concepts}  "
            f"rel_table={rel_table}  cols={rel_table_cols}"
        )

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

def parse_semantic_text(text: str) -> Tuple[SemanticGraph, List[str]]:
    """
    Parse a PyRel semantic layer Python file.

    Returns (SemanticGraph, diagnostics) where diagnostics is a list of
    strings describing what the parser found at each step.

    Primary path: exec the file to get the live Model object, then introspect
    it via model.to_metamodel() for accurate, structure-aware extraction.

    Fallback: regex text parsing for files with non-standard syntax or
    environments where relationalai is not installed.
    """
    diags: List[str] = []
    try:
        diags.append("[EXEC] Attempting metamodel path…")
        model = _exec_model(text, diags)
        diags.append("[MM] Running _parse_from_model…")
        graph = _parse_from_model(model, diags)
        diags.append(
            f"[DONE] Metamodel parse complete — "
            f"{len(graph.concepts)} concepts, {len(graph.relationships)} relationships, "
            f"{len(graph.tables)} tables"
        )
        return graph, diags
    except Exception as exc:
        diags.append(f"[EXEC] FAILED — {type(exc).__name__}: {exc}")
        diags.append("[EXEC] Traceback:")
        for line in traceback.format_exc().splitlines():
            diags.append(f"  {line}")
        diags.append("[EXEC] Falling back to regex parser…")

    try:
        graph = _parse_semantic_text_regex(text, diags)
        diags.append(
            f"[DONE] Regex parse complete — "
            f"{len(graph.concepts)} concepts, {len(graph.relationships)} relationships, "
            f"{len(graph.tables)} tables"
        )
        return graph, diags
    except Exception as exc:
        diags.append(f"[REGEX] FAILED — {type(exc).__name__}: {exc}")
        diags.append("[REGEX] Traceback:")
        for line in traceback.format_exc().splitlines():
            diags.append(f"  {line}")
        empty = SemanticGraph(concepts={}, relationships=[], tables=[])
        return empty, diags


def parse_semantic_file(path: str) -> Tuple[SemanticGraph, List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    return parse_semantic_text(text)
