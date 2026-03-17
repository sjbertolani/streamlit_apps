from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Dict, Iterable, List, Optional, Tuple


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
    source_join: List[Tuple[str, str]] = field(default_factory=list)  # (rel_col, source_id_col)
    target_join: List[Tuple[str, str]] = field(default_factory=list)  # (rel_col, target_id_col)


@dataclass
class SemanticGraph:
    concepts: Dict[str, ConceptInfo]
    relationships: List[RelationshipInfo]
    tables: List[str]


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def _is_valid_identifier(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


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
        ref = text[start:end].strip()
        refs.append(ref)
        idx = end
    return refs


def _split_args(arg_str: str) -> List[str]:
    parts: List[str] = []
    current = []
    depth = 0
    for ch in arg_str:
        if ch == "(" or ch == "[" or ch == "{":
            depth += 1
        elif ch == ")" or ch == "]" or ch == "}":
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


def _match_columns(id_cols: Iterable[str], rel_cols: Iterable[str]) -> List[Tuple[str, str]]:
    id_cols_list = list(id_cols)
    rel_cols_list = list(rel_cols)
    matches: List[Tuple[str, str]] = []
    for rel_col in rel_cols_list:
        rel_norm = _normalize(rel_col)
        for id_col in id_cols_list:
            id_norm = _normalize(id_col)
            if not id_norm:
                continue
            if rel_norm == id_norm or rel_norm in id_norm or id_norm in rel_norm:
                matches.append((rel_col, id_col))
    return matches


def parse_semantic_text(text: str) -> SemanticGraph:
    table_map: Dict[str, str] = {}
    concepts: Dict[str, ConceptInfo] = {}
    relationships: List[RelationshipInfo] = []

    table_re = re.compile(r"^(?P<var>\w+)\s*=\s*model\.Table\(\"(?P<table>[^\"]+)\"\)")
    concept_re = re.compile(r"^(?P<concept>\w+)\s*=\s*model\.Concept\(\"(?P<label>[^\"]+)\"")
    define_new_re = re.compile(r"^model\.define\((?P<concept>\w+)\.new\((?P<args>.*)\)\)\s*$")
    define_re = re.compile(r"^model\.define\((?P<body>.+)\)\s*$")

    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]

    for line in lines:
        m = table_re.match(line)
        if m:
            table_map[m.group("var")] = m.group("table")
            continue
        m = concept_re.match(line)
        if m:
            concept_name = m.group("concept")
            concepts[concept_name] = ConceptInfo(name=concept_name)
            continue

    for line in lines:
        m = define_new_re.match(line)
        if not m:
            continue
        concept_name = m.group("concept")
        args = m.group("args")
        arg_parts = _split_args(args)
        id_cols: List[str] = []
        base_table: Optional[str] = None
        for part in arg_parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            key = key.strip()
            value = value.strip()
            ref = None
            for src_ref in _extract_sources_refs(value):
                parsed = _parse_column_ref(src_ref)
                if parsed:
                    ref = parsed
                    break
            if ref:
                table_var, col = ref
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
        # source concept
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
        # collect column refs from args (ignore invalid identifiers)
        col_refs: List[Tuple[str, str]] = []
        for ref in _extract_sources_refs(args_str):
            parsed = _parse_column_ref(ref)
            if parsed:
                col_refs.append(parsed)
        # pick rel table by most refs
        rel_table: Optional[str] = None
        if col_refs:
            counts: Dict[str, int] = {}
            for table_var, _ in col_refs:
                table_name = table_map.get(table_var)
                if not table_name:
                    continue
                counts[table_name] = counts.get(table_name, 0) + 1
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
            source_join = _match_columns(source_info.id_columns if source_info else [], rel_table_cols)
            target_join = _match_columns(target_info.id_columns if target_info else [], rel_table_cols)
            relationships.append(
                RelationshipInfo(
                    name=rel_name,
                    source=source,
                    target=target,
                    rel_table=rel_table,
                    source_join=source_join,
                    target_join=target_join,
                )
            )

    tables = sorted({t for t in table_map.values() if t})
    return SemanticGraph(concepts=concepts, relationships=relationships, tables=tables)


def parse_semantic_file(path: str) -> SemanticGraph:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    return parse_semantic_text(text)

