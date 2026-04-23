from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable, Iterator

from growler.pointer import ManifestInternal, ManifestLeaf, Pointer, Schema


def empty_tree() -> dict:
    return asdict(ManifestLeaf())


def insert_file(
    tree: dict,
    file_entry: dict,
    partition_spec: list[int],
    schema: Schema,
) -> dict:
    if not partition_spec:
        if tree.get("type") != "leaf":
            raise ValueError("Expected leaf tree when partition_spec is empty")
        tree.setdefault("files", []).append(file_entry)
        return tree
    pvals = file_entry["partition_values"]
    node = tree
    for depth, col_id in enumerate(partition_spec):
        col = schema.by_id(col_id)
        value = pvals.get(col.path)
        if value is None:
            value = pvals.get(str(col.id))
        if value is None:
            raise ValueError(
                f"File missing partition value for column {col.path} (id={col.id})"
            )
        value = str(value)
        if node.get("type") != "internal":
            new_internal = {
                "type": "internal",
                "partition_column_id": col_id,
                "children": {},
            }
            node.clear()
            node.update(new_internal)
        if node.get("partition_column_id") != col_id:
            raise ValueError("Partition column mismatch in manifest tree")
        children = node["children"]
        if value not in children:
            if depth == len(partition_spec) - 1:
                children[value] = asdict(ManifestLeaf())
            else:
                children[value] = {
                    "type": "internal",
                    "partition_column_id": partition_spec[depth + 1],
                    "children": {},
                }
        node = children[value]
    if node.get("type") != "leaf":
        raise ValueError("Expected leaf at terminal position")
    node.setdefault("files", []).append(file_entry)
    return tree


def walk_leaves(tree: dict) -> Iterator[tuple[list[tuple[int, str]], dict]]:
    stack: list[tuple[dict, list[tuple[int, str]]]] = [(tree, [])]
    while stack:
        node, path = stack.pop()
        if node.get("type") == "leaf":
            yield path, node
            continue
        col_id = node.get("partition_column_id")
        for value, child in node.get("children", {}).items():
            stack.append((child, path + [(col_id, value)]))


def walk_partitions(
    tree: dict,
    schema: Schema,
    predicate: Callable[[int, str, str], bool] | None = None,
) -> Iterator[dict]:
    for path, leaf in walk_leaves(tree):
        if predicate is not None and not all(predicate(col_id, schema.by_id(col_id).path, value) for col_id, value in path):
            continue
        pvals = {schema.by_id(col_id).path: value for col_id, value in path}
        yield {
            "partition_values": pvals,
            "files": list(leaf.get("files", [])),
        }


def all_files(tree: dict) -> Iterator[dict]:
    for _, leaf in walk_leaves(tree):
        for f in leaf.get("files", []):
            yield f
