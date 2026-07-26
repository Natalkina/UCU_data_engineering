from __future__ import annotations

from typing import Protocol

from .node import (
    InternalNode,
    LeafNode,
    NodeTooLargeError,
    fits_in_page,
    internal_entry_size,
    leaf_entry_size,
)

_PROBE_PAGE_ID = 0


def _half_point(sizes: list[int]) -> int:
    """Index that splits sizes into two roughly equal byte halves."""
    total = sum(sizes)
    acc = 0
    for i, size in enumerate(sizes):
        acc += size
        if acc * 2 >= total and i + 1 < len(sizes):
            return i + 1
    return len(sizes) - 1


class PageAllocator(Protocol):
    page_size: int

    def allocate(self) -> int: ...

    def write_node(self, page_id: int, node) -> None: ...

    def read_node(self, page_id: int): ...

    def retire(self, page_id: int) -> None: ...


class BTree:
    """Stateless COW B+-tree operations parameterized by a root id."""

    def __init__(self, allocator: PageAllocator):
        self._alloc = allocator

    # reads

    def get(self, root_id: int, key: bytes) -> bytes | None:
        node = self._alloc.read_node(root_id)
        while isinstance(node, InternalNode):
            idx = node.child_for(key)
            node = self._alloc.read_node(node.children[idx])
        assert isinstance(node, LeafNode)
        return node.get(key)

    # writes

    def put(self, root_id: int, key: bytes, value: bytes) -> int:
        path = self._find_path(root_id, key)  # list of (page_id, node)
        leaf_id, leaf = path[-1]

        new_leaf = LeafNode(keys=list(leaf.keys), values=list(leaf.values))
        new_leaf.put(key, value)

        leaf_pieces = self._split_leaf_to_fit(new_leaf)

        leaf_seps = [piece.keys[0] for piece in leaf_pieces[1:]]
        child_update = self._write_pieces(leaf_pieces, leaf_seps)
        self._alloc.retire(leaf_id)

        for i in range(len(path) - 2, -1, -1):
            parent_id, parent = path[i]
            child_idx = parent.child_for(key)
            new_children = list(parent.children)
            new_keys = list(parent.keys)
            new_children[child_idx] = child_update[0][0]
            for offset, (piece_id, sep_key) in enumerate(child_update[1:]):
                new_children.insert(child_idx + 1 + offset, piece_id)
                new_keys.insert(child_idx + offset, sep_key)

            candidate = InternalNode(keys=new_keys, children=new_children)
            pieces, seps = self._split_internal_to_fit(candidate)
            child_update = self._write_pieces(pieces, seps)
            self._alloc.retire(parent_id)

        # Grow the tree height until a single node covers everything
        while len(child_update) > 1:
            new_root = InternalNode(
                keys=[sep_key for _, sep_key in child_update[1:]],
                children=[piece_id for piece_id, _ in child_update],
            )
            pieces, seps = self._split_internal_to_fit(new_root)
            if len(pieces) == len(child_update):
                raise NodeTooLargeError(
                    "cannot build a root over these separator keys: they do not "
                    f"fit in page_size={self._alloc.page_size}"
                )
            child_update = self._write_pieces(pieces, seps)

        new_root_id, _ = child_update[0]
        return new_root_id

    # helpers

    def _write_pieces(
        self, nodes: list[LeafNode] | list[InternalNode], seps: list[bytes]
    ) -> list[tuple[int, bytes | None]]:
        """Allocate a page for each node, write it, and describe the result."""
        assert len(seps) == len(nodes) - 1
        update: list[tuple[int, bytes | None]] = []
        for i, node in enumerate(nodes):
            page_id = self._alloc.allocate()
            self._alloc.write_node(page_id, node)
            update.append((page_id, None if i == 0 else seps[i - 1]))
        return update

    def _find_path(self, root_id: int, key: bytes):
        """Return [(page_id, node), ...] from root down to the target leaf."""
        path = []
        page_id = root_id
        node = self._alloc.read_node(page_id)
        path.append((page_id, node))
        while isinstance(node, InternalNode):
            idx = node.child_for(key)
            page_id = node.children[idx]
            node = self._alloc.read_node(page_id)
            path.append((page_id, node))
        return path

    def _split_leaf_to_fit(self, leaf: LeafNode) -> list[LeafNode]:
        """Split leaf until every piece serializes within a page"""
        page_size = self._alloc.page_size
        if fits_in_page(leaf, _PROBE_PAGE_ID, page_size):
            return [leaf]
        if len(leaf.keys) <= 1:
            detail = (
                f"key/value pair of {leaf_entry_size(leaf.keys[0], leaf.values[0])} bytes"
                if leaf.keys
                else "empty leaf"
            )
            raise NodeTooLargeError(
                f"{detail} does not fit in a page of {page_size} bytes (R1.3)"
            )
        sizes = [leaf_entry_size(k, v) for k, v in zip(leaf.keys, leaf.values)]
        mid = _half_point(sizes)
        left = LeafNode(keys=leaf.keys[:mid], values=leaf.values[:mid])
        right = LeafNode(keys=leaf.keys[mid:], values=leaf.values[mid:])
        return self._split_leaf_to_fit(left) + self._split_leaf_to_fit(right)

    def _split_internal_to_fit(
        self, node: InternalNode
    ) -> tuple[list[InternalNode], list[bytes]]:
        """Split node until every piece fits, promoting separators."""
        page_size = self._alloc.page_size
        if fits_in_page(node, _PROBE_PAGE_ID, page_size):
            return [node], []
        if not node.keys:
            raise NodeTooLargeError(
                f"internal node with one child does not fit in page_size={page_size}"
            )
        sizes = [internal_entry_size(k) for k in node.keys]
        mid = _half_point(sizes)
        sep_key = node.keys[mid]
        left = InternalNode(keys=node.keys[:mid], children=node.children[: mid + 1])
        right = InternalNode(keys=node.keys[mid + 1 :], children=node.children[mid + 1 :])
        left_pieces, left_seps = self._split_internal_to_fit(left)
        right_pieces, right_seps = self._split_internal_to_fit(right)
        return (
            left_pieces + right_pieces,
            left_seps + [sep_key] + right_seps,
        )
