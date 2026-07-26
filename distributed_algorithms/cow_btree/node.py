from __future__ import annotations

import bisect
import struct
from dataclasses import dataclass, field

LEAF = 1
INTERNAL = 2

_U32 = struct.Struct("<I")

NODE_HEADER_SIZE = 1 + 4 + 4
INTERNAL_BASE_SIZE = NODE_HEADER_SIZE + 4


def leaf_entry_size(key: bytes, value: bytes) -> int:
    """Serialized cost of one leaf entry (two length prefixes + payloads)."""
    return 4 + len(key) + 4 + len(value)


def internal_entry_size(key: bytes) -> int:
    """Serialized cost of one separator key plus its extra child pointer."""
    return 4 + len(key) + 4


def _pack_bytes(buf: bytearray, data: bytes) -> None:
    buf += _U32.pack(len(data))
    buf += data


def _unpack_bytes(data: bytes, offset: int) -> tuple[bytes, int]:
    (length,) = _U32.unpack_from(data, offset)
    offset += 4
    value = data[offset : offset + length]
    offset += length
    return value, offset


class NodeTooLargeError(Exception):
    """Raised when a node cannot be serialized within a single page."""


def _check_node_header(data: bytes, expected_type: int, page_id: int | None) -> None:
    """Validate a node page's type marker and embedded page id (R3.3)."""
    node_type = data[0]
    if node_type != expected_type:
        raise ValueError(
            f"expected node type marker {expected_type}, found {node_type}"
        )
    if page_id is not None:
        (stored_page_id,) = _U32.unpack_from(data, 1)
        if stored_page_id != page_id:
            raise ValueError(
                f"page {page_id} describes itself as page {stored_page_id} "
                "(mis-routed or recycled page)"
            )


@dataclass
class LeafNode:
    """In-memory representation of a leaf node."""

    keys: list[bytes] = field(default_factory=list)
    values: list[bytes] = field(default_factory=list)

    def find(self, key: bytes) -> int:
        """Return the index of key via binary search, or its insertion point."""
        return bisect.bisect_left(self.keys, key)

    def get(self, key: bytes) -> bytes | None:
        i = self.find(key)
        if i < len(self.keys) and self.keys[i] == key:
            return self.values[i]
        return None

    def put(self, key: bytes, value: bytes) -> None:
        i = self.find(key)
        if i < len(self.keys) and self.keys[i] == key:
            self.values[i] = value
        else:
            self.keys.insert(i, key)
            self.values.insert(i, value)

    def serialize(self, page_id: int, page_size: int) -> bytes:
        buf = bytearray()
        buf.append(LEAF)
        buf += _U32.pack(page_id)
        buf += _U32.pack(len(self.keys))
        for k, v in zip(self.keys, self.values):
            _pack_bytes(buf, k)
            _pack_bytes(buf, v)
        if len(buf) > page_size:
            raise NodeTooLargeError(
                f"serialized leaf is {len(buf)} bytes, exceeds page_size={page_size}"
            )
        buf += bytes(page_size - len(buf))
        return bytes(buf)

    @classmethod
    def deserialize(cls, data: bytes, page_id: int | None = None) -> "LeafNode":
        _check_node_header(data, LEAF, page_id)
        (count,) = _U32.unpack_from(data, 5)
        offset = 9
        keys = []
        values = []
        for _ in range(count):
            k, offset = _unpack_bytes(data, offset)
            v, offset = _unpack_bytes(data, offset)
            keys.append(k)
            values.append(v)
        return cls(keys=keys, values=values)


@dataclass
class InternalNode:
    """In-memory representation of an internal node."""

    keys: list[bytes] = field(default_factory=list)
    children: list[int] = field(default_factory=list)

    def child_for(self, key: bytes) -> int:
        """Return the index into children to descend into for key.
        """
        i = bisect.bisect_right(self.keys, key)
        return i

    def serialize(self, page_id: int, page_size: int) -> bytes:
        buf = bytearray()
        buf.append(INTERNAL)
        buf += _U32.pack(page_id)
        buf += _U32.pack(len(self.keys))
        for k in self.keys:
            _pack_bytes(buf, k)
        for child in self.children:
            buf += _U32.pack(child)
        if len(buf) > page_size:
            raise NodeTooLargeError(
                f"serialized internal node is {len(buf)} bytes, exceeds page_size={page_size}"
            )
        buf += bytes(page_size - len(buf))
        return bytes(buf)

    @classmethod
    def deserialize(cls, data: bytes, page_id: int | None = None) -> "InternalNode":
        _check_node_header(data, INTERNAL, page_id)
        (key_count,) = _U32.unpack_from(data, 5)
        offset = 9
        keys = []
        for _ in range(key_count):
            k, offset = _unpack_bytes(data, offset)
            keys.append(k)
        children = []
        for _ in range(key_count + 1):
            (child,) = _U32.unpack_from(data, offset)
            children.append(child)
            offset += 4
        return cls(keys=keys, children=children)


def deserialize_node(data: bytes, page_id: int | None = None):
    """Dispatch on the type marker byte and return a Leaf/InternalNode."""
    node_type = data[0]
    if node_type == LEAF:
        return LeafNode.deserialize(data, page_id)
    if node_type == INTERNAL:
        return InternalNode.deserialize(data, page_id)
    raise ValueError(f"unknown node type marker: {node_type}")


def fits_in_page(node, page_id: int, page_size: int) -> bool:
    """Check whether node currently serializes within page_size. """
    try:
        node.serialize(page_id, page_size)
        return True
    except NodeTooLargeError:
        return False
