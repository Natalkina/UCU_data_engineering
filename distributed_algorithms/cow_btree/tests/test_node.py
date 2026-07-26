"""Unit tests for node serialization"""

import pytest

from cow_btree.node import (
    InternalNode,
    LeafNode,
    NodeTooLargeError,
    deserialize_node,
)

PAGE_SIZE = 256


def test_leaf_put_and_get():
    leaf = LeafNode()
    leaf.put(b"b", b"2")
    leaf.put(b"a", b"1")
    leaf.put(b"c", b"3")
    assert leaf.keys == [b"a", b"b", b"c"]
    assert leaf.get(b"a") == b"1"
    assert leaf.get(b"b") == b"2"
    assert leaf.get(b"c") == b"3"
    assert leaf.get(b"missing") is None


def test_leaf_upsert_overwrites():
    leaf = LeafNode()
    leaf.put(b"a", b"1")
    leaf.put(b"a", b"2")
    assert leaf.keys == [b"a"]
    assert leaf.values == [b"2"]


def test_leaf_roundtrip_serialize():
    leaf = LeafNode(keys=[b"a", b"bb", b"ccc"], values=[b"1", b"22", b"333"])
    raw = leaf.serialize(3, PAGE_SIZE)
    assert len(raw) == PAGE_SIZE
    restored = deserialize_node(raw)
    assert isinstance(restored, LeafNode)
    assert restored.keys == leaf.keys
    assert restored.values == leaf.values


def test_leaf_serialize_too_large_raises():
    leaf = LeafNode(keys=[b"x" * 100], values=[b"y" * 100])
    with pytest.raises(NodeTooLargeError):
        leaf.serialize(0, 64)


def test_internal_child_for_routing():
    node = InternalNode(keys=[b"b", b"d"], children=[0, 1, 2])
    assert node.child_for(b"a") == 0
    assert node.child_for(b"b") == 1
    assert node.child_for(b"c") == 1
    assert node.child_for(b"d") == 2
    assert node.child_for(b"z") == 2


def test_internal_roundtrip_serialize():
    node = InternalNode(keys=[b"m"], children=[10, 20])
    raw = node.serialize(5, PAGE_SIZE)
    assert len(raw) == PAGE_SIZE
    restored = deserialize_node(raw)
    assert isinstance(restored, InternalNode)
    assert restored.keys == node.keys
    assert restored.children == node.children


def test_empty_leaf_roundtrip():
    leaf = LeafNode()
    raw = leaf.serialize(1, PAGE_SIZE)
    restored = deserialize_node(raw)
    assert restored.keys == []
    assert restored.values == []


def test_unknown_type_marker_raises():
    raw = bytearray(LeafNode(keys=[b"a"], values=[b"1"]).serialize(1, PAGE_SIZE))
    raw[0] = 99
    with pytest.raises(ValueError, match="unknown node type marker"):
        deserialize_node(bytes(raw))


def test_wrong_type_marker_for_the_requested_class_raises():
    """Asking a class to parse a page of the other type is rejected."""
    raw = LeafNode(keys=[b"a"], values=[b"1"]).serialize(1, PAGE_SIZE)
    with pytest.raises(ValueError, match="node type marker"):
        InternalNode.deserialize(raw)


def test_embedded_page_id_mismatch_raises():
    """A page that describes itself as some other page is a mis-routed or
    recycled page, and must not be parsed as if it were the right one."""
    leaf_raw = LeafNode(keys=[b"a"], values=[b"1"]).serialize(7, PAGE_SIZE)
    with pytest.raises(ValueError, match="describes itself as page 7"):
        deserialize_node(leaf_raw, 3)
    assert deserialize_node(leaf_raw, 7).keys == [b"a"]

    internal_raw = InternalNode(keys=[b"m"], children=[10, 20]).serialize(9, PAGE_SIZE)
    with pytest.raises(ValueError, match="describes itself as page 9"):
        deserialize_node(internal_raw, 4)
    assert deserialize_node(internal_raw, 9).children == [10, 20]


def test_page_id_check_is_skipped_when_not_supplied():
    """The id is optional so a page can still be inspected in isolation."""
    raw = LeafNode(keys=[b"a"], values=[b"1"]).serialize(7, PAGE_SIZE)
    assert deserialize_node(raw).keys == [b"a"]
