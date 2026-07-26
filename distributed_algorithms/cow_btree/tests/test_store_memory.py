import random
import string

import pytest

from cow_btree.page_backend import InMemoryPageBackend
from cow_btree.store import Store

SMALL_PAGE_SIZE = 128


def make_store(page_size=4096):
    backend = InMemoryPageBackend(page_size)
    return Store(backend)


def test_empty_store_get_returns_none():
    store = make_store()
    assert store.get(b"missing") is None


def test_put_and_get_single_key():
    store = make_store()
    store.put(b"hello", b"world")
    assert store.get(b"hello") == b"world"


def test_upsert_overwrites_value():
    store = make_store()
    store.put(b"k", b"v1")
    store.put(b"k", b"v2")
    assert store.get(b"k") == b"v2"


def test_multiple_keys_lookup():
    store = make_store()
    pairs = {b"apple": b"1", b"banana": b"2", b"cherry": b"3", b"date": b"4"}
    for k, v in pairs.items():
        store.put(k, v)
    for k, v in pairs.items():
        assert store.get(k) == v
    assert store.get(b"nonexistent") is None


def test_leaf_split_occurs_and_data_survives():
    """With a small page size, enough inserts force at least one leaf split."""
    store = make_store(page_size=SMALL_PAGE_SIZE)
    keys = [f"key{i:03d}".encode() for i in range(20)]
    for i, k in enumerate(keys):
        store.put(k, f"value{i}".encode())
    for i, k in enumerate(keys):
        assert store.get(k) == f"value{i}".encode()


def test_internal_split_and_multilevel_growth():
    """Insert enough keys to force internal-node splits and root growth."""
    store = make_store(page_size=SMALL_PAGE_SIZE)
    n = 500
    keys = [f"k{i:05d}".encode() for i in range(n)]
    random.Random(42).shuffle(keys)
    for k in keys:
        store.put(k, k[::-1])
    for k in keys:
        assert store.get(k) == k[::-1]

    # Root must have grown beyond a single leaf: verify tree height > 1 by
    # checking the root page is an internal node.
    from cow_btree.node import InternalNode

    root_raw = store._backend.read_page(store._root_id)
    from cow_btree.node import deserialize_node

    root = deserialize_node(root_raw)
    assert isinstance(root, InternalNode)


def test_upsert_after_split_still_correct():
    store = make_store(page_size=SMALL_PAGE_SIZE)
    keys = [f"key{i:03d}".encode() for i in range(50)]
    for k in keys:
        store.put(k, b"initial")
    for k in keys:
        store.put(k, b"updated")
    for k in keys:
        assert store.get(k) == b"updated"


def test_lexicographic_ordering_does_not_affect_correctness():
    store = make_store(page_size=SMALL_PAGE_SIZE)
    alphabet = string.ascii_lowercase
    rng = random.Random(7)
    keys = set()
    while len(keys) < 200:
        length = rng.randint(1, 8)
        keys.add("".join(rng.choice(alphabet) for _ in range(length)).encode())
    keys = list(keys)
    for k in keys:
        store.put(k, k.upper())
    for k in keys:
        assert store.get(k) == k.upper()


def test_variable_length_keys_and_values():
    store = make_store()
    store.put(b"", b"empty-key")
    store.put(b"x", b"")
    store.put(b"y" * 100, b"z" * 100)
    assert store.get(b"") == b"empty-key"
    assert store.get(b"x") == b""
    assert store.get(b"y" * 100) == b"z" * 100


def test_put_and_get_reject_types_that_are_not_bytes_like():
    """str/int/None are not byte strings and must raise."""
    store = make_store()
    for bad in ("not-bytes", 42, None, ["a"]):
        with pytest.raises(TypeError):
            store.put(bad, b"v")
        with pytest.raises(TypeError):
            store.put(b"k", bad)
        with pytest.raises(TypeError):
            store.get(bad)


def test_bytearray_and_memoryview_keys_and_values_round_trip():
    """byte[] covers every bytes-like type, not just bytes."""
    store = make_store()
    store.put(bytearray(b"ba-key"), bytearray(b"ba-value"))
    store.put(memoryview(b"mv-key"), memoryview(b"mv-value"))


    assert store.get(b"ba-key") == b"ba-value"
    assert store.get(bytearray(b"ba-key")) == b"ba-value"
    assert store.get(memoryview(b"ba-key")) == b"ba-value"
    assert store.get(b"mv-key") == b"mv-value"
    assert store.get(bytearray(b"mv-key")) == b"mv-value"
    assert isinstance(store.get(b"ba-key"), bytes)

    store.put(bytearray(b"ba-key"), b"updated")
    assert store.get(b"ba-key") == b"updated"


def test_mutating_the_callers_bytearray_after_put_does_not_change_the_store():
    """put copies bytes-like input, so the caller may reuse its buffer."""
    store = make_store()
    key = bytearray(b"key")
    value = bytearray(b"value")
    store.put(key, value)

    key[0:3] = b"XXX"
    value[0:5] = b"YYYYY"

    assert store.get(b"key") == b"value"
    assert store.get(b"XXX") is None
