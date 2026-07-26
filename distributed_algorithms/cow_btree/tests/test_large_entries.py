"""Regression tests for large (but page-sized) entries and failed puts."""

import pytest

from cow_btree.btree import BTree
from cow_btree.node import InternalNode, LeafNode, NodeTooLargeError, deserialize_node
from cow_btree.page_backend import InMemoryPageBackend
from cow_btree.store import Store

PAGE_SIZE = 4096


def make_store(page_size=PAGE_SIZE):
    return Store(InMemoryPageBackend(page_size))


def read_node(store, page_id):
    return deserialize_node(store._backend.read_page(page_id))


def reachable_pages(store, page_id=None):
    """Page ids reachable from the published root (the live tree)."""
    if page_id is None:
        page_id = store._root_id
    node = read_node(store, page_id)
    pages = {page_id}
    if isinstance(node, InternalNode):
        for child in node.children:
            pages |= reachable_pages(store, child)
    return pages


def assert_no_pages_lost(store):
    """Every page in the file must be live, free, pending, or metadata."""
    accounted = reachable_pages(store)
    accounted.add(0)  # header
    accounted.update(store._free_list_containers)
    accounted.update(store._free_ids)
    accounted.update(page_id for _, page_id in store._pending)
    assert accounted == set(range(store._backend.page_count)), sorted(
        set(range(store._backend.page_count)) - accounted
    )


# one mid-point split is not always enough


def test_upsert_large_value_into_populated_leaf():
    """Replacing a small value with a near-page one must split as needed."""
    store = make_store()
    for i in range(8):
        store.put(f"k{i}".encode(), b"v" * 495)  # leaf ~4009 bytes: fits
    store.put(b"k1", b"V" * 3500)  # would be ~5034 bytes in one leaf

    assert store.get(b"k1") == b"V" * 3500
    for i in range(8):
        if i != 1:
            assert store.get(f"k{i}".encode()) == b"v" * 495
    assert_no_pages_lost(store)


def test_insert_large_new_entry_into_populated_leaf():
    store = make_store()
    for i in range(8):
        store.put(f"k{i}".encode(), b"v" * 495)
    store.put(b"a-big", b"V" * 3500)

    assert store.get(b"a-big") == b"V" * 3500
    for i in range(8):
        assert store.get(f"k{i}".encode()) == b"v" * 495
    assert_no_pages_lost(store)


@pytest.mark.parametrize("value_size", [PAGE_SIZE // 2, PAGE_SIZE - 64])
def test_values_close_to_a_full_page(value_size):
    """Values of half a page and just under a page round-trip"""
    store = make_store()
    keys = [f"key{i:02d}".encode() for i in range(6)]
    for k in keys:
        store.put(k, bytes([k[-1]]) * value_size)
    for k in keys:
        assert store.get(k) == bytes([k[-1]]) * value_size
    assert_no_pages_lost(store)


def test_leaf_mixing_tiny_and_near_page_entries():
    """A leaf whose entry sizes are wildly skewed still splits correctly."""
    store = make_store()
    expected = {}
    for i in range(40):
        key = f"key{i:03d}".encode()
        value = b"x" * (3900 if i % 5 == 0 else 3)
        expected[key] = value
        store.put(key, value)
    for key, value in expected.items():
        assert store.get(key) == value
    assert_no_pages_lost(store)


def test_split_can_produce_more_than_two_leaves():
    """One over-full leaf may legitimately need three or more pages."""
    tree = BTree(_SizeOnlyAllocator(PAGE_SIZE))
    # 3 x 2100-byte entries: no two of them fit together in one page.
    leaf = LeafNode(
        keys=[b"a", b"b", b"c"],
        values=[b"1" * 2100, b"2" * 2100, b"3" * 2100],
    )
    pieces = tree._split_leaf_to_fit(leaf)
    assert len(pieces) == 3
    assert [p.keys for p in pieces] == [[b"a"], [b"b"], [b"c"]]
    for piece in pieces:
        piece.serialize(0, PAGE_SIZE)  # must not raise


def test_multi_way_leaf_split_through_the_store():
    """End to end: a single put forces a >2-way split under the parent."""
    store = make_store()
    for i in range(6):
        store.put(f"k{i}".encode(), b"v" * 600)  # leaf ~3657 bytes: fits
    store.put(b"k2", b"V" * 3900)  # ~6961 bytes -> at least 3 leaves

    root = read_node(store, store._root_id)
    assert isinstance(root, InternalNode)
    assert len(root.children) >= 3
    assert store.get(b"k2") == b"V" * 3900
    for i in range(6):
        if i != 2:
            assert store.get(f"k{i}".encode()) == b"v" * 600
    assert_no_pages_lost(store)


def test_large_keys_force_internal_splits():
    """Big separator keys make internal nodes overflow too (_split_internal)."""
    store = make_store()
    keys = [bytes([ord("a") + (i % 26)]) + b"-" + b"K" * 900 + f"{i:03d}".encode() for i in range(60)]
    for i, k in enumerate(keys):
        store.put(k, f"v{i}".encode())
    for i, k in enumerate(keys):
        assert store.get(k) == f"v{i}".encode()
    assert isinstance(read_node(store, store._root_id), InternalNode)
    assert_no_pages_lost(store)


def test_keys_so_large_that_only_one_fits_per_node():
    store = make_store(page_size=128)
    expected = {b"K" * 100 + f"{i:02d}".encode(): b"v" for i in range(25)}
    for k, v in expected.items():
        store.put(k, v)
    for k, v in expected.items():
        assert store.get(k) == v
    assert_no_pages_lost(store)


def test_split_internal_promotes_without_duplicating_keys():
    """Multi-way internal splits keep the promote-the-middle convention."""
    tree = BTree(_SizeOnlyAllocator(PAGE_SIZE))
    # Separator keys so large that no two of them fit in one internal page.
    keys = [b"k%d" % i + b"K" * 2100 for i in range(4)]
    node = InternalNode(keys=list(keys), children=[10, 11, 12, 13, 14])
    pieces, promoted = tree._split_internal_to_fit(node)

    assert len(pieces) >= 3
    assert len(promoted) == len(pieces) - 1
    # Keys are partitioned, never duplicated, and stay in order.
    flattened = []
    for piece, sep in zip(pieces, promoted + [None]):
        flattened.extend(piece.keys)
        if sep is not None:
            flattened.append(sep)
    assert flattened == keys
    # Children are partitioned in order as well, one more than the keys.
    children = [c for piece in pieces for c in piece.children]
    assert children == node.children
    for piece in pieces:
        assert len(piece.children) == len(piece.keys) + 1
        piece.serialize(0, PAGE_SIZE)  # must not raise


def test_large_values_survive_a_reopen(tmp_path):
    """The pages written by a multi-way split are readable after recovery."""
    from cow_btree.page_backend import MMapPageBackend

    path = str(tmp_path / "large.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))
    expected = {f"key{i:02d}".encode(): b"z" * (3000 if i % 3 == 0 else 30) for i in range(30)}
    for k, v in expected.items():
        store.put(k, v)
    store.close()

    store2 = Store(MMapPageBackend(path, PAGE_SIZE))
    for k, v in expected.items():
        assert store2.get(k) == v
    store2.close()


# a failed put must be a no-op


def test_pair_larger_than_a_page_is_rejected():
    store = make_store()
    with pytest.raises(NodeTooLargeError):
        store.put(b"too-big", b"x" * PAGE_SIZE)
    assert store.get(b"too-big") is None


def test_failed_put_leaves_the_store_usable_and_loses_no_pages():
    store = make_store()
    expected = {f"key{i:03d}".encode(): b"v" * 300 for i in range(30)}
    for k, v in expected.items():
        store.put(k, v)

    root_before = store._root_id
    epoch_before = store._epoch
    free_before = sorted(store._free_ids)
    pages_before = store._backend.page_count

    for _ in range(5):
        with pytest.raises(NodeTooLargeError):
            store.put(b"key005", b"x" * (PAGE_SIZE * 2))

    # Nothing was published and no bookkeeping was left half-applied.
    assert store._root_id == root_before
    assert store._epoch == epoch_before
    assert sorted(store._free_ids) == free_before
    assert store._pending_this_commit == []
    assert store._allocated_this_attempt == []
    assert store._backend.page_count == pages_before

    for k, v in expected.items():
        assert store.get(k) == v
    store.put(b"key005", b"after-failure")
    assert store.get(b"key005") == b"after-failure"
    assert_no_pages_lost(store)


def test_put_failing_mid_write_returns_its_pages_and_keeps_live_ones():
    """Inject an I/O error after some pages are already written."""
    store = make_store(page_size=256)
    expected = {f"key{i:03d}".encode(): b"v" * 20 for i in range(40)}
    for k, v in expected.items():
        store.put(k, v)

    assert isinstance(read_node(store, store._root_id), InternalNode)
    root_before = store._root_id
    free_before = sorted(store._free_ids)
    pending_before = list(store._pending)
    backend = store._backend
    real_write_page = backend.write_page
    state = {"remaining": 1}

    def flaky_write_page(page_id, data):
        if state["remaining"] == 0:
            raise OSError("injected write failure")
        state["remaining"] -= 1
        real_write_page(page_id, data)

    backend.write_page = flaky_write_page
    with pytest.raises(OSError):
        store.put(b"key017", b"updated")
    backend.write_page = real_write_page

    assert store._root_id == root_before
    assert sorted(store._free_ids) == free_before
    assert store._pending == pending_before
    assert store._pending_this_commit == []
    assert store._allocated_this_attempt == []
    assert_no_pages_lost(store)

    for k, v in expected.items():
        assert store.get(k) == v
    store.put(b"key017", b"updated")
    assert store.get(b"key017") == b"updated"
    assert_no_pages_lost(store)


class _SizeOnlyAllocator:
    """Minimal allocator stub: the split helpers only need page_size."""

    def __init__(self, page_size: int):
        self.page_size = page_size
