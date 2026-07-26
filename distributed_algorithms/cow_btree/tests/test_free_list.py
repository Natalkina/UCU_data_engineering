"""Free-list persistence: write amplification, page accounting, recovery."""

import os
import struct

import pytest

from cow_btree.node import InternalNode, NodeTooLargeError, deserialize_node
from cow_btree.page_backend import InMemoryPageBackend, MMapPageBackend
from cow_btree.store import FREE_LIST, Store

SMALL_PAGE = 128
PAGE_SIZE = 256

_HEADER_FMT = struct.Struct("<BII")
_FL_HEADER = struct.Struct("<BIII")
_U32 = struct.Struct("<I")


class CountingBackend(InMemoryPageBackend):
    """In-memory backend that counts write_page calls."""

    def __init__(self, page_size: int):
        super().__init__(page_size)
        self.writes = 0

    def write_page(self, page_id: int, data: bytes) -> None:
        self.writes += 1
        super().write_page(page_id, data)


def _writes_for_one_put(store: Store, backend: CountingBackend, key: bytes,
                        value: bytes) -> int:
    backend.writes = 0
    store.put(key, value)
    return backend.writes


def _walk_free_list(backend, head: int) -> tuple[list[int], list[int]]:
    """Return (container ids, free page ids) by walking the on-disk chain."""
    containers: list[int] = []
    free_ids: list[int] = []
    page_id = head
    while page_id != 0:
        raw = backend.read_page(page_id)
        marker, own_id, next_page, count = _FL_HEADER.unpack_from(raw, 0)
        assert marker == FREE_LIST, f"page {page_id} is not a free-list page"
        assert own_id == page_id, f"page {page_id} carries id {own_id}"
        containers.append(page_id)
        offset = _FL_HEADER.size
        for _ in range(count):
            (pid,) = _U32.unpack_from(raw, offset)
            free_ids.append(pid)
            offset += 4
        page_id = next_page
    return containers, free_ids


def _unreachable_pages(path: str, page_size: int) -> list[int]:
    """Page ids in the file that nothing in the header's graph names."""
    backend = MMapPageBackend(path, page_size)
    try:
        marker, root_id, free_head = _HEADER_FMT.unpack_from(backend.read_page(0), 0)
        assert marker == 0x2A
        seen = {0}

        stack = [root_id]
        while stack:
            page_id = stack.pop()
            seen.add(page_id)
            node = deserialize_node(backend.read_page(page_id))
            if isinstance(node, InternalNode):
                stack.extend(node.children)

        containers, free_ids = _walk_free_list(backend, free_head)
        seen.update(containers)
        seen.update(free_ids)
        return sorted(set(range(backend.page_count)) - seen)
    finally:
        backend.close()


# write amplification


def test_write_amplification_does_not_grow_with_the_free_set():
    """One put costs O(tree height) writes no matter how big the free set is."""
    backend = CountingBackend(SMALL_PAGE)
    store = Store(backend)
    keys = [f"k{i:03d}".encode() for i in range(40)]
    for k in keys:
        store.put(k, b"v" * 8)

    baseline = _writes_for_one_put(store, backend, keys[0], b"z" * 8)

    token, _ = store._begin_read()
    for _ in range(20):
        for k in keys:
            store.put(k, b"w" * 8)
    store._end_read(token)
    store.put(keys[0], b"u" * 8)
    store.put(keys[0], b"c" * 8)

    assert len(store._free_ids) > 1000, "the parked reader should have grown the free set"
    after = _writes_for_one_put(store, backend, keys[0], b"y" * 8)
    assert after <= baseline + 2, (baseline, after, len(store._free_ids))

    for i, k in enumerate(keys):
        assert store.get(k) is not None, i


def test_free_list_io_per_commit_is_one_container_in_the_steady_state():
    """Only the container holding the top of the stack is rewritten"""
    backend = CountingBackend(SMALL_PAGE)
    store = Store(backend)
    for i in range(200):
        store.put(f"k{i:04d}".encode(), b"v" * 8)

    # Force a multi-container free set, then confirm a commit touches at
    store._free_ids.extend(range(store._backend.page_count, store._backend.page_count + 300))
    for _ in range(300):
        store._backend.allocate_page()
    containers_before = len(store._free_list_containers)
    store.put(b"k0000", b"w" * 8)
    assert len(store._free_list_containers) > 3

    written: list[int] = []
    real_write_page = backend.write_page

    def spy(page_id, data):
        written.append(page_id)
        real_write_page(page_id, data)

    backend.write_page = spy
    store.put(b"k0001", b"x" * 8)
    backend.write_page = real_write_page

    containers = set(store._free_list_containers)
    touched_containers = [pid for pid in written if pid in containers]
    assert len(touched_containers) <= 2, (touched_containers, containers_before)


# тo page falls out of the graph


def test_every_page_is_reachable_from_the_header_after_reopen(tmp_path):
    """Tree pages + containers + free ids must cover the whole file."""
    path = str(tmp_path / "accounting.db")
    for cycle in range(3):
        store = Store(MMapPageBackend(path, PAGE_SIZE))
        for i in range(400):
            store.put(f"k{i:04d}".encode(), f"v{cycle}".encode() * 3)
        store.close()
        assert _unreachable_pages(path, PAGE_SIZE) == [], f"cycle {cycle}"


def test_open_close_cycles_do_not_grow_the_file(tmp_path):
    """close() persists the last commit's reclamation, so nothing leaks."""
    path = str(tmp_path / "cycles.db")
    sizes = []
    for cycle in range(4):
        store = Store(MMapPageBackend(path, PAGE_SIZE))
        for i in range(50):
            store.put(f"k{i:04d}".encode(), f"v{cycle}".encode() * 4)
        store.close()
        sizes.append(os.path.getsize(path))
    assert sizes[1:] == [sizes[1]] * 3, sizes


def test_container_pool_stays_linked_when_the_free_set_shrinks(tmp_path):
    """A container the free set shrank out of keeps count = 0, stays linked."""
    path = str(tmp_path / "shrink.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))
    for i in range(400):
        store.put(f"k{i:04d}".encode(), b"v" * 10)
    store._free_ids.extend(range(store._backend.page_count, store._backend.page_count + 200))
    for _ in range(200):
        store._backend.allocate_page()
    store.put(b"k0000", b"w" * 10)
    pool = list(store._free_list_containers)
    assert len(pool) > 3

    store._free_ids.clear()
    store._free_clean = 0
    store.put(b"k0001", b"x" * 10)

    assert store._free_list_head != 0
    containers, free_ids = _walk_free_list(store._backend, store._free_list_head)
    assert sorted(containers) == sorted(pool), "a container fell out of the chain"
    assert free_ids == [] or len(free_ids) < 10
    store.close()

    reopened = Store(MMapPageBackend(path, PAGE_SIZE))
    assert sorted(reopened._free_list_containers) == sorted(pool)
    reopened.close()


def test_a_fresh_store_still_has_a_free_list_head(tmp_path):
    """Even with nothing free, one container is written so the head lives on."""
    path = str(tmp_path / "fresh.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))
    store.put(b"k", b"v")
    assert store._free_list_head != 0
    containers, free_ids = _walk_free_list(store._backend, store._free_list_head)
    assert containers == store._free_list_containers
    store.close()
    assert _unreachable_pages(path, PAGE_SIZE) == []


# recovery of a large free set


def test_reopen_recovers_a_large_free_set_and_reuses_it(tmp_path):
    """A recovered free set must be usable, not just readable."""
    path = str(tmp_path / "reuse.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))
    keys = [f"k{i:04d}".encode() for i in range(60)]
    for k in keys:
        store.put(k, b"v" * 10)

    token, _ = store._begin_read()
    for _ in range(10):
        for k in keys:
            store.put(k, b"w" * 10)
    store._end_read(token)
    store.put(keys[0], b"u" * 10)
    in_memory_free = len(store._free_ids)
    assert in_memory_free > 200
    store.close()

    backend = MMapPageBackend(path, PAGE_SIZE)
    reopened = Store(backend)
    assert len(reopened._free_ids) >= in_memory_free
    assert not set(reopened._free_ids) & set(reopened._free_list_containers)
    for k in keys:
        assert reopened.get(k) is not None

    pages_before = backend.page_count
    for _ in range(10):
        for k in keys:
            reopened.put(k, b"z" * 10)
    assert backend.page_count == pages_before, "recovered free pages were not reused"
    for k in keys:
        assert reopened.get(k) == b"z" * 10
    reopened.close()
    assert _unreachable_pages(path, PAGE_SIZE) == []


def test_reopen_repositions_a_chain_written_in_a_different_order(tmp_path):
    """A chain whose containers are not in stack position heals on the next commit."""
    path = str(tmp_path / "legacy_order.db")
    store = Store(MMapPageBackend(path, SMALL_PAGE))
    for i in range(60):
        store.put(f"k{i:04d}".encode(), b"v" * 6)
    first_spare = store._backend.page_count
    for _ in range(60):
        store._backend.allocate_page()
    store._free_ids.extend(range(first_spare, first_spare + 60))
    store.put(b"k0000", b"w" * 6)
    assert len(store._free_list_containers) >= 3
    store.close()

    backend = MMapPageBackend(path, SMALL_PAGE)
    _, root_id, head = _HEADER_FMT.unpack_from(backend.read_page(0), 0)
    containers, _ = _walk_free_list(backend, head)
    containers.reverse()
    chunks = []
    for container_id in containers:
        raw = backend.read_page(container_id)
        _, _, _, count = _FL_HEADER.unpack_from(raw, 0)
        chunks.append(
            [_U32.unpack_from(raw, _FL_HEADER.size + 4 * i)[0] for i in range(count)]
        )
    expected_free = {pid for chunk in chunks for pid in chunk}
    for index, (container_id, chunk) in enumerate(zip(containers, chunks)):
        next_page = containers[index + 1] if index + 1 < len(containers) else 0
        raw = bytearray(SMALL_PAGE)
        _FL_HEADER.pack_into(raw, 0, FREE_LIST, container_id, next_page, len(chunk))
        for i, pid in enumerate(chunk):
            _U32.pack_into(raw, _FL_HEADER.size + 4 * i, pid)
        backend.write_page(container_id, bytes(raw))
    header = bytearray(SMALL_PAGE)
    _HEADER_FMT.pack_into(header, 0, 0x2A, root_id, containers[0])
    backend.write_page(0, bytes(header))
    backend.close()

    reopened = Store(MMapPageBackend(path, SMALL_PAGE))
    assert set(reopened._free_ids) == expected_free
    assert reopened._free_clean == 0, "a mis-positioned chain must not count as clean"
    handed_out = []
    for i in range(60, 100):
        reopened.put(f"k{i:04d}".encode(), b"x" * 6)
        handed_out.extend(reopened._allocated_this_attempt)
    reopened.close()

    backend = MMapPageBackend(path, SMALL_PAGE)
    _, _, head = _HEADER_FMT.unpack_from(backend.read_page(0), 0)
    _, free_ids = _walk_free_list(backend, head)
    backend.close()
    assert len(free_ids) == len(set(free_ids)), "duplicate id on the free list"
    assert _unreachable_pages(path, SMALL_PAGE) == []

    final = Store(MMapPageBackend(path, SMALL_PAGE))
    for i in range(100):
        assert final.get(f"k{i:04d}".encode()) is not None, i
    final.close()


# corrupt metadata is rejected, not parsed


def _reopenable_store_with_containers() -> InMemoryPageBackend:
    """A closed-over backend holding a committed store with a free list."""
    backend = InMemoryPageBackend(SMALL_PAGE)
    store = Store(backend)
    for i in range(30):
        store.put(f"k{i:03d}".encode(), b"v" * 6)
    for i in range(30):
        store.put(f"k{i:03d}".encode(), b"w" * 6)
    store.close()
    assert store._free_list_containers
    return backend, store._free_list_containers[-1]


def test_reopen_rejects_a_free_list_page_with_a_bad_marker():
    backend, head = _reopenable_store_with_containers()
    raw = bytearray(backend.read_page(head))
    raw[0] = FREE_LIST + 7
    backend.write_page(head, bytes(raw))

    with pytest.raises(ValueError, match="not a free-list page"):
        Store(backend)


def test_reopen_rejects_a_free_list_page_carrying_the_wrong_page_id():
    backend, head = _reopenable_store_with_containers()
    raw = bytearray(backend.read_page(head))
    _U32.pack_into(raw, 1, head + 1000)
    backend.write_page(head, bytes(raw))

    with pytest.raises(ValueError, match="describes itself as page"):
        Store(backend)


def test_reopen_rejects_a_tree_page_that_is_not_the_page_it_claims():
    """A mis-routed child pointer must fail the read, not parse."""
    backend = InMemoryPageBackend(SMALL_PAGE)
    store = Store(backend)
    for i in range(30):
        store.put(f"k{i:03d}".encode(), b"v" * 6)

    root = deserialize_node(backend.read_page(store._root_id), store._root_id)
    assert isinstance(root, InternalNode), "need a tree with an internal root"
    a, b = root.children[0], root.children[1]
    page_a = backend.read_page(a)
    backend.write_page(a, backend.read_page(b))
    backend.write_page(b, page_a)

    with pytest.raises(ValueError, match="describes itself as page"):
        for i in range(30):
            store.get(f"k{i:03d}".encode())


# close() robustness


def test_close_is_idempotent(tmp_path):
    path = str(tmp_path / "twice.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))
    store.put(b"k", b"v")
    store.close()
    store.close()
    size = os.path.getsize(path)

    reopened = Store(MMapPageBackend(path, PAGE_SIZE))
    assert reopened.get(b"k") == b"v"
    reopened.close()
    assert os.path.getsize(path) == size


def test_close_after_a_failed_put_is_safe(tmp_path):
    """An aborted attempt leaves root and free set consistent; close persists that."""
    path = str(tmp_path / "failed.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))
    for i in range(40):
        store.put(f"k{i:04d}".encode(), b"v" * 10)
    with pytest.raises(NodeTooLargeError):
        store.put(b"huge", b"x" * (PAGE_SIZE * 2))
    store.close()

    assert _unreachable_pages(path, PAGE_SIZE) == []
    reopened = Store(MMapPageBackend(path, PAGE_SIZE))
    for i in range(40):
        assert reopened.get(f"k{i:04d}".encode()) == b"v" * 10
    assert reopened.get(b"huge") is None
    reopened.close()
