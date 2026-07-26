"""Regression tests for the put() commit protocol (R2.2, R4.2, R5.2)."""

import threading

import pytest

from cow_btree.page_backend import InMemoryPageBackend, PageBackend
from cow_btree.store import Store

PAGE_SIZE = 128


# Bug A: pages must not be recycled before the new root is published


class _HookedStore(Store):
    """Store that parks the writer inside the commit's reclamation step."""

    def __init__(self, backend: PageBackend, hook):
        self._hook = hook
        super().__init__(backend)

    def _reclaim(self, new_epoch: int) -> None:
        super()._reclaim(new_epoch)
        self._hook(self, new_epoch)


def test_reader_snapshot_survives_reclamation_window():
    """A reader registering in the commit window keeps a readable snapshot.

    Fails against the pre-fix ordering (``_reclaim`` before publishing
    ``_root_id``/``_epoch``): the reader's snapshotted root page is found on
    the free list and subsequent writes recycle it, so reads through the
    snapshot return ``None`` or garbage.
    """
    keys = [f"key{i:03d}".encode() for i in range(40)]

    window_open = threading.Event()
    reader_registered = threading.Event()
    writes_done = threading.Event()
    armed = threading.Event()

    def hook(store: Store, new_epoch: int) -> None:
        # Only park once, on the commit the test arms explicitly.
        if not armed.is_set():
            return
        armed.clear()
        window_open.set()
        assert reader_registered.wait(timeout=30)

    backend = InMemoryPageBackend(PAGE_SIZE)
    store = _HookedStore(backend, hook)

    for k in keys:
        store.put(k, b"v0")
    for k in keys:
        store.put(k, b"v1")

    failures: list[str] = []
    observed: dict[str, object] = {}

    def reader() -> None:
        assert window_open.wait(timeout=30)
        token, root_id = store._begin_read()
        try:
            observed["root_id"] = root_id
            observed["free_ids_at_snapshot"] = list(store._free_ids)
            if root_id in store._free_ids:
                failures.append(
                    f"snapshotted root page {root_id} is already on the free list"
                )
            reader_registered.set()

            assert writes_done.wait(timeout=30)

            if root_id in store._free_ids:
                failures.append(
                    f"snapshotted root page {root_id} was reclaimed while in use"
                )
            for k in keys:
                value = store._tree.get(root_id, k)
                if value is None:
                    failures.append(f"lost read for {k!r} through snapshot")
                    break
                if value not in (b"v0", b"v1", b"v2"):
                    failures.append(f"torn read for {k!r}: {value!r}")
                    break
        finally:
            store._end_read(token)

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()

    armed.set()
    store.put(keys[0], b"v2")
    for k in keys:
        store.put(k, b"v2")
    writes_done.set()

    reader_thread.join(timeout=30)
    assert not reader_thread.is_alive()
    assert not failures, (failures, observed)

    for k in keys:
        assert store.get(k) == b"v2"


def test_publish_is_atomic_with_respect_to_readers():
    """``_begin_read`` never sees a new root paired with an old epoch."""
    backend = InMemoryPageBackend(PAGE_SIZE)
    store = Store(backend)
    for i in range(30):
        store.put(f"k{i:03d}".encode(), b"0")

    stop = threading.Event()
    seen: list[tuple[int, int]] = []
    errors: list[str] = []

    def writer() -> None:
        for i in range(1, 200):
            store.put(b"k000", str(i).encode())
        stop.set()

    def reader() -> None:
        while not stop.is_set():
            with store._reader_lock:
                pair = (store._epoch, store._root_id)
            seen.append(pair)
            token, root_id = store._begin_read()
            try:
                if store._tree.get(root_id, b"k000") is None:
                    errors.append("lost read")
                    return
            finally:
                store._end_read(token)

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    by_epoch: dict[int, int] = {}
    for epoch, root_id in seen:
        if by_epoch.setdefault(epoch, root_id) != root_id:
            errors.append(f"epoch {epoch} observed with two roots")
    assert not errors, errors


# Bug B: data must be durable before the header that points at it


class _RecordingBackend(PageBackend):
    """Wraps a backend and records the order of page writes and flushes."""

    def __init__(self, inner: PageBackend):
        self._inner = inner
        self.page_size = inner.page_size
        self.ops: list[tuple[str, int]] = []

    def read_page(self, page_id: int) -> bytes:
        return self._inner.read_page(page_id)

    def write_page(self, page_id: int, data: bytes) -> None:
        self._inner.write_page(page_id, data)
        self.ops.append(("write", page_id))

    def allocate_page(self) -> int:
        page_id = self._inner.allocate_page()
        self.ops.append(("allocate", page_id))
        return page_id

    def flush(self) -> None:
        self._inner.flush()
        self.ops.append(("flush", -1))

    @property
    def page_count(self) -> int:
        return self._inner.page_count

    def close(self) -> None:
        self._inner.close()


def _assert_two_phase(ops: list[tuple[str, int]]) -> None:
    header_writes = [i for i, (op, pid) in enumerate(ops) if op == "write" and pid == 0]
    assert header_writes, f"no header write recorded: {ops}"
    header = header_writes[-1]
    assert len(header_writes) == 1, f"header written more than once: {ops}"

    data_writes = [
        i for i, (op, pid) in enumerate(ops) if op == "write" and pid != 0 and i < header
    ]
    assert data_writes, f"no data page written before the header: {ops}"
    last_data = data_writes[-1]

    flushes = [i for i, (op, _) in enumerate(ops) if op == "flush"]
    assert any(last_data < i < header for i in flushes), (
        "phase 1 missing: no flush between the last data page write and the "
        f"header write: {ops}"
    )
    assert any(i > header for i in flushes), (
        f"phase 2 missing: no flush after the header write: {ops}"
    )

    assert not [
        i for i, (op, _) in enumerate(ops) if op == "write" and i > header
    ], f"page written after the commit point: {ops}"


def test_commit_flushes_data_before_and_after_the_header():
    backend = _RecordingBackend(InMemoryPageBackend(PAGE_SIZE))
    store = Store(backend)

    backend.ops = []
    store.put(b"a", b"1")
    _assert_two_phase(backend.ops)

    for i in range(60):
        store.put(f"k{i:03d}".encode(), b"x" * 8)
    backend.ops = []
    store.put(b"k030", b"y" * 8)
    _assert_two_phase(backend.ops)


def test_failure_before_the_commit_point_is_a_no_op():
    """A failure anywhere in phase 1 rolls the whole attempt back."""
    backend = InMemoryPageBackend(PAGE_SIZE)
    store = Store(backend)
    expected = {f"k{i:03d}".encode(): b"v" * 10 for i in range(40)}
    for k, v in expected.items():
        store.put(k, v)

    root_before = store._root_id
    epoch_before = store._epoch
    free_before = sorted(store._free_ids)
    pending_before = list(store._pending)
    header_before = backend.read_page(0)

    real_flush = backend.flush
    state = {"fail": True}

    def failing_flush() -> None:
        if state["fail"]:
            state["fail"] = False
            raise OSError("injected flush failure")
        real_flush()

    backend.flush = failing_flush
    with pytest.raises(OSError):
        store.put(b"k017", b"updated")
    backend.flush = real_flush

    assert store._root_id == root_before
    assert store._epoch == epoch_before
    assert sorted(store._free_ids) == free_before
    assert store._pending == pending_before
    assert store._pending_this_commit == []
    assert store._allocated_this_attempt == []
    assert backend.read_page(0) == header_before

    for k, v in expected.items():
        assert store.get(k) == v
    store.put(b"k017", b"updated")
    assert store.get(b"k017") == b"updated"


def test_free_list_is_persisted_before_the_header():
    """The free-list containers belong to phase 1, not after the header."""
    backend = _RecordingBackend(InMemoryPageBackend(PAGE_SIZE))
    store = Store(backend)
    keys = [f"k{i:03d}".encode() for i in range(40)]
    for k in keys:
        store.put(k, b"v")
    token, _root_id = store._begin_read()
    for k in keys[:20]:
        store.put(k, b"w")
    store._end_read(token)
    store.put(keys[0], b"x")
    assert store._free_ids, "test needs a non-empty in-memory free list"

    backend.ops = []
    store.put(b"k010", b"y")
    assert store._free_list_head != 0, "free list was not persisted"

    containers = set(store._free_list_containers)
    header = max(
        i for i, (op, pid) in enumerate(backend.ops) if op == "write" and pid == 0
    )
    container_writes = [
        i
        for i, (op, pid) in enumerate(backend.ops)
        if op == "write" and pid in containers
    ]
    assert container_writes, f"free list was not rewritten: {backend.ops}"
    assert max(container_writes) < header, (
        f"free-list container written after the header: {backend.ops}"
    )
