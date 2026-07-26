"""Concurrency tests"""

import os
import threading

from cow_btree.page_backend import InMemoryPageBackend, MMapPageBackend
from cow_btree.store import Store

PAGE_SIZE = 256


def test_concurrent_readers_during_writes_in_memory():
    backend = InMemoryPageBackend(PAGE_SIZE)
    store = Store(backend)

    # Seed some keys so readers have something to observe from the start.
    seeded_keys = [f"seed{i}".encode() for i in range(10)]
    for k in seeded_keys:
        store.put(k, b"0")

    stop = threading.Event()
    errors = []

    def writer():
        for i in range(200):
            for k in seeded_keys:
                store.put(k, str(i).encode())
        stop.set()

    def reader():
        while not stop.is_set():
            for k in seeded_keys:
                v = store.get(k)
                if v is None:
                    errors.append(f"lost read for {k!r}")
                    return
                try:
                    int(v)
                except ValueError:
                    errors.append(f"torn/corrupt read for {k!r}: {v!r}")
                    return

    writer_thread = threading.Thread(target=writer)
    reader_threads = [threading.Thread(target=reader) for _ in range(8)]

    writer_thread.start()
    for t in reader_threads:
        t.start()

    writer_thread.join(timeout=30)
    stop.set()
    for t in reader_threads:
        t.join(timeout=30)

    assert not errors, errors


def test_concurrent_readers_never_see_corrupt_values_across_two_keys():
    """A writer updating two keys in a tight loop; every read of either key
    must return a well-formed value that was actually written.
    """
    backend = InMemoryPageBackend(PAGE_SIZE)
    store = Store(backend)
    store.put(b"a", b"0")
    store.put(b"b", b"0")

    stop = threading.Event()
    errors = []
    iterations = 300

    def writer():
        for i in range(1, iterations + 1):
            val = str(i).encode()
            store.put(b"a", val)
            store.put(b"b", val)
        stop.set()

    def reader():
        while not stop.is_set():
            va = store.get(b"a")
            vb = store.get(b"b")
            if va is None or vb is None:
                errors.append("lost read")
                return

            if int(va) < 0 or int(vb) < 0:
                errors.append("corrupt value")
                return

    writer_thread = threading.Thread(target=writer)
    readers = [threading.Thread(target=reader) for _ in range(6)]
    writer_thread.start()
    for t in readers:
        t.start()
    writer_thread.join(timeout=30)
    stop.set()
    for t in readers:
        t.join(timeout=30)

    assert not errors, errors
    assert store.get(b"a") == str(iterations).encode()


def test_file_size_bounded_under_repeated_overwrites(tmp_path):
    """Repeatedly overwriting the same keys must not grow the file
    without bound - proves space reuse."""
    path = str(tmp_path / "bounded.db")
    backend = MMapPageBackend(path, PAGE_SIZE)
    store = Store(backend)

    keys = [f"key{i}".encode() for i in range(5)]
    for k in keys:
        store.put(k, b"x" * 20)

    size_after_warmup = os.path.getsize(path)

    for i in range(500):
        for k in keys:
            store.put(k, (str(i) * 5).encode())

    size_after_many_overwrites = os.path.getsize(path)
    store.close()

    assert size_after_many_overwrites <= size_after_warmup + 20 * PAGE_SIZE, (
        size_after_warmup,
        size_after_many_overwrites,
    )


def test_concurrent_readers_while_the_mmap_file_grows(tmp_path):
    """Readers must survive the writer growing the file."""
    path = str(tmp_path / "growing.db")
    store = Store(MMapPageBackend(path, PAGE_SIZE))

    value = b"v" * 20
    seeded_keys = [f"seed{i:04d}".encode() for i in range(20)]
    for k in seeded_keys:
        store.put(k, value)

    size_after_seed = os.path.getsize(path)
    stop = threading.Event()
    errors = []

    def writer():
        try:
            for i in range(150):
                store.put(f"grow{i:05d}".encode(), value)
        except BaseException as exc:
            errors.append(f"writer {type(exc).__name__}: {exc}")
        finally:
            stop.set()

    def reader():
        try:
            while not stop.is_set():
                for k in seeded_keys:
                    v = store.get(k)
                    if v != value:
                        errors.append(f"bad read for {k!r}: {v!r}")
                        return
        except BaseException as exc:
            errors.append(f"reader {type(exc).__name__}: {exc}")

    writer_thread = threading.Thread(target=writer)
    reader_threads = [threading.Thread(target=reader) for _ in range(8)]
    writer_thread.start()
    for t in reader_threads:
        t.start()
    writer_thread.join(timeout=60)
    stop.set()
    for t in reader_threads:
        t.join(timeout=60)

    assert not errors, errors
    assert os.path.getsize(path) > size_after_seed
    assert len(store._backend._retired_maps) >= 2
    assert all(not m.closed for m in store._backend._retired_maps)
    for k in seeded_keys:
        assert store.get(k) == value
    store.close()

    assert os.path.getsize(path) % PAGE_SIZE == 0
    reopened = Store(MMapPageBackend(path, PAGE_SIZE))
    try:
        for k in seeded_keys:
            assert reopened.get(k) == value
    finally:
        reopened.close()


def test_concurrent_writers_are_serialized_and_all_puts_land():
    """Multiple writer threads hammering put() concurrently: single-writer
    lock must ensure no update is lost."""
    backend = InMemoryPageBackend(PAGE_SIZE)
    store = Store(backend)

    n_threads = 6
    n_per_thread = 50

    def writer(idx):
        for j in range(n_per_thread):
            key = f"t{idx}-{j}".encode()
            store.put(key, b"done")

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    for i in range(n_threads):
        for j in range(n_per_thread):
            key = f"t{i}-{j}".encode()
            assert store.get(key) == b"done"
