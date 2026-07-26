"""Integration tests against the real mmap file backend"""

import os

import pytest

from cow_btree.page_backend import MMapPageBackend
from cow_btree.store import Store

PAGE_SIZE = 512


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.db")


def test_open_or_create_new_file(db_path):
    assert not os.path.exists(db_path) or os.path.getsize(db_path) == 0
    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    assert store.get(b"anything") is None
    store.put(b"k", b"v")
    assert store.get(b"k") == b"v"
    store.close()
    assert os.path.exists(db_path)
    assert os.path.getsize(db_path) > 0


def test_reopen_recovers_data(db_path):
    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    pairs = {f"key{i}".encode(): f"value{i}".encode() for i in range(30)}
    for k, v in pairs.items():
        store.put(k, v)
    store.close()

    backend2 = MMapPageBackend(db_path, PAGE_SIZE)
    store2 = Store(backend2)
    for k, v in pairs.items():
        assert store2.get(k) == v
    store2.close()


def test_reopen_after_many_commits_and_splits(db_path):
    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    keys = [f"item-{i:04d}".encode() for i in range(300)]
    for k in keys:
        store.put(k, k[::-1])
    store.close()

    backend2 = MMapPageBackend(db_path, PAGE_SIZE)
    store2 = Store(backend2)
    for k in keys:
        assert store2.get(k) == k[::-1]
    assert store2.get(b"not-there") is None
    store2.close()


def test_reopen_preserves_upserts(db_path):
    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    store.put(b"k", b"v1")
    store.put(b"k", b"v2")
    store.put(b"k", b"v3")
    store.close()

    backend2 = MMapPageBackend(db_path, PAGE_SIZE)
    store2 = Store(backend2)
    assert store2.get(b"k") == b"v3"
    store2.close()


def test_close_trims_the_preallocated_tail(db_path):
    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    for i in range(100):
        store.put(f"key{i:04d}".encode(), b"v" * 40)
    page_count = backend.page_count

    assert os.path.getsize(db_path) >= page_count * PAGE_SIZE
    store.close()

    assert os.path.getsize(db_path) == page_count * PAGE_SIZE

    backend2 = MMapPageBackend(db_path, PAGE_SIZE)
    assert backend2.page_count == page_count
    store2 = Store(backend2)
    for i in range(100):
        assert store2.get(f"key{i:04d}".encode()) == b"v" * 40
    store2.close()


def test_reopen_of_a_file_with_an_untrimmed_tail(db_path, tmp_path):
    """A file that was never closed (crash) still reopens"""
    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    pairs = {f"k{i:03d}".encode(): f"v{i:03d}".encode() for i in range(60)}
    for k, v in pairs.items():
        store.put(k, v)
    backend.flush()

    crashed = str(tmp_path / "crashed.db")
    with open(db_path, "rb") as src, open(crashed, "wb") as dst:
        dst.write(src.read())
    store.close()

    assert os.path.getsize(crashed) % PAGE_SIZE == 0
    recovered = Store(MMapPageBackend(crashed, PAGE_SIZE))
    for k, v in pairs.items():
        assert recovered.get(k) == v
    for k in list(pairs)[:10]:
        recovered.put(k, b"updated")
    recovered.put(b"brand-new", b"x")
    for k, v in pairs.items():
        assert recovered.get(k) == (b"updated" if k in list(pairs)[:10] else v)
    assert recovered.get(b"brand-new") == b"x"
    recovered.close()


def test_reopen_with_a_different_page_size_is_rejected(db_path):
    """The header records the page size, so a mismatch fails loudly"""
    store = Store(MMapPageBackend(db_path, PAGE_SIZE))
    for i in range(40):
        store.put(f"k{i:03d}".encode(), b"v" * 20)
    store.close()

    for wrong_size in (PAGE_SIZE // 2, PAGE_SIZE * 2):
        with pytest.raises(ValueError, match="page_size"):
            Store(MMapPageBackend(db_path, wrong_size))

    reopened = Store(MMapPageBackend(db_path, PAGE_SIZE))
    for i in range(40):
        assert reopened.get(f"k{i:03d}".encode()) == b"v" * 20
    reopened.close()


def test_reopen_with_a_page_size_larger_than_the_whole_file_is_rejected(db_path):
    """A short file must never be mistaken for a new one"""
    small = 256
    store = Store(MMapPageBackend(db_path, small))
    store.put(b"k1", b"v1")
    store.put(b"k2", b"v2")
    store.close()

    with open(db_path, "rb") as fh:
        before = fh.read()
    # The premise of the bug: the file really is shorter than one big page.
    assert 0 < len(before) < 4096

    with pytest.raises(ValueError, match="less than one page"):
        Store(MMapPageBackend(db_path, 4096))

    with open(db_path, "rb") as fh:
        assert fh.read() == before  # not truncated, not overwritten

    reopened = Store(MMapPageBackend(db_path, small))
    assert reopened.get(b"k1") == b"v1"
    assert reopened.get(b"k2") == b"v2"
    reopened.close()


def test_a_file_shorter_than_a_page_is_not_treated_as_empty(db_path):
    """The same rule for any non-empty file, not just a cow_btree one."""
    with open(db_path, "wb") as fh:
        fh.write(b"junk")
    with pytest.raises(ValueError, match="less than one page"):
        MMapPageBackend(db_path, PAGE_SIZE)
    with open(db_path, "rb") as fh:
        assert fh.read() == b"junk"

    # A zero-byte file is still "create me", as open-or-create requires.
    with open(db_path, "wb"):
        pass
    store = Store(MMapPageBackend(db_path, PAGE_SIZE))
    store.put(b"k", b"v")
    assert store.get(b"k") == b"v"
    store.close()


def test_reopen_of_a_file_without_a_recorded_page_size(db_path):
    """A header written before the page-size field existed reads 0 there."""
    store = Store(MMapPageBackend(db_path, PAGE_SIZE))
    for i in range(40):
        store.put(f"k{i:03d}".encode(), b"v" * 20)
    store.close()

    with open(db_path, "r+b") as fh:
        fh.seek(9)
        fh.write(b"\x00\x00\x00\x00")

    reopened = Store(MMapPageBackend(db_path, PAGE_SIZE))
    for i in range(40):
        assert reopened.get(f"k{i:03d}".encode()) == b"v" * 20
    reopened.put(b"new", b"value")
    assert reopened.get(b"new") == b"value"
    reopened.close()

    with open(db_path, "rb") as fh:
        header = fh.read(13)
    assert int.from_bytes(header[9:13], "little") == PAGE_SIZE


def test_reopen_rejects_a_foreign_file(db_path):
    """A file that is not a cow_btree file fails on the header marker."""
    with open(db_path, "wb") as fh:
        fh.write(b"not a cow_btree file" + bytes(PAGE_SIZE * 2 - 20))
    with pytest.raises(ValueError, match="bad header marker"):
        Store(MMapPageBackend(db_path, PAGE_SIZE))


def test_multiple_reopen_cycles(db_path):
    all_pairs = {}
    for cycle in range(4):
        backend = MMapPageBackend(db_path, PAGE_SIZE)
        store = Store(backend)
        for k, v in all_pairs.items():
            assert store.get(k) == v
        for i in range(20):
            k = f"cycle{cycle}-{i}".encode()
            v = f"val{cycle}-{i}".encode()
            store.put(k, v)
            all_pairs[k] = v
        store.close()

    backend = MMapPageBackend(db_path, PAGE_SIZE)
    store = Store(backend)
    for k, v in all_pairs.items():
        assert store.get(k) == v
    store.close()
