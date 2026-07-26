from __future__ import annotations

import struct
import threading

from .btree import BTree
from .node import LeafNode, deserialize_node
from .page_backend import PageBackend

_HEADER_MARKER = 0x2A
_HEADER_FMT = struct.Struct("<BIII")
FREE_LIST = 3

_FL_HEADER = struct.Struct("<BIII")  # marker, page id, next page id, count
_U32 = struct.Struct("<I")


def _as_bytes(value: object, what: str) -> bytes:
    """Normalize a bytes-like argument to immutable bytes."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"{what} must be bytes-like, got {type(value).__name__}")


class Store:
    """Persistent, thread-safe, copy-on-write B+-tree key-value store."""

    def __init__(self, backend: PageBackend):
        self._backend = backend
        self._write_lock = threading.Lock()
        self._reader_lock = threading.Lock()
        self._active_readers: dict[int, int] = {}  # token -> epoch
        self._next_reader_token = 0
        self._pending: list[tuple[int, int]] = []  # (retire_epoch, page_id)
        self._pending_this_commit: list[int] = []
        self._allocated_this_attempt: list[int] = []
        self._free_ids: list[int] = []
        self._free_list_containers: list[int] = []
        self._free_clean = 0
        self._free_dirty_containers = 0
        self._epoch = 0
        self._closed = False

        if backend.page_count == 0:
            self._init_fresh()
        else:
            self._recover()

        self._tree = BTree(_Allocator(self))

    # initialization / recovery

    def _init_fresh(self) -> None:
        header_id = self._backend.allocate_page()
        assert header_id == 0
        root_id = self._backend.allocate_page()
        empty_leaf = LeafNode()
        self._backend.write_page(
            root_id, empty_leaf.serialize(root_id, self._backend.page_size)
        )
        self._root_id = root_id
        self._free_list_head = 0
        self._backend.flush()
        self._write_header(root_id)
        self._backend.flush()

    def _recover(self) -> None:
        raw = self._backend.read_page(0)
        marker, root_id, free_list_head, page_size = _HEADER_FMT.unpack_from(raw, 0)
        if marker != _HEADER_MARKER:
            raise ValueError("not a cow_btree file (bad header marker)")
        if page_size and page_size != self._backend.page_size:
            raise ValueError(
                f"file was written with page_size={page_size}, "
                f"but the backend was opened with page_size={self._backend.page_size}"
            )
        self._root_id = root_id
        self._free_list_head = free_list_head
        chunks, self._free_list_containers = self._read_free_list(free_list_head)
        self._free_ids = [pid for chunk in chunks for pid in chunk]
        self._free_clean = self._clean_prefix(chunks)
        self._free_dirty_containers = len(self._free_list_containers)

    def _clean_prefix(self, chunks: list[list[int]]) -> int:
        """How many recovered ids are in the slot _persist_free_list expects."""
        max_per_page = self._ids_per_container
        clean = 0
        for index, chunk in enumerate(chunks):
            if len(chunk) == max_per_page:
                clean += len(chunk)
                continue
            if all(not rest for rest in chunks[index + 1 :]):
                clean += len(chunk)
            break
        return clean

    def _read_free_list(self, head: int) -> tuple[list[list[int]], list[int]]:
        """Return (per-container id chunks, container page ids) from the chain."""
        chunks: list[list[int]] = []
        containers: list[int] = []
        page_id = head
        while page_id != 0:
            raw = self._backend.read_page(page_id)
            marker, stored_page_id, next_page, count = _FL_HEADER.unpack_from(raw, 0)
            if marker != FREE_LIST:
                raise ValueError(
                    f"page {page_id} is not a free-list page "
                    f"(expected marker {FREE_LIST}, found {marker})"
                )
            if stored_page_id != page_id:
                raise ValueError(
                    f"free-list page {page_id} describes itself as page "
                    f"{stored_page_id} (broken chain)"
                )
            offset = _FL_HEADER.size
            chunk: list[int] = []
            for _ in range(count):
                (pid,) = _U32.unpack_from(raw, offset)
                chunk.append(pid)
                offset += 4
            chunks.append(chunk)
            containers.append(page_id)
            page_id = next_page
        containers.reverse()
        chunks.reverse()
        return chunks, containers

    def _write_header(self, root_id: int) -> None:
        """Write page 0 naming root_id as the published root."""
        raw = bytearray(self._backend.page_size)
        _HEADER_FMT.pack_into(
            raw,
            0,
            _HEADER_MARKER,
            root_id,
            self._free_list_head,
            self._backend.page_size,
        )
        self._backend.write_page(0, bytes(raw))

    @property
    def _ids_per_container(self) -> int:
        """How many uint32 page ids fit in one free-list container page."""
        return (self._backend.page_size - _FL_HEADER.size) // 4

    def _write_container(self, index: int) -> None:
        """Write container index with the slice of the free set it owns. """
        max_per_page = self._ids_per_container
        container_id = self._free_list_containers[index]
        chunk = self._free_ids[index * max_per_page : (index + 1) * max_per_page]
        next_page = self._free_list_containers[index - 1] if index else 0
        raw = bytearray(self._backend.page_size)
        _FL_HEADER.pack_into(raw, 0, FREE_LIST, container_id, next_page, len(chunk))
        offset = _FL_HEADER.size
        for pid in chunk:
            _U32.pack_into(raw, offset, pid)
            offset += 4
        self._backend.write_page(container_id, bytes(raw))

    def _persist_free_list(self) -> None:
        """Mirror self._free_ids onto the container chain.bite"""
        max_per_page = self._ids_per_container
        count = len(self._free_ids)
        needed = max(1, -(-count // max_per_page))
        while len(self._free_list_containers) < needed:
            self._free_list_containers.append(self._backend.allocate_page())

        start = self._free_clean // max_per_page
        end = max(needed, self._free_dirty_containers)
        self._free_dirty_containers = end
        for index in range(start, end):
            self._write_container(index)

        self._free_list_head = self._free_list_containers[-1]
        self._free_clean = count
        self._free_dirty_containers = needed

    # public API

    def put(self, key: bytes, value: bytes) -> None:
        """Insert or overwrite key with value."""
        key = _as_bytes(key, "key")
        value = _as_bytes(value, "value")
        with self._write_lock:
            self._begin_attempt()
            new_epoch = self._epoch + 1
            try:
                new_root_id = self._tree.put(self._root_id, key, value)
                self._persist_free_list()
                self._backend.flush()
            except BaseException:
                self._abort_attempt()
                raise
            self._finish_attempt()
            self._write_header(new_root_id)
            self._backend.flush()

            for page_id in self._pending_this_commit:
                self._pending.append((new_epoch, page_id))
            self._pending_this_commit = []

            with self._reader_lock:
                self._root_id = new_root_id
                self._epoch = new_epoch

            self._reclaim(new_epoch)

    def get(self, key: bytes) -> bytes | None:
        """Look up key, returning its value or None if absent."""
        key = _as_bytes(key, "key")
        token, root_id = self._begin_read()
        try:
            return self._tree.get(root_id, key)
        finally:
            self._end_read(token)

    def close(self) -> None:
        """Flush the final free-list state and release the backend."""
        with self._write_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._reclaim(self._epoch)
                self._persist_free_list()
                self._backend.flush()
                self._write_header(self._root_id)
                self._backend.flush()
            finally:
                self._backend.close()

    # reader epoch registry

    def _begin_read(self) -> tuple[int, int]:
        with self._reader_lock:
            token = self._next_reader_token
            self._next_reader_token += 1
            epoch = self._epoch
            self._active_readers[token] = epoch
            return token, self._root_id

    def _end_read(self, token: int) -> None:
        with self._reader_lock:
            del self._active_readers[token]

    def _min_active_epoch(self) -> int | None:
        with self._reader_lock:
            if not self._active_readers:
                return None
            return min(self._active_readers.values())

    def _reclaim(self, new_epoch: int) -> None:
        """Move pages from the pending (retired) list to the real free list."""
        min_epoch = self._min_active_epoch()
        still_pending = []
        for retire_epoch, page_id in self._pending:
            if min_epoch is None or min_epoch >= retire_epoch:
                self._free_ids.append(page_id)
            else:
                still_pending.append((retire_epoch, page_id))
        self._pending = still_pending

    # write attempt bookkeeping

    def _begin_attempt(self) -> None:
        self._allocated_this_attempt = []
        self._pending_this_commit = []

    def _abort_attempt(self) -> None:
        """Roll back an attempt that raised before anything was published."""
        while self._allocated_this_attempt:
            self._free_ids.append(self._allocated_this_attempt.pop())
        self._pending_this_commit = []

    def _finish_attempt(self) -> None:
        """Discard the allocation log once the attempt is known to succeed."""
        self._allocated_this_attempt = []

    # glue used by _Allocator

    def _allocate_page_id(self) -> int:
        if self._free_ids:
            page_id = self._free_ids.pop()
            self._free_clean = min(self._free_clean, len(self._free_ids))
        else:
            page_id = self._backend.allocate_page()
        self._allocated_this_attempt.append(page_id)
        return page_id

    def _retire_page_id(self, page_id: int) -> None:
        self._pending_this_commit.append(page_id)


class _Allocator:
    """Adapts :class:Store to the :class:~cow_btree.btree.PageAllocator protocol."""

    def __init__(self, store: Store):
        self._store = store
        self.page_size = store._backend.page_size

    def allocate(self) -> int:
        return self._store._allocate_page_id()

    def write_node(self, page_id: int, node) -> None:
        data = node.serialize(page_id, self.page_size)
        self._store._backend.write_page(page_id, data)

    def read_node(self, page_id: int):
        raw = self._store._backend.read_page(page_id)
        return deserialize_node(raw, page_id)

    def retire(self, page_id: int) -> None:
        self._store._retire_page_id(page_id)
