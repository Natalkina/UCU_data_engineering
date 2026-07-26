from __future__ import annotations

import abc
import mmap
import os


class PageBackend(abc.ABC):
    """Abstract interface for fixed-size page storage."""

    page_size: int

    @abc.abstractmethod
    def read_page(self, page_id: int) -> bytes:
        """Return the raw bytes stored at page_id."""

    @abc.abstractmethod
    def write_page(self, page_id: int, data: bytes) -> None:
        """Persist data (must be exactly page_size bytes) at page_id."""

    @abc.abstractmethod
    def allocate_page(self) -> int:
        """Grow the backing storage by one page and return its id."""

    @abc.abstractmethod
    def flush(self) -> None:
        """Ensure all writes so far are durable."""

    @property
    @abc.abstractmethod
    def page_count(self) -> int:
        """Total number of pages currently allocated (including page 0)."""

    def close(self) -> None:  # pragma: no cover - trivial default
        """Release any resources. Default is a no-op."""


class InMemoryPageBackend(PageBackend):
    """Keeps all pages in a Python list. Used for fast unit tests."""

    def __init__(self, page_size: int):
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        self.page_size = page_size
        self._pages: list[bytearray] = []

    def read_page(self, page_id: int) -> bytes:
        self._check_id(page_id)
        return bytes(self._pages[page_id])

    def write_page(self, page_id: int, data: bytes) -> None:
        self._check_id(page_id)
        self._check_data(data)
        self._pages[page_id] = bytearray(data)

    def allocate_page(self) -> int:
        self._pages.append(bytearray(self.page_size))
        return len(self._pages) - 1

    def flush(self) -> None:
        pass

    @property
    def page_count(self) -> int:
        return len(self._pages)

    def _check_id(self, page_id: int) -> None:
        if not (0 <= page_id < len(self._pages)):
            raise IndexError(f"page id {page_id} out of range")

    def _check_data(self, data: bytes) -> None:
        if len(data) != self.page_size:
            raise ValueError(
                f"page data must be exactly {self.page_size} bytes, got {len(data)}"
            )


class MMapPageBackend(PageBackend):
    """Stores pages in a real file, cached via mmap"""

    def __init__(self, path: str, page_size: int):
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        self.page_size = page_size
        self._path = path

        # Open for read/write, creating if necessary.
        self._fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            size = os.fstat(self._fd).st_size
            if 0 < size < page_size:
                raise ValueError(
                    f"{path!r} is {size} bytes, less than one page of "
                    f"{page_size} bytes: it is not an empty file and cannot "
                    "be read at this page size (it was most likely written "
                    "with a smaller one)"
                )
            self._mapped_size = size
            self._page_count = size // page_size
            self._mmap: mmap.mmap | None = None
            if size > 0:
                self._mmap = mmap.mmap(self._fd, size)
            self._retired_maps: list[mmap.mmap] = []
        except Exception:
            os.close(self._fd)
            raise

    # helpers

    def _grown_size(self, min_size: int) -> int:
        """Next mapped size that fits min_size, rounded up to a page."""
        target = max(min_size, self._mapped_size * 2)
        pages = -(-target // self.page_size)  # ceil division
        return pages * self.page_size

    def _remap(self, new_size: int) -> None:
        """Grow the file to new_size and publish a larger mapping."""
        old = self._mmap
        if old is not None:
            old.flush()
        os.ftruncate(self._fd, new_size)
        new_map = mmap.mmap(self._fd, new_size)
        self._mmap = new_map
        self._mapped_size = new_size
        if old is not None:
            self._retired_maps.append(old)

    def _check_id(self, page_id: int) -> None:
        if page_id < 0 or page_id >= self._page_count:
            raise IndexError(f"page id {page_id} out of range")

    # page_backend interface

    def read_page(self, page_id: int) -> bytes:
        self._check_id(page_id)
        offset = page_id * self.page_size
        current = self._mmap
        assert current is not None
        return bytes(current[offset : offset + self.page_size])

    def write_page(self, page_id: int, data: bytes) -> None:
        self._check_id(page_id)
        if len(data) != self.page_size:
            raise ValueError(
                f"page data must be exactly {self.page_size} bytes, got {len(data)}"
            )
        offset = page_id * self.page_size
        current = self._mmap
        assert current is not None
        current[offset : offset + self.page_size] = data

    def allocate_page(self) -> int:
        page_id = self._page_count
        needed = (page_id + 1) * self.page_size
        if needed > self._mapped_size:
            self._remap(self._grown_size(needed))
        self._page_count = page_id + 1
        return page_id

    def flush(self) -> None:
        current = self._mmap
        if current is not None:
            current.flush()
        os.fsync(self._fd)

    @property
    def page_count(self) -> int:
        return self._page_count

    def close(self) -> None:
        if self._mmap is not None:
            self._mmap.flush()
            self._mmap.close()
            self._mmap = None
        for retired in self._retired_maps:
            retired.close()
        self._retired_maps = []
        logical_size = self._page_count * self.page_size
        if logical_size < self._mapped_size:
            os.ftruncate(self._fd, logical_size)
            self._mapped_size = logical_size
            os.fsync(self._fd)
        os.close(self._fd)
