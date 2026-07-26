# Design Note (R7.4)

## On-disk page format

The file is a flat array of fixed-size pages (`page_size` bytes each).

- **Page 0** is the header: a marker byte, the current root page id, the
  free-list head page id, and the page size (`_HEADER_FMT` in `store.py`).
- **Leaf / internal nodes** (`node.py`) start with a 9-byte header: type
  marker (1 byte, `LEAF`/`INTERNAL`), own page id (4 bytes), entry/key
  count (4 bytes). Leaves store `(key, value)` pairs as length-prefixed
  blobs. Internal nodes store `N` separator keys plus `N+1` child page
  ids, since a routing node with `N` keys always has one more child than
  it has keys.
- **Free-list pages** (`FREE_LIST` marker) chain together lists of
  reclaimed page ids (`_FL_HEADER`: marker, own id, next page id, count).
- Every page is zero-padded to exactly `page_size`; `NodeTooLargeError` is
  raised if a node's serialized form would overflow one page.

## Copy-on-write commit

Nodes are never mutated in place. A `put` walks the path from root to
leaf, and every node on that path is rewritten as a *new* page (new page
id, old page id retired) — this is standard COW-B-tree path copying:
children point at fixed page ids, so once a parent is rewritten with a
new child id, the whole path up to the root changes.

The commit protocol in `Store.put`:
1. Build the new tree via `BTree.put`, allocating fresh pages for every
   changed node (`_Allocator.write_node` / `allocate`).
2. Persist the free-list state and `flush()` the backend so all new pages
   are durable *before* they're referenced.
3. Publish the new root by overwriting page 0 (`_write_header`) and
   `flush()` again.

Only after the header flush is the new root visible: `_root_id` is
updated inside the reader lock, so readers either see the fully-old tree
or the fully-new tree, never a mix. Readers never take the write lock and
never block writers (`_begin_read`/`_end_read` just record which epoch a
reader is using).

## Reclamation scheme

Pages replaced by a commit can't be freed immediately — a concurrent
reader may still be traversing them via the old root. Each commit
therefore:
- Tags every page it retired with the epoch that will make it safe to
  reuse (`_pending_this_commit` → `_pending` list of `(retire_epoch,
  page_id)`).
- Tracks the epoch each active reader started at (`_active_readers`).
- After publishing, calls `_reclaim(new_epoch)`, which computes the
  minimum epoch across active readers and moves any pending page whose
  retire epoch is `<= min_active_epoch` into the real free list
  (`_free_ids`), making it available to `_allocate_page_id` for reuse.
  Pages retired more recently than the oldest active reader stay pending.

This is an epoch-based (MVCC-style) garbage collector: it's safe because
a reader that started at epoch *E* can only ever reach pages that were
alive at or after *E*, so any page retired at an epoch a reader has
already passed can be reclaimed once no reader is still behind it.
