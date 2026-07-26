from cow_btree.page_backend import MMapPageBackend
from cow_btree.store import Store

import tempfile

temp_dir = tempfile.gettempdir()

PAGE_SIZE = 256
db_path = temp_dir + '/temp.db'

store = Store(MMapPageBackend(db_path, PAGE_SIZE))

store.put(b"hello1", b"world1")
store.put(b"hello2", b"world2")
store.put(b"hello3", b"world3")
store.put(b"hello4", b"world4")
print(store.get(b"hello4"))