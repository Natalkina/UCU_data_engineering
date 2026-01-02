from flask import Flask
import threading
import os
from waitress import serve

app = Flask(__name__)

DB_FILE = "counter.txt"

class MemoryCounter:
    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.value += 1
            return self.value

    def get_count(self):
        with self.lock:
            return self.value

    def reset(self):
        with self.lock:
            self.value = 0

class DiskCounter:
    def __init__(self):
        self.lock = threading.Lock()
        self.save(0)
        if not os.path.exists(DB_FILE):
            self.save(0)

    def save(self, value):
        with open(DB_FILE, "w") as f:
            f.write(str(value))

    def increment(self):
        with self.lock:
            with open(DB_FILE, "r+") as f:
                val = int(f.read() or 0)
                val += 1
                f.seek(0)
                f.write(str(val))
                f.truncate()
            return val

    def get_count(self):
        with self.lock:
            with open(DB_FILE, "r") as f:
                return int(f.read() or 0)

    def reset(self):
        with self.lock:
            self.save(0)


# MemoryCounter
counter = MemoryCounter()
# DiscCounter
# counter = DiskCounter()

@app.route('/inc')
def inc():
    counter.increment()
    return "OK", 200

@app.route('/count')
def count():
    return str(counter.get_count()), 200

@app.route('/reset')
def reset():
    counter.reset()
    return "OK", 200


if __name__ == '__main__':
    serve(app, host="127.0.0.1", port=8080, threads=20)
