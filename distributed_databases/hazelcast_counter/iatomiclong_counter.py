import hazelcast
import threading
import time

THREADS = 10
ITERATIONS = 10_000
KEY = "counter"

client = hazelcast.HazelcastClient(
    cluster_name="lab-cluster",
    cluster_members=[
        "127.0.0.1:5701",
        "127.0.0.1:5702",
        "127.0.0.1:5703",
    ],
    redo_operation=True,
)

atomic_long = client.cp_subsystem.get_atomic_long("atomic_counter").blocking()

def reset_atomic():
    atomic_long.set(0)

def run_test(target_func, name):
    reset_atomic()
    threads = []
    start_time = time.time()

    for _ in range(THREADS):
        t = threading.Thread(target=target_func)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    final_val = atomic_long.get()
    print(f"[{name}] Time: {end_time - start_time:.2f}s, Final Value: {final_val}")

def atomic_long_counter():
    for _ in range(ITERATIONS):
        atomic_long.increment_and_get()

if __name__ == "__main__":
    print("Hazelcast IAtomicLong Counter Test Killed leader\n")
    run_test(atomic_long_counter, "IAtomicLong — CP Subsystem (Raft)")

    client.shutdown()
