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

hz_map = client.get_map("distributed_counter").blocking()

def reset_map():
    hz_map.put(KEY, 0)

def run_test(reset_func, target_func, name, final_getter):
    reset_func()
    threads = []
    start_time = time.time()

    for _ in range(THREADS):
        t = threading.Thread(target=target_func)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    final_val = final_getter()
    print(f"[{name}] Time: {end_time - start_time:.2f}s, Final Value: {final_val}")

# counters
def no_lock_counter():
    for _ in range(ITERATIONS):
        val = hz_map.get(KEY)
        hz_map.put(KEY, val + 1)

def pessimistic_counter():
    for _ in range(ITERATIONS):
        hz_map.lock(KEY)
        try:
            val = hz_map.get(KEY)
            hz_map.put(KEY, val + 1)
        finally:
            hz_map.unlock(KEY)

def optimistic_counter():
    for _ in range(ITERATIONS):
        while True:
            old = hz_map.get(KEY)
            if hz_map.replace_if_same(KEY, old, old + 1):
                break

def get_map_value():
    return hz_map.get(KEY)

if __name__ == "__main__":
    print("Hazelcast Distributed Map Counter Tests with Killed node\n")
    #
    # run_test(reset_map, no_lock_counter, "Distributed Map — NO LOCK", get_map_value)
    run_test(reset_map, pessimistic_counter, "Distributed Map — PESSIMISTIC LOCK", get_map_value)
    run_test(reset_map, optimistic_counter, "Distributed Map — OPTIMISTIC LOCK", get_map_value)

    client.shutdown()
