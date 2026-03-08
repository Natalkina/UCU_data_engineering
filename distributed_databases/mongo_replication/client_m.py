import requests
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

URL_INC = "http://127.0.0.1:8080/inc"
URL_COUNT = "http://127.0.0.1:8080/count"


def run_load_test(num_clients, requests_per_client):
    requests.get("http://127.0.0.1:8080/reset")
    time.sleep(0.2)
    total_requests = num_clients * requests_per_client
    barrier = Barrier(num_clients)

    def worker():
        session = requests.Session()
        barrier.wait()
        for _ in range(requests_per_client):
            r = session.get(URL_INC, timeout=5)
            if r.status_code != 200:
                raise RuntimeError("Request failed")

    start = time.time()

    with ThreadPoolExecutor(max_workers=num_clients) as ex:
        futures = [ex.submit(worker) for _ in range(num_clients)]
        for f in futures:
            f.result()

    elapsed = time.time() - start
    count = int(requests.get(URL_COUNT).text)
    throughput = total_requests / elapsed

    print(
        f"{num_clients} clients | "
        f"time={elapsed:.2f}s | "
        f"throughput={throughput:.1f} rps | "
        f"count={count}"
    )

if __name__ == "__main__":
    requests_per_client = 10000
    for num_clients in [1, 2, 5, 10]:
        run_load_test(num_clients=num_clients, requests_per_client=requests_per_client)
