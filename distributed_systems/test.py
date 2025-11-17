import requests
import time
import subprocess
import threading
from datetime import datetime

M = "http://localhost:42000"
S1 = "http://localhost:42001"
S2 = "http://localhost:42002"

REPLICATION_TIMEOUT = 30  # seconds

def ts():
    """Timestamp prefix for logs."""
    return datetime.now().strftime("[%H:%M:%S]")

def docker_compose(cmd):
    """Run docker-compose commands."""
    print(ts(), "docker-compose", cmd)
    result = subprocess.run(
        ["docker-compose"] + cmd.split(),
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
    return result.returncode == 0

def wait_up(url, timeout=20):
    print(ts(), f"Waiting for {url} to be up...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url + "/messages", timeout=1)
            if r.status_code == 200:
                print(ts(), f"{url} is UP")
                return True
        except:
            pass
        time.sleep(0.5)
    return False

def send(msg, w):
    print(ts(), f"POST {msg} w={w}")
    try:
        r = requests.post(
            M + "/messages",
            json={"message": msg, "write_concern": w},
            timeout=REPLICATION_TIMEOUT
        )
        print(ts(), "=>", r.status_code, r.text)
        return r
    except Exception as e:
        print(ts(), "=> Exception:", e)
        return None


# --- 1. Clean environment ---
print(ts(), "Stopping any running containers...")
docker_compose("down")

# --- 2. Start Master + S1 only ---
print(ts(), "Starting Master + S1...")
docker_compose("up -d master secondary1")

assert wait_up(M) and wait_up(S1), "Master or S1 did not start"

# --- 3. Send initial messages ---
print(ts(), "2) Msg1 W=1 -> OK immediately")
send("Msg1", 1)

print(ts(), "3) Msg2 W=2 -> OK (S1 is up)")
send("Msg2", 2)

# --- 4. Send Msg3 W=3 in background ---
print(ts(), "4) Msg3 W=3 -> will block until S2 joins; run in background")
res_holder = {}
def send_bg():
    res_holder['r'] = send("Msg3", 3)

t = threading.Thread(target=send_bg, daemon=True)
t.start()

time.sleep(10)

print(ts(), "5) Msg4 W=1 -> OK immediately")
send("Msg4", 1)

# --- 5. Start S2 now ---
print(ts(), "Starting S2 container...")
docker_compose("up -d secondary2")
assert wait_up(S2), "S2 didn't start"

# --- 6. Poll until all messages appear on S2 ---
print(ts(), "Waiting for full replication on S2...")

expected_messages = ["Msg1", "Msg2", "Msg3", "Msg4"]
timeout = 30
start = time.time()

while time.time() - start < timeout:
    r = requests.get(S2 + "/messages")
    s2_messages = [m["message"] for m in r.json()]
    if s2_messages == expected_messages:
        break
    time.sleep(2)

print(ts(), "S2 messages:", s2_messages)

if s2_messages == expected_messages:
    print(ts(), "ACCEPTANCE TEST PASSED")
else:
    print(ts(), "ACCEPTANCE TEST FAILED", s2_messages)
