import requests
import time
import subprocess
import threading
from datetime import datetime

# ------------------------
# Configuration
# ------------------------
M = "http://localhost:42000"
S1 = "http://localhost:42001"
S2 = "http://localhost:42002"

# keys for health checking
S1_KEY = "http://secondary1:42001"
S2_KEY = "http://secondary2:42002"

REPLICATION_TIMEOUT = 30  # max waiting for master POST

# ------------------------
# Helpers
# ------------------------
def ts():
    return datetime.now().strftime("[%H:%M:%S]")

def dc(cmd):
    print(ts(), "docker-compose", cmd)
    result = subprocess.run(
        ["docker-compose"] + cmd.split(), capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
    return result.returncode == 0

def wait_http_ok(url, endpoint="/messages", timeout=25):
    print(ts(), f"Waiting for {url}{endpoint} ...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url + endpoint, timeout=1)
            if r.status_code == 200:
                print(ts(), f"{url} is UP")
                return True
        except:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"{url} did not start")

def master_health():
    try:
        r = requests.get(M + "/health", timeout=1)
        return r.json()
    except:
        return {"status": "down"}

def send(msg, w):
    print(ts(), f"SEND {msg} (W={w}) → Master")
    try:
        r = requests.post(
            M + "/messages",
            json={"message": msg, "write_concern": w},
            timeout=REPLICATION_TIMEOUT
        )
        print(ts(), "↳", r.status_code, r.text)
        return r
    except Exception as e:
        print(ts(), "Exception:", e)
        return None

# ===========================================================
# BEGIN TEST
# ===========================================================
print(ts(), "Stopping all containers…")
dc("down")

print(ts(), "\n=== Start Master + S1 ===")
dc("up -d master secondary1")
wait_http_ok(M)
wait_http_ok(S1)

# Wait a bit to let Master detect S2 as Unhealthy (S2 not started yet)
print("[*] Waiting for Master to detect S2 as NOT Healthy...")
deadline = time.time() + 30
became_not_healthy = False

while time.time() < deadline:
    h = master_health()
    s2_status = h["secondaries"].get(S2)

    # If master returns no status OR any status != Healthy, we treat it as Unhealthy
    if s2_status != "Healthy":
        became_not_healthy = True
        print(f"[*] Master detected S2 as {s2_status}")
        break

    time.sleep(1)

if not became_not_healthy:
    print("Warning: S2 did not become NOT Healthy in time, continuing...")

# -----------------------------------------------------------
print(ts(), "\n--- Send Msg1 W=1 (must be OK immediately)")
r1 = send("Msg1", 1)
assert r1 and r1.status_code == 201

print(ts(), "\n--- Send Msg2 W=2 (S1 alive → must be OK)")
r2 = send("Msg2", 2)
assert r2 and r2.status_code == 201

print(ts(), "\n--- Send Msg3 W=3 (must BLOCK — no S2 yet)")
res_holder = {}
def send_bg():
    res_holder["res"] = send("Msg3", 3)
t = threading.Thread(target=send_bg, daemon=True)
t.start()

time.sleep(25)
print(ts(), "Master health while waiting for S2:", master_health())

print(ts(), "\n--- Send Msg4 W=1 (must be OK immediately)")
r4 = send("Msg4", 1)
assert r4 and r4.status_code == 201

# ===========================================================
# START S2
# ===========================================================
print(ts(), "\n=== Starting S2 ===")
dc("up -d secondary2")
wait_http_ok(S2)

# Wait for heartbeat to detect S2 as Healthy
print(ts(), "Waiting for Master to mark S2 as Healthy...")
hb_start = time.time()
while time.time() - hb_start < 20:
    h = master_health()
    if h.get("secondaries", {}).get(S2_KEY) == "Healthy":
        print(ts(), "S2 detected as Healthy")
        break
    time.sleep(1)
else:
    print(ts(), "Warning: S2 did not become Healthy in time")

print(ts(), "Master health now:", master_health())

# Wait until Msg3 finishes
print(ts(), "Waiting for Msg3 W=3 POST to finish...")
t.join(timeout=20)
print(ts(), "Msg3 result:", res_holder.get("res"))

# ===========================================================
# VALIDATE REPLICATION ON S2
# ===========================================================
print(ts(), "\nValidating messages on S2…")
expected = ["Msg1", "Msg2", "Msg3", "Msg4"]

replication_start = time.time()
s2_msgs = []
while time.time() - replication_start < 40:
    try:
        r = requests.get(S2 + "/messages")
        s2_msgs = [m["message"] for m in r.json()]
        print(ts(), "S2 →", s2_msgs)
        if s2_msgs == expected:
            break
    except:
        pass
    time.sleep(2)

print(ts(), "\n=== FINAL RESULT ===")
print("Messages on S2:", s2_msgs)

if s2_msgs == expected:
    print("\nACCEPTANCE TEST PASSED")
else:
    print("\nACCEPTANCE TEST FAILED")
