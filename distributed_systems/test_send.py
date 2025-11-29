import threading
import time
import requests

url = "http://localhost:42000/messages"

def send(message):
    payload = {"message": message, "write_concern": 2}
    resp = requests.post(url, json=payload)

threeads = [];
for i in range(0, 100):
    message = "message %s %s" % (str(i), str(time.time()))
    print(message)
    send(message)
    # t = threading.Thread(target=send, args=(message,))
    # t.start()
    # threeads.append(t)


for t in threeads:
    t.join()