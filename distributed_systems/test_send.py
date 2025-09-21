import threading
import time
import requests

url = "http://localhost:42000/messages"

def send(message):
    payload = {"message": message}
    resp = requests.post(url, json=payload)

threeads = [];
for i in range(1, 10):
    message = "message %s %s" % (str(i), str(time.time()))
    t = threading.Thread(target=send, args=(message,))
    t.start()
    threeads.append(t)


for t in threeads:
    t.join()