FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY raft_node.py node.py /app/

CMD ["python", "node.py"]
