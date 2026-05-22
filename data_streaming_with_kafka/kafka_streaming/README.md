# Kafka Streaming — Browser History Domain Statistics

A simple Kafka streaming application that:
1. Reads browser history from a CSV file and produces URL messages to a Kafka topic
2. Consumes those messages, extracts root domains , and prints the top 5 most visited

## Architecture

```
[history.csv] -> [producer.py] -> [Redpanda/Kafka] -> [consumer.py] -> Top 5 domains
```

## Prerequisites

- Docker & Docker Compose
- Python 3.10+

## How to Run

### 1. Start Redpanda

```bash
docker compose up -d
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Prepare your dataset

Export your browser history to a CSV file with a `url` column. A sample `history.csv` is included for testing.


### 4. Run the producer 

```bash
python producer.py history.csv
```

### 5. Run the consumer 

```bash
python consumer.py
```

The consumer will print the top 5 root domains by visit count.

### 6. Stop Redpanda

```bash
docker compose down
```

## Sample Output

```
Consuming messages...

=== Top 5 Root Domains ===
  .com: 22 visits
  .org: 5 visits
  .ua: 2 visits
  .io: 1 visits
  .us: 1 visits
```
