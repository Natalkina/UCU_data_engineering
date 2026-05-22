import csv
import time
import sys
from kafka import KafkaProducer

#  reads browser history csv and sends urls to Kafka


TOPIC = "browser-history"
BROKER = "localhost:9092"


def main():
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "history.csv"

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            url = row.get("url") or row.get("URL") or ""
            if url:
                producer.send(TOPIC, value=url)
                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} messages...")
                time.sleep(0.01)

    producer.flush()
    print(f"Done. Sent {count} messages total.")


if __name__ == "__main__":
    main()
