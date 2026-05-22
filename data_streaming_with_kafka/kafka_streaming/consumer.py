from collections import Counter
from urllib.parse import urlparse
from kafka import KafkaConsumer


# consumes urls from Kafka, counts visits per root domain
TOPIC = "browser-history"
BROKER = "localhost:9092"

def extract_root_domain(url: str) -> str | None:
    try:
        host = urlparse(url).hostname or ""
        parts = host.rsplit(".", 1)
        return parts[-1] if len(parts) >= 2 else None
    except Exception:
        return None


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=10000,
    )

    domain_counts = Counter()

    print("Consuming messages...")
    for msg in consumer:
        tld = extract_root_domain(msg.value)
        if tld:
            domain_counts[tld] += 1

    print("\n=== Top 5 Root Domains ===")
    for domain, count in domain_counts.most_common(5):
        print(f"  .{domain}: {count} visits")


if __name__ == "__main__":
    main()
