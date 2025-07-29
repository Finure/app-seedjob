import os
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SECRET_PATH = "/etc/kafka/secrets"
KAFKA_CA_PATH = "/etc/kafka/ca"


def read_secret(path):
    with open(path, "r") as f:
        return f.read().strip()


KAFKA_KEY_PASSWORD = read_secret(
    os.path.join(KAFKA_SECRET_PATH, "user.password")
)
KAFKA_KEY = os.path.join(KAFKA_SECRET_PATH, "user.key")
KAFKA_CERT = os.path.join(KAFKA_SECRET_PATH, "user.crt")
KAFKA_CA = os.path.join(KAFKA_CA_PATH, "ca.crt")

admin_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SSL",
    "ssl.key.location": KAFKA_KEY,
    "ssl.certificate.location": KAFKA_CERT,
    "ssl.ca.location": KAFKA_CA,
    "ssl.key.password": KAFKA_KEY_PASSWORD,
}


def topic_exists(admin_client, topic_name):
    try:
        metadata = admin_client.list_topics(timeout=10)
        return topic_name in metadata.topics
    except Exception as e:
        print(f"Error checking topic: {e}")
        return False


def main():
    if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC:
        print("Missing KAFKA_BOOTSTRAP_SERVERS or KAFKA_TOPIC")
        exit(1)

    admin = AdminClient(admin_conf)

    if topic_exists(admin, KAFKA_TOPIC):
        print(f"Topic '{KAFKA_TOPIC}' exists")
        exit(0)
    else:
        print(f"Topic '{KAFKA_TOPIC}' does not exist")
        exit(1)


if __name__ == "__main__":
    main()
