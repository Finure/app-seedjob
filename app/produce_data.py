import os
import json
import pandas as pd
from dotenv import load_dotenv
import kagglehub
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from confluent_kafka import Producer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SECRET_PATH = "/etc/kafka/secrets"
KAFKA_CA_PATH = "/etc/kafka/ca"

if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC:
    raise EnvironmentError(
        "KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set"
    )


def read_secret(path):
    with open(path, "r") as f:
        return f.read().strip()


KAFKA_KEY_PASSWORD = read_secret(
    os.path.join(KAFKA_SECRET_PATH, "user.password")
)
KAFKA_KEY = os.path.join(KAFKA_SECRET_PATH, "user.key")
KAFKA_CERT = os.path.join(KAFKA_SECRET_PATH, "user.crt")
KAFKA_CA = os.path.join(KAFKA_CA_PATH, "ca.crt")

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SSL",
    "ssl.key.location": KAFKA_KEY,
    "ssl.certificate.location": KAFKA_CERT,
    "ssl.ca.location": KAFKA_CA,
    "ssl.key.password": KAFKA_KEY_PASSWORD,
    "client.id": "finure",
    "queue.buffering.max.messages": int(
        os.getenv("KAFKA_MAX_MSGS", "1000000")
    ),
    "queue.buffering.max.kbytes": int(os.getenv("KAFKA_MAX_KB", "2097152")),
    "batch.num.messages": int(os.getenv("KAFKA_BATCH_NUM", "10000")),
    "batch.size": int(os.getenv("KAFKA_BATCH_SIZE", "1048576")),
    "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "500")),
    "compression.type": os.getenv("KAFKA_COMPRESSION", "lz4"),
}
producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for record {msg.key()}: {err}")


print("Downloading Kaggle dataset")
path = kagglehub.dataset_download("dotskilled/bank-credit-card-approval")

csv_file = next(
    (
        os.path.join(path, f)
        for f in os.listdir(path)
        if f.lower() == "credit_approval_data.csv"
    ),
    None,
)
if not csv_file:
    raise FileNotFoundError("credit_approval_data.csv not found")

df = pd.read_csv(csv_file)


df.columns = [c.strip() for c in df.columns]
rename_map = {
    "ID": "id",
    "Age": "age",
    "Income": "income",
    "Employed": "employed",
    "CreditScore": "credit_score",
    "LoanAmount": "loan_amount",
    "Approved": "approved",
}
df = df.rename(columns=rename_map)

required = ["id", "age", "income", "employed", "credit_score", "loan_amount", "approved"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"CSV missing required columns: {missing}")


def send_batch(batch_rows):
    success, fail = 0, 0
    for row in batch_rows:
        try:
            data = {
                "id": int(row["id"]),
                "age": int(row["age"]),
                "income": int(row["income"]),
                "employed": int(row["employed"]),
                "credit_score": int(row["credit_score"]),
                "loan_amount": int(row["loan_amount"]),
                "approved": int(row["approved"]),
            }

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(data["id"]),
                value=json.dumps(data),
                callback=delivery_report,
            )
            success += 1
        except Exception as e:
            print(f"Failed to send record {row['id']}: {e}")
            fail += 1

    producer.poll(0)
    return success, fail


print("Streaming records to Kafka topic:", KAFKA_TOPIC)
batch_size = 10000
futures = []
total_sent = 0
total_failed = 0

with ThreadPoolExecutor(max_workers=4) as executor:
    for i in tqdm(range(0, len(df), batch_size), desc="Submitting batches"):
        batch_df = df.iloc[i:i + batch_size]
        batch_rows = batch_df.to_dict(orient="records")
        futures.append(executor.submit(send_batch, batch_rows))

    for future in tqdm(
        as_completed(futures),
        total=len(futures),
        desc="Awaiting batch results",
    ):
        sent, failed = future.result()
        total_sent += sent
        total_failed += failed

producer.flush()
print("Kafka streaming completed!")
print(f"Total records processed: {len(df)}")
print(f"Records sent: {total_sent}")
print(f"Records failed: {total_failed}")
