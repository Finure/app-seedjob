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
path = kagglehub.dataset_download("rikdifos/credit-card-approval-prediction")

csv_file = next(
    (
        os.path.join(path, f)
        for f in os.listdir(path)
        if f.lower() == "application_record.csv"
    ),
    None,
)
if not csv_file:
    raise FileNotFoundError("application_record.csv not found")

df = pd.read_csv(csv_file)


def send_batch(batch_rows):
    success, fail = 0, 0
    for row in batch_rows:
        try:
            data = {
                "id": int(row["ID"]),
                "code_gender": row["CODE_GENDER"],
                "flag_own_car": row["FLAG_OWN_CAR"],
                "flag_own_realty": row["FLAG_OWN_REALTY"],
                "cnt_children": int(row["CNT_CHILDREN"]),
                "amt_income_total": float(row["AMT_INCOME_TOTAL"]),
                "name_income_type": row["NAME_INCOME_TYPE"],
                "name_education_type": row["NAME_EDUCATION_TYPE"],
                "name_family_status": row["NAME_FAMILY_STATUS"],
                "name_housing_type": row["NAME_HOUSING_TYPE"],
                "days_birth": int(row["DAYS_BIRTH"]),
                "days_employed": int(row["DAYS_EMPLOYED"]),
                "flag_mobil": int(row["FLAG_MOBIL"]),
                "flag_work_phone": int(row["FLAG_WORK_PHONE"]),
                "flag_phone": int(row["FLAG_PHONE"]),
                "flag_email": int(row["FLAG_EMAIL"]),
                "occupation_type": (
                    row["OCCUPATION_TYPE"]
                    if pd.notnull(row["OCCUPATION_TYPE"])
                    else None
                ),
                "cnt_fam_members": float(row["CNT_FAM_MEMBERS"]),
            }

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(data["id"]),
                value=json.dumps(data),
                callback=delivery_report,
            )
            success += 1
        except Exception as e:
            print(f"Failed to send record {row['ID']}: {e}")
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
