import os
import io
import json
import time
import random
from datetime import datetime, timezone

import boto3
import psycopg2
from kafka import KafkaConsumer
from PIL import Image, ImageDraw

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

S3_INPUT_BUCKET = os.environ["S3_INPUT_BUCKET"]
S3_OUTPUT_BUCKET = os.environ["S3_OUTPUT_BUCKET"]

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")

AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

s3 = boto3.client("s3", region_name=AWS_REGION)

def db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        sslmode=DB_SSLMODE,
    )

def ensure_table():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP,
                    image_key TEXT,
                    overlay_key TEXT,
                    status TEXT,
                    box JSONB
                )
            """)
        conn.commit()

def random_bbox(width, height):
    x1 = random.randint(0, max(0, width - 120))
    y1 = random.randint(0, max(0, height - 120))
    x2 = min(width, x1 + random.randint(60, 180))
    y2 = min(height, y1 + random.randint(60, 180))
    return {
        "x1": x1,
        "y1": y1,
        "x2": x2,
        "y2": y2,
        "label": "RFO_placeholder"
    }

def process_one(image_key):
    print(f"Starting processing for {image_key}", flush=True)
    print(f"Downloading s3://{S3_INPUT_BUCKET}/{image_key}", flush=True)

    obj = s3.get_object(Bucket=S3_INPUT_BUCKET, Key=image_key)
    image_bytes = obj["Body"].read()

    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    width, height = image.size

    box = random_bbox(width, height)

    draw = ImageDraw.Draw(image)
    draw.rectangle(
        [box["x1"], box["y1"], box["x2"], box["y2"]],
        outline="red",
        width=5
    )
    draw.text(
        (box["x1"], max(0, box["y1"] - 18)),
        box["label"],
        fill="red"
    )

    out = io.BytesIO()
    image.save(out, format="PNG")
    out.seek(0)

    overlay_key = f"overlays/{int(time.time())}_{os.path.basename(image_key)}"

    print(f"Uploading overlay to s3://{S3_OUTPUT_BUCKET}/{overlay_key}", flush=True)

    s3.put_object(
        Bucket=S3_OUTPUT_BUCKET,
        Key=overlay_key,
        Body=out.getvalue(),
        ContentType="image/png"
    )

    print("Inserting job row into PostgreSQL", flush=True)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO jobs (created_at, image_key, overlay_key, status, box)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                datetime.now(timezone.utc),
                image_key,
                overlay_key,
                "DONE",
                json.dumps(box)
            ))
        conn.commit()

    print(f"processed {image_key} -> {overlay_key} box={box}", flush=True)

def main():
    ensure_table()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SSL"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="rfo-worker-group",
    )

    print("worker started, waiting for messages...", flush=True)

    for msg in consumer:
        payload = msg.value
        print("Received message:", payload, flush=True)

        image_key = payload.get("image_key")
        if not image_key:
            print("Skipping message without image_key", flush=True)
            continue

        try:
            process_one(image_key)
        except Exception as e:
            print(f"ERROR processing {image_key}: {e}", flush=True)

if __name__ == "__main__":
    main()