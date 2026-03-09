import os
import json
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request

app = Flask(__name__)

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")

def db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        sslmode=DB_SSLMODE,
    )

@app.route("/recent", methods=["GET"])
def recent():
    n = int(request.args.get("n", 5))

    with db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT created_at, image_key, overlay_key, status, box
                FROM jobs
                ORDER BY created_at DESC
                LIMIT %s
            """, (n,))
            rows = cur.fetchall()

    cleaned_rows = []
    for row in rows:
        row = dict(row)
        if "box" in row and isinstance(row["box"], str):
            try:
                row["box"] = json.loads(row["box"])
            except Exception:
                pass
        cleaned_rows.append(row)

    return jsonify(cleaned_rows)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "recent-api","version","ci-cd-test-1"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)