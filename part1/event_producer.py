import json
import os
import uuid

import pymysql
from kafka import KafkaProducer
from pymysql.cursors import SSCursor

from configs import jdbc_config, kafka_config


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=True).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )


if __name__ == "__main__":
    max_rows = int(os.getenv("PRODUCER_MAX_ROWS", "20000"))

    # Stage 3: read competition results from MySQL using a streaming cursor.
    conn = pymysql.connect(
        host=jdbc_config["host"],
        user=jdbc_config["jdbc_user"],
        password=jdbc_config["jdbc_password"],
        database=jdbc_config["db"],
        port=jdbc_config["port"],
        charset="utf8mb4",
        cursorclass=SSCursor,
    )

    producer = build_producer()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT edition, edition_id, country_noc, sport, event, result_id, athlete, athlete_id, pos, medal, isTeamSport "
                f"FROM {jdbc_config['results_table']}"
            )

            sent = 0
            # Stage 3: write competition results to the input Kafka topic.
            for row in cursor:
                payload = {
                    "edition": row[0],
                    "edition_id": row[1],
                    "country_noc": row[2],
                    "sport": row[3],
                    "event": row[4],
                    "result_id": row[5],
                    "athlete": row[6],
                    "athlete_id": row[7],
                    "pos": row[8],
                    "medal": row[9],
                    "isTeamSport": row[10],
                }
                producer.send(
                    kafka_config["input_topic"],
                    key=str(uuid.uuid4()),
                    value=payload,
                )
                sent += 1

                if sent % 500 == 0:
                    producer.flush()
                    print(f"Sent {sent} rows")

                if sent >= max_rows:
                    break

            producer.flush()
            print(f"Producer finished. Total sent rows: {sent}")
    finally:
        producer.close()
        conn.close()
