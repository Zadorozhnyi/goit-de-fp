import json

from kafka import KafkaConsumer

from configs import kafka_config


if __name__ == "__main__":
    consumer = KafkaConsumer(
        kafka_config["output_topic"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="oza_final_project_consumer",
    )

    print(f"Subscribed to topic: {kafka_config['output_topic']}")

    try:
        for message in consumer:
            print(f"key={message.key} value={message.value}")
    finally:
        consumer.close()
