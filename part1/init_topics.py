from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import kafka_config


def create_topic_if_needed(client: KafkaAdminClient, name: str) -> None:
    topic = NewTopic(name=name, num_partitions=2, replication_factor=1)
    try:
        client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{name}' created")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists")


if __name__ == "__main__":
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
    )

    try:
        create_topic_if_needed(admin_client, kafka_config["input_topic"])
        create_topic_if_needed(admin_client, kafka_config["output_topic"])
        print("Current oza topics:")
        for topic_name in sorted(admin_client.list_topics()):
            if topic_name.startswith("oza_"):
                print(topic_name)
    finally:
        admin_client.close()
