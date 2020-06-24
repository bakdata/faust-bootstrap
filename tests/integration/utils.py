from typing import Dict
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic


def generate_dummy_messages(cluster_metadata: Dict, amout_of_messages: int, topic: str):
    ip = cluster_metadata["kafka"]["host"]
    port = cluster_metadata["kafka"]["port"]
    conf_producer = {
        "bootstrap.servers": f"{ip}:{port}"
    }
    producer = Producer(conf_producer)
    for x in range(amout_of_messages):
        producer.poll(0)
        producer.produce(topic, f"{x}".encode("utf-8"), )
        producer.flush(1)


def create_admin_client_from_metadata(metadata):
    ip = metadata["kafka"]["host"]
    port = metadata["kafka"]["port"]
    conf = {"bootstrap.servers": f"{ip}:{port}"}
    client_admin = AdminClient(conf)
    return client_admin

def setup_cluster_with_topics(client_admin, topics):
    topics_metadata = get_topics_metadata(client_admin)
    assert len(topics_metadata) == 2 or len(topics_metadata) == 3, \
        "You should have 2 topics in the kafka cluster because of the image configuration!"
    for topic in topics:
        create_new_topic(topic, client_admin)
        time.sleep(10)
    topics_metadata_after_topics = get_topics_metadata(client_admin)
    assert len(topics_metadata_after_topics) == len(topics_metadata) + len(topics), \
        "You should have the new topics you created!"


def delete_topic(client_admin: AdminClient, topic: str):
    fs = client_admin.delete_topics([topic])
    for topic, f in fs.items():
        f.result()
        time.sleep(2)
    time.sleep(20)


def get_topics_metadata(client: AdminClient) -> Dict:
    return client.list_topics().topics


def create_new_topic(topic_name: str, client: AdminClient):
    dummy_topic = NewTopic(topic_name, 1, replication_factor=1)
    topic_structure = client.create_topics([dummy_topic])
    for topic, future in topic_structure.items():
        future.result()
