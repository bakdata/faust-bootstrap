from faust_bootstrap.core.streams.utils import exist_topic
from tests.integration.utils import create_admin_client_from_metadata, setup_cluster_with_topics, \
    delete_topic


def test_topic_exist(cluster_metadata):
    client_admin = create_admin_client_from_metadata(cluster_metadata)
    setup_cluster_with_topics(client_admin, ["test_topic_exist"])
    brokers = [f"kafka://{cluster_metadata['kafka']['host']}:{cluster_metadata['kafka']['port']}"]
    result = exist_topic(brokers, "test_topic_exist")
    delete_topic(client_admin, "test_topic_exist")
    assert result is True


def test_topic_not_exist(cluster_metadata):
    brokers = [f"kafka://{cluster_metadata['kafka']['host']}:{cluster_metadata['kafka']['port']}"]
    result = exist_topic(brokers, "test_topic_exist")
    assert result is False
