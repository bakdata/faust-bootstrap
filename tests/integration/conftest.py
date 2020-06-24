import pytest
import os

import requests
from pytest_docker.plugin import get_docker_ip

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "tests", "integration", "docker-compose.yml")


@pytest.fixture(scope="session")
def ip_docker(docker_ip):
    os.environ["IPDOCKER"] = docker_ip
    return docker_ip


@pytest.fixture(scope="session")
def cluster_metadata(ip_docker, docker_services):
    """Ensure that HTTP service is up and responsive."""

    # `port_for` takes a container port and returns the corresponding host port

    url = "http://{}:{}".format(ip_docker, 8081)
    docker_services.wait_until_responsive(
        timeout=300, pause=20, check=lambda: is_responsive(url)
    )

    cluster_metadata = {
        "kafka": {
            "host": ip_docker,
            "port": "9094"
        },
        "schema_registry": {
            "host": ip_docker,
            "port": "8081"
        }
    }

    return cluster_metadata


def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "tests", "integration", "docker-compose.yml")


@pytest.fixture(scope="session")
def docker_ip():
    """Determine the IP address for TCP connections to Docker containers."""

    return get_docker_ip()
