import re
from unittest import mock

import pytest

from parsl.providers.kubernetes.kube import KubernetesProvider
from parsl.tests.test_utils.test_sanitize_dns import DNS_SUBDOMAIN_REGEX

_MOCK_BASE = "parsl.providers.kubernetes.kube"


@pytest.fixture(autouse=True)
def mock_kube_config():
    with mock.patch(f"{_MOCK_BASE}.config") as mock_config:
        mock_config.load_kube_config.return_value = None
        yield mock_config


@pytest.fixture
def mock_kube_client():
    mock_client = mock.MagicMock()
    with mock.patch(f"{_MOCK_BASE}.client.CoreV1Api") as mock_api:
        mock_api.return_value = mock_client
        yield mock_client


@pytest.mark.local
def test_submit_happy_path(mock_kube_client: mock.MagicMock):
    image = "test-image"
    namespace = "test-namespace"
    cmd_string = "test-command"
    volumes = [("test-volume", "test-mount-path")]
    service_account_name = "test-service-account"
    annotations = {"test-annotation": "test-value"}
    max_cpu = 2
    max_mem = "2Gi"
    init_cpu = 1
    init_mem = "1Gi"
    provider = KubernetesProvider(
        image=image,
        persistent_volumes=volumes,
        namespace=namespace,
        service_account_name=service_account_name,
        annotations=annotations,
        max_cpu=max_cpu,
        max_mem=max_mem,
        init_cpu=init_cpu,
        init_mem=init_mem,
    )

    job_name = "test.job.name"
    job_id = provider.submit(cmd_string=cmd_string, tasks_per_node=1, job_name=job_name)

    assert job_id in provider.resources
    assert mock_kube_client.create_namespaced_pod.call_count == 1

    call_args = mock_kube_client.create_namespaced_pod.call_args[1]
    pod = call_args["body"]
    container = pod.spec.containers[0]
    volume = container.volume_mounts[0]

    assert image == container.image
    assert namespace == call_args["namespace"]
    assert any(cmd_string in arg for arg in container.args)
    assert volumes[0] == (volume.name, volume.mount_path)
    assert service_account_name == pod.spec.service_account_name
    assert annotations == pod.metadata.annotations
    assert str(max_cpu) == container.resources.limits["cpu"]
    assert max_mem == container.resources.limits["memory"]
    assert str(init_cpu) == container.resources.requests["cpu"]
    assert init_mem == container.resources.requests["memory"]
    assert job_id == pod.metadata.labels["parsl-job-id"]
    assert job_id == container.name
    assert f"{job_name}.{job_id}" == pod.metadata.name


@pytest.mark.local
@mock.patch(f"{_MOCK_BASE}.KubernetesProvider._create_pod")
@pytest.mark.parametrize("char", (".", "-"))
def test_submit_pod_name_includes_job_id(mock_create_pod: mock.MagicMock, char: str):
    provider = KubernetesProvider(image="test-image")

    job_name = "a." * 121 + f"a{char}" + "a" * 9
    assert len(job_name) == 253  # Max length for pod name
    job_id = provider.submit(cmd_string="test-command", tasks_per_node=1, job_name=job_name)

    expected_pod_name = job_name[:253 - len(job_id) - 2] + f".{job_id}"
    actual_pod_name = mock_create_pod.call_args[1]["pod_name"]
    assert re.match(DNS_SUBDOMAIN_REGEX, actual_pod_name)
    assert expected_pod_name == actual_pod_name


@pytest.mark.local
@mock.patch(f"{_MOCK_BASE}.KubernetesProvider._create_pod")
@mock.patch(f"{_MOCK_BASE}.logger")
@pytest.mark.parametrize("job_name", ("", ".", "-", "a.-.a", "$$$"))
def test_submit_invalid_job_name(mock_logger: mock.MagicMock, mock_create_pod: mock.MagicMock, job_name: str):
    provider = KubernetesProvider(image="test-image")
    job_id = provider.submit(cmd_string="test-command", tasks_per_node=1, job_name=job_name)
    assert mock_logger.warning.call_count == 1
    assert f"Invalid pod name '{job_name}' for job '{job_id}'" in mock_logger.warning.call_args[0][0]
    assert f"parsl.kube.{job_id}" == mock_create_pod.call_args[1]["pod_name"]
