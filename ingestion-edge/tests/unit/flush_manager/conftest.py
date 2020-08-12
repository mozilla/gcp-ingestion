from unittest.mock import MagicMock

from kubernetes.client import CoreV1Api, BatchV1Api
import pytest


@pytest.fixture
def api():
    return MagicMock(spec=CoreV1Api)


@pytest.fixture
def batch_api():
    return MagicMock(spec=BatchV1Api)
