from dataclasses import dataclass
from ingestion_edge.dockerflow import check_queue_size
import pytest


@dataclass
class Queue:
    size: int = 0
    unack: int = 0

    def unack_count(self):
        return self.unack


@pytest.mark.parametrize("q", [None, Queue()])
def test_ok(q):
    assert check_queue_size(q) == []


def test_pending():
    status = check_queue_size(Queue(1))
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.I001"
    assert error.level == 20
    assert error.msg == "queue contains pending messages"


def test_unack():
    status = check_queue_size(Queue(unack=1))
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.I002"
    assert error.level == 20
    assert error.msg == "queue contains unacked messages"


def test_error():
    status = check_queue_size(False)
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.E002"
    assert error.level == 40
    assert error.msg == "queue raised exception on access"
