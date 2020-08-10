from datetime import datetime
from unittest.mock import MagicMock, call

from kubernetes.client import (
    V1Job,
    V1JobCondition,
    V1JobStatus,
    V1JobList,
    V1ObjectMeta,
)
from kubernetes.client.rest import ApiException
import pytest

from ingestion_edge.flush_manager import delete_complete_jobs


def test_delete_complete_jobs(api: MagicMock, batch_api: MagicMock):
    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[
            # delete because complete
            *(
                V1Job(
                    metadata=V1ObjectMeta(
                        name=f"flush-pv-{i}",
                        uid=f"uid-flush-pv-{i}",
                        resource_version=f"{i}",
                    ),
                    status=V1JobStatus(
                        conditions=[V1JobCondition(status="", type="Complete")]
                    ),
                )
                for i in range(3)
            ),
            # don't delete because already deleted
            V1Job(
                metadata=V1ObjectMeta(
                    name="flush-pv-3", deletion_timestamp=datetime.utcnow(),
                ),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="", type="Complete")]
                ),
            ),
            # don't delete because not complete
            V1Job(
                metadata=V1ObjectMeta(name="flush-pv-4"),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="", type="Pending")]
                ),
            ),
            # don't delete because status does not contain conditions
            V1Job(metadata=V1ObjectMeta(name="flush-pv-5"), status=V1JobStatus()),
            # don't delete because not a flush job
            V1Job(
                metadata=V1ObjectMeta(name="cron-1"),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="", type="Complete")]
                ),
            ),
        ]
    )

    def delete_job(name, namespace, body):
        if name == "flush-pv-1":
            raise ApiException(reason="Conflict")
        if name == "flush-pv-2":
            raise ApiException(reason="Not Found")

    batch_api.delete_namespaced_job.side_effect = delete_job

    delete_complete_jobs(api, batch_api, "namespace")

    batch_api.list_namespaced_job.assert_called_once_with("namespace")
    assert [(f"pv-{i}", "Delete") for i in range(3)] == [
        (
            call.kwargs["name"],
            call.kwargs["body"].spec.persistent_volume_reclaim_policy,
        )
        for call in api.patch_persistent_volume.call_args_list
    ]
    assert [
        (f"flush-pv-{i}", "namespace", [("Job", f"flush-pv-{i}")]) for i in range(3)
    ] == [
        (
            call.kwargs["name"],
            call.kwargs["namespace"],
            [
                (ref.kind, ref.name)
                for ref in call.kwargs["body"].metadata.owner_references
            ],
        )
        for call in api.patch_namespaced_persistent_volume_claim.call_args_list
    ]
    assert [
        (f"flush-pv-{i}", "namespace", f"uid-flush-pv-{i}", f"{i}") for i in range(3)
    ] == [
        (
            call.kwargs["name"],
            call.kwargs["namespace"],
            call.kwargs["body"].preconditions.uid,
            call.kwargs["body"].preconditions.resource_version,
        )
        for call in batch_api.delete_namespaced_job.call_args_list
    ]


def test_delete_complete_jobs_raises_server_error(api: MagicMock, batch_api: MagicMock):
    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[
            # delete because complete
            V1Job(
                metadata=V1ObjectMeta(
                    name="flush-pv-1", uid="uid-flush-pv-1", resource_version="1"
                ),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="", type="Complete")]
                ),
            ),
        ]
    )

    def delete_job(name, namespace, body):
        raise ApiException(reason="Server Error")

    batch_api.delete_namespaced_job.side_effect = delete_job

    with pytest.raises(ApiException):
        delete_complete_jobs(api, batch_api, "namespace")

    batch_api.list_namespaced_job.called_once_with("namespace")
