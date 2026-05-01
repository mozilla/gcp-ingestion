from datetime import datetime
from unittest.mock import MagicMock, call

from kubernetes.client import (
    V1Job,
    V1JobCondition,
    V1JobList,
    V1JobStatus,
    V1ObjectMeta,
    V1ObjectReference,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimList,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodList,
    V1PodSpec,
    V1Volume,
)
from kubernetes.client.rest import ApiException
import pytest

from ingestion_edge.flush_manager import (
    delete_complete_jobs_pvc_mode,
    flush_detached_pvcs,
    flush_detached_pvcs_and_delete_complete_jobs,
)


def test_flush_detached_pvcs(api: MagicMock, batch_api: MagicMock):
    api.list_namespaced_pod.return_value = V1PodList(
        items=[
            # queue-web-3 is attached, must not be flushed
            V1Pod(
                spec=V1PodSpec(
                    containers=[],
                    volumes=[
                        V1Volume(
                            name="queue",
                            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                                claim_name="queue-web-3",
                            ),
                        )
                    ],
                ),
            ),
        ]
    )
    api.list_namespaced_persistent_volume_claim.return_value = V1PersistentVolumeClaimList(  # noqa: E501
        items=[
            # flush because detached and no existing job
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="queue-web-0"),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-0"),
            ),
            # don't flush because existing job covers it
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="queue-web-1"),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-1"),
            ),
            # don't flush because pvc has a deletion timestamp
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(
                    name="queue-web-2",
                    deletion_timestamp=datetime.utcnow(),
                ),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-2"),
            ),
            # don't flush because attached to a pod
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="queue-web-3"),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-3"),
            ),
            # don't flush because name does not match prefix
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="other-web-0"),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-other"),
            ),
            # flush because detached and no existing job
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="queue-web-4"),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-4"),
            ),
        ]
    )
    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[V1Job(metadata=V1ObjectMeta(name="flush-queue-web-1"))]
    )

    flush_detached_pvcs(
        api,
        batch_api,
        ["command"],
        [],
        "image",
        "namespace",
        "service-account-name",
        "queue-",
    )

    api.list_namespaced_pod.assert_called_once_with("namespace")
    api.list_namespaced_persistent_volume_claim.assert_called_once_with("namespace")
    batch_api.list_namespaced_job.assert_called_once_with("namespace")
    # Must NOT touch cluster-scoped APIs.
    api.list_persistent_volume.assert_not_called()
    api.create_namespaced_persistent_volume_claim.assert_not_called()
    api.patch_persistent_volume.assert_not_called()
    # Created flush jobs map to the orphan PVCs, mounting them directly.
    assert [
        ("flush-queue-web-0", "queue-web-0"),
        ("flush-queue-web-4", "queue-web-4"),
    ] == [
        (
            call.kwargs["body"].metadata.name,
            call.kwargs["body"].spec.template.spec.volumes[0]
            .persistent_volume_claim.claim_name,
        )
        for call in batch_api.create_namespaced_job.call_args_list
    ]


def test_delete_complete_jobs_pvc_mode(api: MagicMock, batch_api: MagicMock):
    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[
            V1Job(
                metadata=V1ObjectMeta(
                    name=f"flush-queue-web-{i}",
                    uid=f"uid-flush-queue-web-{i}",
                    resource_version=f"{i}",
                ),
                status=V1JobStatus(
                    conditions=[
                        V1JobCondition(status="True", type="SuccessCriteriaMet"),
                        V1JobCondition(status="True", type="Complete"),
                    ]
                ),
            )
            for i in range(2)
        ]
        + [
            # not complete: skip
            V1Job(
                metadata=V1ObjectMeta(name="flush-queue-web-2"),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="", type="Pending")]
                ),
            ),
            # not a flush job: skip
            V1Job(
                metadata=V1ObjectMeta(name="some-other-job"),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="True", type="Complete")]
                ),
            ),
        ]
    )

    delete_complete_jobs_pvc_mode(api, batch_api, "namespace")

    batch_api.list_namespaced_job.assert_called_once_with("namespace")
    # Must not touch the cluster-scoped PV API.
    api.patch_persistent_volume.assert_not_called()
    # Each complete flush job's underlying PVC gets an owner_reference to the job.
    assert [
        ("queue-web-0", [("Job", "flush-queue-web-0")]),
        ("queue-web-1", [("Job", "flush-queue-web-1")]),
    ] == [
        (
            call.kwargs["name"],
            [
                (ref.kind, ref.name)
                for ref in call.kwargs["body"].metadata.owner_references
            ],
        )
        for call in api.patch_namespaced_persistent_volume_claim.call_args_list
    ]
    # Job deletes use foreground propagation so the PVC cascade-deletes too.
    assert all(
        call.kwargs["body"].propagation_policy == "Foreground"
        for call in batch_api.delete_namespaced_job.call_args_list
    )
    assert [
        f"flush-queue-web-{i}" for i in range(2)
    ] == [
        call.kwargs["name"] for call in batch_api.delete_namespaced_job.call_args_list
    ]


def test_delete_complete_jobs_pvc_mode_tolerates_already_gone(
    api: MagicMock, batch_api: MagicMock
):
    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[
            V1Job(
                metadata=V1ObjectMeta(
                    name="flush-queue-web-0",
                    uid="uid",
                    resource_version="1",
                ),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="True", type="Complete")]
                ),
            ),
        ]
    )

    def delete_job(name, namespace, body):
        raise ApiException(reason="Not Found")

    batch_api.delete_namespaced_job.side_effect = delete_job
    delete_complete_jobs_pvc_mode(api, batch_api, "namespace")


def test_delete_complete_jobs_pvc_mode_raises_server_error(
    api: MagicMock, batch_api: MagicMock
):
    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[
            V1Job(
                metadata=V1ObjectMeta(
                    name="flush-queue-web-0",
                    uid="uid",
                    resource_version="1",
                ),
                status=V1JobStatus(
                    conditions=[V1JobCondition(status="True", type="Complete")]
                ),
            ),
        ]
    )

    def delete_job(name, namespace, body):
        raise ApiException(reason="Server Error")

    batch_api.delete_namespaced_job.side_effect = delete_job
    with pytest.raises(ApiException):
        delete_complete_jobs_pvc_mode(api, batch_api, "namespace")


def test_flush_and_delete_pvc_mode(api: MagicMock, batch_api: MagicMock):
    flush_detached_pvcs_and_delete_complete_jobs(
        api,
        batch_api,
        ["command"],
        [],
        "image",
        "namespace",
        "service-account-name",
        "queue-",
    )
    api.list_persistent_volume.assert_not_called()
    api.list_namespaced_pod.assert_called_once_with("namespace")
    api.list_namespaced_persistent_volume_claim.assert_called_once_with("namespace")
    assert [
        call("namespace") for _ in range(2)
    ] == batch_api.list_namespaced_job.call_args_list
