from datetime import datetime
from unittest.mock import MagicMock, call

from kubernetes.client import (
    V1Job,
    V1JobList,
    V1ObjectMeta,
    V1ObjectReference,
    V1PersistentVolume,
    V1PersistentVolumeStatus,
    V1PersistentVolumeList,
    V1PersistentVolumeClaim,
    V1PersistentVolumeSpec,
)
from kubernetes.client.rest import ApiException
import pytest

from ingestion_edge.flush_manager import flush_released_pvs


def test_flush_released_pvs(api: MagicMock, batch_api: MagicMock):
    api.list_persistent_volume.return_value = V1PersistentVolumeList(
        items=[
            # don't flush because job exists
            V1PersistentVolume(metadata=V1ObjectMeta(name="pv-0")),
            # don't flush because wrong namespace
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-4"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(namespace="other"),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Released"),
            ),
            # don't flush because it's already done
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-5"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(namespace="namespace"),
                    persistent_volume_reclaim_policy="Delete",
                ),
                status=V1PersistentVolumeStatus(phase="Released"),
            ),
            # don't flush because it's in use
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-6"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="queue-web-0", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Bound"),
            ),
            # try to flush because pvc is bound but job was created after jobs were listed
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-7"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="flush-pv-7", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Bound"),
            ),
            # flush because pvc is bound but job does not exist
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-8"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="flush-pv-8", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Bound"),
            ),
            # flush because pvc is not yet bound and job does not exist
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-9"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="queue-web-0", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Released"),
            ),
            # flush because pvc and job both do not exist
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-A"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="queue-web-0", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Released"),
            ),
        ]
    )

    def create_pvc(namespace: str, body: V1PersistentVolumeClaim):
        if body.metadata.name == "flush-pv-9":
            exc = ApiException(status=409, reason="Conflict")
            exc.body = '{"reason":"AlreadyExists"}'
            raise exc
        body.metadata.uid = "uid-" + body.metadata.name
        body.metadata.resource_version = "1"
        return body

    api.create_namespaced_persistent_volume_claim.side_effect = create_pvc

    def read_pvc(name: str, namespace: str):
        return V1PersistentVolumeClaim(
            metadata=V1ObjectMeta(
                name=name, namespace=namespace, uid="uid-" + name, resource_version="2"
            )
        )

    api.read_namespaced_persistent_volume_claim.side_effect = read_pvc

    batch_api.list_namespaced_job.return_value = V1JobList(
        items=[V1Job(metadata=V1ObjectMeta(name="flush-pv-0"))]
    )

    def create_job(namespace, body):
        if body.metadata.name == "flush-pv-7":
            exc = ApiException(status=409, reason="Conflict")
            exc.body = '{"reason":"AlreadyExists"}'
            raise exc

    batch_api.create_namespaced_job.side_effect = create_job

    flush_released_pvs(api, batch_api, "command", "env", "image", "namespace")

    api.list_persistent_volume.assert_called_once_with()
    batch_api.list_namespaced_job.assert_called_once_with("namespace")
    assert [f"flush-pv-{i}" for i in "9A"] == [
        call.kwargs["body"].metadata.name
        for call in api.create_namespaced_persistent_volume_claim.call_args_list
    ]
    api.read_namespaced_persistent_volume_claim.assert_called_once_with(
        "flush-pv-9", "namespace"
    )
    assert [("pv-9", "flush-pv-9"), ("pv-A", "flush-pv-A"),] == [
        (
            call.kwargs["name"],
            call.kwargs["body"].spec.claim_ref
            and call.kwargs["body"].spec.claim_ref.name,
        )
        for call in api.patch_persistent_volume.call_args_list
    ]
    assert [f"flush-pv-{i}" for i in "789A"] == [
        call.kwargs["body"].metadata.name
        for call in batch_api.create_namespaced_job.call_args_list
    ]
    batch_api.read_namespaced_job.assert_called_once_with("flush-pv-7", "namespace")


def test_create_flush_job_raises_server_error(api: MagicMock, batch_api: MagicMock):
    api.list_persistent_volume.return_value = V1PersistentVolumeList(
        items=[
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-0"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="queue-web-0", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Released"),
            ),
        ]
    )

    def create_job(namespace, body):
        raise ApiException(reason="Server Error")

    batch_api.create_namespaced_job.side_effect = create_job

    with pytest.raises(ApiException):
        flush_released_pvs(api, batch_api, "command", "env", "image", "namespace")

    api.list_persistent_volume.assert_called_once_with()
    batch_api.list_namespaced_job.called_once_with("namespace")


def test_create_pvc_raises_server_error(api: MagicMock, batch_api: MagicMock):
    api.list_persistent_volume.return_value = V1PersistentVolumeList(
        items=[
            V1PersistentVolume(
                metadata=V1ObjectMeta(name="pv-0"),
                spec=V1PersistentVolumeSpec(
                    claim_ref=V1ObjectReference(
                        name="queue-web-0", namespace="namespace"
                    ),
                    persistent_volume_reclaim_policy="Retain",
                ),
                status=V1PersistentVolumeStatus(phase="Released"),
            ),
        ]
    )

    def create_pvc(namespace, body):
        raise ApiException(reason="Server Error")

    api.create_namespaced_persistent_volume_claim.side_effect = create_pvc

    with pytest.raises(ApiException):
        flush_released_pvs(api, batch_api, "command", "env", "image", "namespace")

    api.list_persistent_volume.assert_called_once_with()
    batch_api.list_namespaced_job.called_once_with("namespace")
