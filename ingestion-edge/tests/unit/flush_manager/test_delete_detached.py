from datetime import datetime, timedelta
from unittest.mock import MagicMock
from time import sleep

from kubernetes.client import (
    V1ObjectMeta,
    V1ObjectReference,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimList,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodStatus,
    V1PodCondition,
    V1PodList,
    V1PodSpec,
    V1Volume,
)
from kubernetes.client.rest import ApiException
import pytest

from ingestion_edge.flush_manager import PvcCacheEntry, delete_detached_pvcs


def test_delete_detached_pvcs(api: MagicMock):
    api.list_namespaced_pod.return_value = V1PodList(
        items=[
            # pvc is attached
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
            # pvc not attached because spec is missing
            V1Pod(),
            # pvc not attached because volumes are missing
            V1Pod(spec=V1PodSpec(containers=[],),),
            # pvc not attached because volume is not persistent
            V1Pod(spec=V1PodSpec(containers=[], volumes=[V1Volume(name="queue")]),),
            # pvc not attached because pod is unschedulable due to pvc
            V1Pod(
                metadata=V1ObjectMeta(
                    name="web-0",
                    namespace="default",
                    uid="uid-web-0",
                    resource_version="1",
                    owner_references=[V1ObjectReference(kind="StatefulSet")],
                ),
                status=V1PodStatus(
                    phase="Pending",
                    conditions=[
                        V1PodCondition(
                            status="Not Ready",
                            type="False",
                            reason="Unschedulable",
                            message='persistentvolumeclaim "queue-web-0" not found',
                        )
                    ],
                ),
            ),
        ]
    )
    api.list_namespaced_persistent_volume_claim.return_value = V1PersistentVolumeClaimList(
        items=[
            # should delete 0-2, 3 is in attached pvcs
            *(
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name=f"queue-web-{i}",
                        uid=f"uid-queue-web-{i}",
                        resource_version=f"{i}",
                    ),
                    spec=V1PersistentVolumeClaimSpec(volume_name=f"pv-{i}"),
                )
                for i in range(4)
            ),
            # name does not start with claim prefix
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name="other-web-0"),),
        ]
    )

    def delete_pvc(name, namespace, body):
        if name == "queue-web-1":
            raise ApiException(reason="Conflict")
        if name == "queue-web-2":
            raise ApiException(reason="Not Found")

    api.delete_namespaced_persistent_volume_claim.side_effect = delete_pvc
    pvc_cleanup_delay = timedelta(microseconds=1)
    delay_complete = datetime.utcnow() - pvc_cleanup_delay
    cache = {
        # wrong pv name, should be overwritten
        "queue-web-0": PvcCacheEntry(pv="wrong", time=delay_complete),
        # no longer detached, should be removed
        "queue-web-3": PvcCacheEntry(pv="pv-3", time=delay_complete),
    }

    delete_detached_pvcs(api, "namespace", "queue-", pvc_cleanup_delay, cache)

    api.list_namespaced_pod.assert_called_once_with("namespace")
    api.list_namespaced_persistent_volume_claim.assert_called_once_with("namespace")
    api.delete_namespaced_persistent_volume_claim.assert_not_called()
    assert {f"queue-web-{i}": f"pv-{i}" for i in range(3)} == {
        k: v.pv for k, v in cache.items()
    }
    api.list_namespaced_pod.reset_mock()
    api.list_namespaced_persistent_volume_claim.reset_mock()
    previous_cache = {**cache}

    delete_detached_pvcs(api, "namespace", "queue-", pvc_cleanup_delay, cache)

    api.list_namespaced_pod.assert_called_once_with("namespace")
    api.list_namespaced_persistent_volume_claim.assert_called_once_with("namespace")
    assert previous_cache == cache
    assert [
        (f"queue-web-{i}", "namespace", f"uid-queue-web-{i}", f"{i}") for i in range(3)
    ] == [
        (
            call.kwargs["name"],
            call.kwargs["namespace"],
            call.kwargs["body"].preconditions.uid,
            call.kwargs["body"].preconditions.resource_version,
        )
        for call in api.delete_namespaced_persistent_volume_claim.call_args_list
    ]


def test_delete_detached_pvcs_raises_server_error(api: MagicMock):
    api.list_namespaced_pod.return_value = V1PodList(items=[])
    api.list_namespaced_persistent_volume_claim.return_value = V1PersistentVolumeClaimList(
        items=[
            # should be deleted
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(
                    name="queue-web-0", uid="uid-queue-web-0", resource_version="1"
                ),
                spec=V1PersistentVolumeClaimSpec(volume_name="pv-0"),
            )
        ]
    )

    def delete_pvc(name, namespace, body):
        raise ApiException(reason="Server Error")

    api.delete_namespaced_persistent_volume_claim.side_effect = delete_pvc

    with pytest.raises(ApiException):
        delete_detached_pvcs(api, "namespace", "queue-", timedelta(microseconds=0), {})

    api.list_namespaced_pod.assert_called_once_with("namespace")
    api.list_namespaced_persistent_volume_claim.assert_called_once_with("namespace")
    assert [("queue-web-0", "namespace", "uid-queue-web-0", "1")] == [
        (
            call.kwargs["name"],
            call.kwargs["namespace"],
            call.kwargs["body"].preconditions.uid,
            call.kwargs["body"].preconditions.resource_version,
        )
        for call in api.delete_namespaced_persistent_volume_claim.call_args_list
    ]
