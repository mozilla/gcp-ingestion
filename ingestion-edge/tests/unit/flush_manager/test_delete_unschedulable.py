from unittest.mock import MagicMock

from kubernetes.client import (
    V1ObjectMeta,
    V1ObjectReference,
    V1Pod,
    V1PodStatus,
    V1PodCondition,
    V1PodList,
)
from kubernetes.client.rest import ApiException
import pytest

from ingestion_edge.flush_manager import delete_unschedulable_pods


def test_delete_unschedulable_pods(api: MagicMock):
    api.list_namespaced_pod.return_value = V1PodList(
        items=[
            V1Pod(
                metadata=V1ObjectMeta(
                    name=f"web-{i}",
                    namespace="default",
                    uid=f"uid-web-{i}",
                    resource_version=f"{i}",
                    owner_references=[V1ObjectReference(kind="StatefulSet")],
                ),
                status=V1PodStatus(
                    phase="Pending",
                    conditions=[
                        V1PodCondition(
                            status="Not Ready",
                            type="False",
                            reason="Unschedulable",
                            # 0-2 should be deleted, 3 should have the wrong message
                            message=""
                            if i == 3
                            else f'persistentvolumeclaim "queue-web-{i}" not found',
                        )
                    ],
                ),
            )
            for i in range(4)
        ]
    )

    def delete_pod(name, namespace, body):
        if name == "web-1":
            raise ApiException(reason="Conflict")
        if name == "web-2":
            raise ApiException(reason="Not Found")

    api.delete_namespaced_pod.side_effect = delete_pod
    delete_unschedulable_pods(api, "namespace")
    assert [(f"web-{i}", "namespace", f"uid-web-{i}", f"{i}") for i in range(3)] == [
        (
            call.kwargs["name"],
            call.kwargs["namespace"],
            call.kwargs["body"].preconditions.uid,
            call.kwargs["body"].preconditions.resource_version,
        )
        for call in api.delete_namespaced_pod.call_args_list
    ]


def test_delete_unschedulable_pods_raises_server_error(api: MagicMock):
    api.list_namespaced_pod.return_value = V1PodList(
        items=[
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

    def delete_pod(name, namespace, body):
        raise ApiException(reason="Server Error")

    api.delete_namespaced_pod.side_effect = delete_pod

    with pytest.raises(ApiException):
        delete_unschedulable_pods(api, "namespace")

    api.list_namespaced_pod.called_once_with("namespace")
