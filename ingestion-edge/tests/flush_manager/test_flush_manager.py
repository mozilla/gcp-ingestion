from typing import Any, Dict
from uuid import uuid4
import os
import time

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import SubscriberClient
from kubernetes.client.rest import ApiException
from kubernetes.client import (
    AppsV1Api,
    BatchV1Api,
    CoreV1Api,
    V1DeleteOptions,
    V1Preconditions,
    V1StatefulSet,
)
import pytest
import requests

from .conftest import kube_methods, kube_resource


def list_pods(api: CoreV1Api):
    return api.list_namespaced_pod("default").items


def list_pvcs(api: CoreV1Api):
    return api.list_namespaced_persistent_volume_claim("default").items


def create_static_pvs(api: CoreV1Api):
    created = False
    for pvc in list_pvcs(api):
        if pvc.status.phase != "Pending" or not pvc.metadata.name.startswith("queue-"):
            continue
        print("creating pv")
        kube_methods("kube/web.pv.yml", name="pv-" + uuid4().hex, pvc=pvc)["create"]()
        created = True
    if created:
        while any(pod.status.phase == "Pending" for pod in list_pods(api)):
            pass


def delete_pvcs(api: CoreV1Api):
    for pvc in list_pvcs(api):
        api.delete_namespaced_persistent_volume_claim(
            pvc.metadata.name,
            pvc.metadata.namespace,
            body=V1DeleteOptions(
                grace_period_seconds=0,
                propagation_policy="Background",
                preconditions=V1Preconditions(
                    resource_version=pvc.metadata.resource_version,
                    uid=pvc.metadata.uid,
                ),
            ),
        )


def restart_web_pods(api: CoreV1Api):
    for pod in api.list_namespaced_pod("default").items:
        if pod.metadata.labels.get("app") != "web":
            continue
        api.delete_namespaced_pod(
            pod.metadata.name,
            pod.metadata.namespace,
            body=V1DeleteOptions(
                grace_period_seconds=0,
                propagation_policy="Background",
                preconditions=V1Preconditions(
                    resource_version=pod.metadata.resource_version,
                    uid=pod.metadata.uid,
                ),
            ),
        )


def test_flush_manager(options: Dict[str, Any], emulator: str, web: str):
    print("starting test")
    api = CoreV1Api()
    # max number of loops to run when waiting for kube actions to complete
    max_wait_loops = 20 if options["cluster"] is None else 60

    # server has invalid PUBSUB_EMULATOR, so that only flush can deliver messages
    static_pvs = options["cluster"] is None
    if static_pvs:
        create_static_pvs(api)

    print("waiting for pods to be healthy")
    for _ in range(max_wait_loops):
        if all(
            pod.status.phase == "Running"
            for pod in api.list_namespaced_pod("default").items
        ):
            break
        time.sleep(1)
    else:
        assert False, "pods did not become healthy"

    # create a subscription to the defined topic
    print("creating pubsub subscription")
    os.environ["PUBSUB_EMULATOR_HOST"] = emulator
    sub_client = SubscriberClient()
    topic_path = "projects/{project}/topics/{topic}".format(**options)
    subscription_path = "projects/{project}/subscriptions/{topic}".format(**options)
    try:
        sub_client.create_subscription(subscription_path, topic_path, retry=None)
    except AlreadyExists:
        pass

    print("posting message 0")
    requests.post(web, headers={"host": "web"}, json={"id": 0}).raise_for_status()
    print("setting up race condition: attached pvc is also deleted")
    delete_pvcs(api)
    print("setting up race condition: pod unschedulable due to missing pvc")
    with pytest.raises(ApiException) as excinfo:
        restart_web_pods(api)
    assert excinfo.value.reason == "Conflict"
    print("posting message 1")
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.post(web, headers={"host": "web"}, json={"id": 1}).raise_for_status()

    print("starting flush-manager")
    # TODO optionally run flush-manager via subprocess.Popen, to ensure testing
    # current code and enable code coverage
    _sa = kube_resource("kube/flush-manager.sa.yml", **options)
    _cluster_role = kube_resource("kube/flush-manager.clusterrole.yml", **options)
    _cluster_role_binding = kube_resource(
        "kube/flush-manager.clusterrolebinding.yml", **options
    )
    _role = kube_resource("kube/flush-manager.role.yml", **options)
    _role_binding = kube_resource("kube/flush-manager.rolebinding.yml", **options)
    _deploy = kube_resource("kube/flush-manager.deploy.yml", **options)
    with _sa, _cluster_role, _cluster_role_binding, _role, _role_binding, _deploy:
        print("posting message 2 until successful")
        for i in range(max_wait_loops):
            try:
                requests.post(
                    web, headers={"host": "web"}, json={"id": 2}
                ).raise_for_status()
            except requests.exceptions.ConnectionError:
                if i > 0 and static_pvs:
                    create_static_pvs(api)
            else:
                break
            time.sleep(1)
        else:
            assert False, "pod did not recover"
        # scale to 0 pods
        print("scaling web to 0 pods")
        AppsV1Api().patch_namespaced_stateful_set_scale(
            name="web",
            namespace="default",
            body=V1StatefulSet(
                api_version="apps/v1", kind="StatefulSet", spec=dict(replicas=0)
            ),
        )
        # wait for no pvcs
        print("waiting for cleanup to complete")
        for _ in range(max_wait_loops):
            if not api.list_persistent_volume().items:
                break
            time.sleep(1)
        else:
            print("pvs were not cleaned up")
            assert [] == api.list_persistent_volume().items
    # assert jobs and pvcs also deleted
    assert [] == list_pvcs(api)
    assert [] == BatchV1Api().list_namespaced_job("default").items
    # assert received message id 0 and 2
    assert [b'{"id": 0}', b'{"id": 2}'] == [
        element.message.data
        for element in sub_client.pull(subscription_path, 2).received_messages
    ]
