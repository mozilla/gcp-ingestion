from functools import partial
from contextlib import contextmanager
from traceback import print_exc
from typing import Any, Callable, Dict, Iterator, Optional
import os.path
import time
import re

from google.api_core.exceptions import AlreadyExists
from google.cloud.container_v1 import ClusterManagerClient
from google.cloud.container_v1.types import (
    Cluster,
    NodePool,
    Operation,
    GetOperationRequest,
)
from kubernetes.config import load_kube_config
from kubernetes.client.rest import ApiException
import _pytest.config.argparsing  # for types only
import _pytest.fixtures  # for types only
import google.auth
import kubernetes.client as kube
import pytest
import yaml


def pytest_addoption(parser: _pytest.config.argparsing.Parser):
    # Cluster options
    group = parser.getgroup("flush manager tests")
    group.addoption(
        "--docker-desktop",
        action="store_const",
        const=None,
        dest="cluster",
        help="Use docker-desktop kubernetes configuration instead of GKE",
    )
    group.addoption(
        "--cluster",
        dest="cluster",
        default="flush-manager-test",
        help="Name of GKE cluster to create for test resources; "
        "default is 'flush-manager-test'",
    )
    group.addoption(
        "--location",
        dest="location",
        default="us-west1-a",
        help="Location to use for --cluster; can be a region or zone; "
        "default is us-west1-a",
    )
    group.addoption(
        "--preemptible",
        action="store_true",
        dest="preemptible",
        default=False,
        help="Use preemptible instances for --cluster; default is False",
    )
    group.addoption(
        "--project",
        dest="project",
        default=None,
        help="Project to use for --cluster; default is from credentials",
    )
    # Deployment options
    group.addoption(
        "--image",
        dest="image",
        default="mozilla/ingestion-edge:latest",
        help="Docker image for server, flush manager, and flush containers;"
        "default is 'mozilla/ingestion-edge:latest'",
    )
    group.addoption(
        "--topic",
        dest="topic",
        default="topic",
        help="PubSub topic name, default is 'topic'",
    )


@pytest.fixture
def options(request: _pytest.fixtures.SubRequest) -> Dict[str, Any]:
    result = vars(request.config.option)
    result["provisioner"] = (
        "kubernetes.io/no-provisioner"
        if result["cluster"] is None
        else "kubernetes.io/gce-pd"
    )
    return result


@pytest.fixture
def cluster(options: Dict[str, Any]) -> Iterator[Optional[str]]:
    if options["cluster"] is None:
        load_kube_config()
        if options["project"] is None:
            options["project"] = "test"
        yield None
    else:
        credentials, project = google.auth.default()
        if options["project"] is None:
            options["project"] = project
        gke = ClusterManagerClient(credentials=credentials)
        # create cluster
        parent = f"projects/{options['project']}/locations/{options['location']}"
        name = f"{parent}/clusters/{options['cluster']}"
        try:
            operation = gke.create_cluster(
                cluster=Cluster(
                    name=options["cluster"],
                    logging_service=None,
                    monitoring_service=None,
                    node_pools=[
                        NodePool(
                            initial_node_count=1,
                            name="test",
                            config={
                                "preemptible": options["preemptible"],
                                "machine_type": "n1-highcpu-2",
                            },
                        )
                    ],
                ),
                parent=parent,
            )
        except AlreadyExists:
            pass
        else:
            # wait for operation to complete
            request = GetOperationRequest(
                name=operation.self_link.split("projects").pop()
            )
            while gke.get_operation(request).status <= Operation.Status.RUNNING:
                time.sleep(15)
        # set kube credentials
        cluster = gke.get_cluster(name=name)
        config = kube.Configuration()
        config.host = f"https://{cluster.endpoint}:443"
        config.verify_ssl = False
        config.api_key = {"authorization": f"Bearer {credentials.token}"}
        kube.Configuration.set_default(config)
        # delete cluster after test completes
        try:
            yield options["cluster"]
        finally:
            try:
                # delete persistent volumes because gke cluster delete won't do it
                # https://cloud.google.com/kubernetes-engine/docs/how-to/deleting-a-cluster#overview
                api = kube.CoreV1Api()
                for pv in api.list_persistent_volume().items:
                    try:
                        pv.spec.persistent_volume_reclaim_policy = "Delete"
                        api.patch_persistent_volume(
                            name=pv.metadata.name, body=pv,
                        )
                        api.delete_persistent_volume(
                            name=pv.metadata.name,
                            grace_period_seconds=0,
                            propagation_policy="Foreground",
                        )
                    except ApiException:
                        print_exc()
                # wait for pv deletes to complete
                for _ in range(60):
                    if not api.list_persistent_volume().items:
                        break
                    time.sleep(1)
                else:
                    print("FAILED TO CLEANUP PERSISTENT VOLUMES")
            finally:
                gke.delete_cluster(name=name)


def kube_methods(path: str, **replace) -> Dict[str, Callable]:
    with open(os.path.join(os.path.dirname(__file__), path)) as p:
        raw = p.read().format_map(replace)
    body = yaml.load(raw)
    kind = re.sub(r"(?<!^)(?=[A-Z])", "_", body["kind"]).lower()
    name = body["metadata"]["name"]
    namespace = body["metadata"].get("namespace")
    is_namespaced = namespace is not None
    group, _, version = body["apiVersion"].partition("/")
    if not version:
        group, version = "core", group
    group = group.replace(".k8s.io", "").title().replace(".", "")
    api = getattr(kube, f"{group}{version.capitalize()}Api")()
    return {
        method: partial(
            getattr(api, f"{method}{'_namespaced' if is_namespaced else ''}_{kind}"),
            *args,
        )
        for method, args in {
            "create": [namespace, body] if is_namespaced else [body],
            "patch": [name, namespace, body] if is_namespaced else [name, body],
            "read": [name, namespace] if is_namespaced else [name],
            "delete": [name, namespace] if is_namespaced else [name],
        }.items()
    }


@contextmanager
def kube_resource(path: str, cluster: Optional[str], **kwargs) -> Iterator[Any]:
    methods = kube_methods(path, **kwargs)
    try:
        result = methods["create"]()
    except ApiException as e:
        if e.reason == "Conflict":
            # patch if already exists
            result = methods["patch"]()
        else:
            raise
    try:
        yield result
    finally:
        try:
            methods["delete"]()
        except ApiException as e:
            if e.reason != "Not Found":
                raise


@pytest.fixture
def emulator(cluster: Optional[str], options: Dict[str, Any]) -> Iterator[str]:
    _deploy = kube_resource("kube/emulator.deploy.yml", **options)
    _svc = kube_resource("kube/emulator.svc.yml", **options)
    with _deploy, _svc:
        read_svc = kube_methods("kube/emulator.svc.yml")["read"]
        while True:
            svc = read_svc()
            for ingress in svc.status.load_balancer.ingress or ():
                host = ingress.ip or ingress.hostname
                if host:
                    yield f"{host}:{svc.spec.ports[0].port}"
                    return
            time.sleep(1)


@pytest.fixture
def web(cluster: Optional[str], options: Dict[str, Any]) -> Iterator[str]:
    _sc = kube_resource("kube/web.sc.yml", **options)
    _sts = kube_resource("kube/web.sts.yml", **options)
    _svc = kube_resource("kube/web.svc.yml", **options)
    with _sc, _sts, _svc:
        read_svc = kube_methods("kube/web.svc.yml")["read"]
        while True:
            svc = read_svc()
            for ingress in svc.status.load_balancer.ingress or ():
                host = ingress.ip or ingress.hostname
                if host:
                    yield f"http://{host}:{svc.spec.ports[0].port}/test"
                    return
            time.sleep(1)
