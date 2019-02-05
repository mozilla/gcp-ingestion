# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from functools import partial
from google.api_core.exceptions import AlreadyExists
from google.cloud.container_v1 import ClusterManagerClient
from google.cloud.container_v1.types import Cluster, NodePool, Operation
from google.cloud.pubsub_v1 import PublisherClient
from typing import Any, Callable, Dict, List, Generator

# importing from private module _pytest for types only
import _pytest.config.argparsing
import _pytest.fixtures
import google.auth
import json
import kubernetes.client as kube
import os.path
import pytest
import time
import yaml


def pytest_addoption(parser: _pytest.config.argparsing.Parser):
    # Test options
    parser.addoption(
        "--min-success-rate",
        dest="min_success_rate",
        default=1000,
        type=int,
        help="Minimum 200 responses per non-200 response to require during "
        "--test-period, default is 1000 (0.1%% errors)",
    )
    parser.addoption(
        "--min-throughput",
        dest="min_throughput",
        default=15000,
        type=int,
        help="Minimum 200 responses per second to require during --test-period, "
        "default is 15000",
    )
    parser.addoption(
        "--test-period",
        dest="test_period",
        default=1800,
        type=int,
        help="Number of seconds to evaluate after warmup, default is 1800 (30 minutes)",
    )
    parser.addoption(
        "--warmup-threshold",
        dest="warmup_threshold",
        default=15000,
        type=int,
        help="Minimum 200 responses per second that indicate warmup is complete, "
        "default is 15000",
    )
    parser.addoption(
        "--warmup-timeout",
        dest="warmup_timeout",
        default=600,
        type=int,
        help="Maximum number of seconds to wait for warmup to complete, default is "
        "600 (10 minutes)",
    )
    # Cluster options
    parser.addoption(
        "--cluster",
        dest="cluster",
        default="load-test",
        help="Name of GKE cluster to create for test resources, default is 'load-test',"
        " ignored when --load-balancer and --no-traffic-generator are both specified",
    )
    parser.addoption(
        "--location",
        dest="location",
        default="us-west1",
        help="Location to use for --cluster, default is us-west1",
    )
    parser.addoption(
        "--preemptible",
        action="store_true",
        dest="preemptible",
        default=False,
        help="Use preemptible instances for --cluster, default is False",
    )
    parser.addoption(
        "--project",
        dest="project",
        default=None,
        help="Project to use for --cluster, default is from credentials",
    )
    # Server options
    parser.addoption(
        "--load-balancer",
        dest="load_balancer",
        default=None,
        help="Load Balancing url map to monitor, implies --no-generator when "
        "--server-uri is not specified, ignores --image and --no-emulator",
    )
    parser.addoption(
        "--server-uri",
        dest="server_uri",
        default=None,
        help="Server uri like 'https://edge.stage.domain.com/submit/telemetry/suffix', "
        "ignored when --no-generator is specified or --load-balancer is missing",
    )
    parser.addoption(
        "--image",
        dest="image",
        default="mozilla/ingestion-edge:latest",
        help="Docker image for server deployment, default is "
        "'mozilla/ingestion-edge:latest', ignored when --load-balancer is specified",
    )
    parser.addoption(
        "--no-emulator",
        action="store_false",
        dest="emulator",
        default=True,
        help="Don't use a PubSub emulator, ignored when --load-balancer is specified",
    )
    parser.addoption(
        "--topic",
        dest="topic",
        default="topic",
        help="PubSub topic name, default is 'topic', ignored when --load-balancer is "
        "specified",
    )
    # Traffic generator options
    parser.addoption(
        "--no-generator",
        action="store_false",
        dest="generator",
        default=True,
        help="Don't deploy a traffic generator, ignore --script",
    )
    parser.addoption(
        "--script",
        dest="script",
        default="tests/load/wrk/telemetry.lua",
        help="Lua script to use for traffic generator deployment, default is "
        "'tests/load/wrk/telemetry.lua', ignored when --no-generator is specified",
    )


@pytest.fixture
def options(request: _pytest.fixtures.SubRequest) -> Dict[str, Any]:
    return vars(request.config.option)


@pytest.fixture
def node_pools(options: Dict[str, Any]) -> Generator[List[str], None, None]:
    credentials, project = google.auth.default()
    if options["project"] is None:
        options["project"] = project
    gke = ClusterManagerClient(credentials=credentials)
    # build node pool configurations
    pools = {}
    if options["generator"]:
        if options["load_balancer"] is None or options["server_uri"] is not None:
            pools["generator"] = NodePool(initial_node_count=1)
            pools["generator"].config.machine_type = "n1-highcpu-2"
    if options["load_balancer"] is None:
        pools["server"] = NodePool(initial_node_count=4)
        pools["server"].config.machine_type = "n1-highcpu-2"
        if options["emulator"]:
            pools["emulator"] = NodePool(initial_node_count=1)
        else:
            # need pubsub permissions
            pools["server"].config.oauth_scopes.append(
                "https://www.googleapis.com/auth/pubsub"
            )
    # add labels
    for name, pool in pools.items():
        pool.name = name
        pool.config.preemptible = options["preemptible"]
        pool.config.labels["name"] = name
        if options["location"][-2] == "-":
            # triple node count for single zone cluster
            pool.initial_node_count *= 3
    # create cluster
    if not pools:
        yield []  # nothing to create
    else:
        kwargs = {
            "project_id": None,
            "zone": None,
            "cluster": Cluster(
                name=options["cluster"],
                logging_service=None,
                monitoring_service=None,
                node_pools=list(pools.values()),
            ),
            "parent": f"projects/{options['project']}/locations/{options['location']}",
        }
        name = f"{kwargs['parent']}/clusters/{options['cluster']}"
        try:
            response = gke.create_cluster(**kwargs)
        except AlreadyExists:
            pass
        else:
            # wait for operation to complete
            op = response.self_link.split("projects").pop()
            while gke.get_operation(None, None, None, op).status <= Operation.RUNNING:
                time.sleep(15)
        # set kube credentials
        cluster = gke.get_cluster(None, None, None, name)
        config = kube.Configuration()
        config.host = f"https://{cluster.endpoint}:443"
        config.verify_ssl = False
        config.api_key = {"authorization": f"Bearer {credentials.token}"}
        kube.Configuration.set_default(config)
        # delete cluster after test completes
        yield list(pools)
        gke.delete_cluster(None, None, None, name)


def kube_methods(path, **replace) -> Dict[str, Callable]:
    with open(os.path.join(os.path.dirname(__file__), path)) as p:
        raw = p.read().format_map(replace)
    body = yaml.load(raw)
    kind = body["kind"].lower()
    name = body["metadata"]["name"]
    namespace = body["metadata"].get("namespace", "default")
    group, _, version = body["apiVersion"].partition("/")
    if not version:
        group, version = "core", group
    api = getattr(kube, f"{group.capitalize()}{version.capitalize()}Api")()
    return {
        method: partial(getattr(api, f"{method}_namespaced_{kind}"), *args)
        for method, args in {
            "create": [namespace, body],
            "patch": [name, namespace, body],
            "read": [name, namespace],
        }.items()
    }


def kube_apply(path, **kwargs):
    methods = kube_methods(path, **kwargs)
    try:
        return methods["create"]()
    except kube.rest.ApiException as e:
        if json.loads(e.body)["reason"] == "AlreadyExists":
            # patch if already exists
            return methods["patch"]()
        raise


@pytest.fixture
def emulator(
    node_pools: List[str], options: Dict[str, Any]
) -> Generator[str, None, None]:
    if "emulator" in node_pools:
        kube_apply("kube/emulator.deploy.yml", **options)
        kube_apply("kube/emulator.svc.yml")
        yield "emulator:8000"
    elif "server" in node_pools:
        pubsub = PublisherClient()
        topic = f"projects/{options['project']}/topics/{options['topic']}"
        try:
            pubsub.create_topic(topic)
        except AlreadyExists:
            pass
        yield ""
        pubsub.delete_topic(topic)
    else:
        yield ""


@pytest.fixture
def server_uri(emulator: str, node_pools: List[str], options: Dict[str, Any]) -> str:
    if "server" in node_pools:
        kube_apply("kube/server.secret.yml")
        kube_apply("kube/server.deploy.yml", emulator_host=emulator, **options)
        kube_apply("kube/server.svc.yml")
        kube_apply("kube/server.ingress.yml")
        methods = kube_methods("kube/server.ingress.yml")
        while True:
            ingress = methods["read"]().status.load_balancer.ingress
            if ingress is not None and ingress[0].ip:
                return f"https://{ingress[0].ip}/submit/telemetry/suffix"
            time.sleep(15)
    return options["server_uri"]


@pytest.fixture
def load_balancer(
    node_pools: List[str], options: Dict[str, Any], server_uri: str
) -> str:
    if "server" in node_pools:
        methods = kube_methods("kube/server.ingress.yml")
        while True:
            metadata = methods["read"]().metadata
            if "ingress.kubernetes.io/url-map" in metadata.annotations:
                return metadata.annotations["ingress.kubernetes.io/url-map"]
            time.sleep(15)
    return options["load_balancer"]


@pytest.fixture
def generator(node_pools: List[str], options: Dict[str, Any], server_uri: str):
    if "generator" in node_pools:
        kube_apply("kube/generator.deploy.yml", server_uri=server_uri, **options)
