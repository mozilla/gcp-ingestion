"""Continuously flush ingestion-edge queue from detached persistent volumes."""

from argparse import ArgumentParser
from functools import partial
from multiprocessing.pool import ThreadPool
from time import sleep
from typing import Callable, List
import json
import os

from kubernetes.config import load_incluster_config
from kubernetes.client import (
    BatchV1Api,
    CoreV1Api,
    V1Container,
    V1DeleteOptions,
    V1EnvVar,
    V1Job,
    V1JobSpec,
    V1ObjectMeta,
    V1ObjectReference,
    V1OwnerReference,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1PersistentVolumeSpec,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Preconditions,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)
from kubernetes.client.rest import ApiException

from .config import get_config_dict, logger

JOB_AND_PVC_PREFIX = "flush-"
ALREADY_EXISTS = "AlreadyExists"
CONFLICT = "Conflict"
NOT_FOUND = "Not Found"

DEFAULT_IMAGE_VERSION = "latest"
try:
    with open("version.json") as fp:
        DEFAULT_IMAGE_VERSION = str(
            json.load(fp).get("version") or DEFAULT_IMAGE_VERSION
        )
except (FileNotFoundError, json.decoder.JSONDecodeError):  # pragma: no cover
    pass  # use default

DEFAULT_ENV = [
    V1EnvVar(name=name, value=os.environ[name])
    for name in get_config_dict()
    if name in os.environ
]

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--command",
    default=["python", "-m", "ingestion_edge.flush"],
    type=json.loads,
    help="Docker command for flush jobs; must drain queue until empty or exit non-zero",
)
parser.add_argument(
    "--env", default=DEFAULT_ENV, type=json.loads, help="env to use for flush jobs",
)
parser.add_argument(
    "--image",
    default="mozilla/ingestion-edge:" + DEFAULT_IMAGE_VERSION,
    help="Docker image to use for flush jobs",
)
parser.add_argument(
    "--namespace", default="default", help="Kubernetes namespace to use",
)
parser.add_argument(
    "--claim-prefix",
    default="queue-",
    help="Prefix for the names of persistent volume claims to delete",
)


def _job_and_pvc_name_from_pv(pv: V1PersistentVolume) -> str:
    return JOB_AND_PVC_PREFIX + pv.metadata.name


def _pv_name_from_job(job: V1Job) -> str:
    return job.metadata.name.replace(JOB_AND_PVC_PREFIX, "", 1)


def _is_flush_job(job: V1Job) -> bool:
    return job.metadata.name.startswith(JOB_AND_PVC_PREFIX)


def _create_pvc(
    api: CoreV1Api, name: str, namespace: str, pv: V1PersistentVolume
) -> V1PersistentVolumeClaim:
    logger.info(f"creating pvc: {name}")
    try:
        return api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=V1PersistentVolumeClaim(
                api_version="v1",
                kind="PersistentVolumeClaim",
                metadata=V1ObjectMeta(name=name, namespace=namespace),
                spec=V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=V1ResourceRequirements(requests=pv.spec.capacity),
                    storage_class_name=pv.spec.storage_class_name,
                    volume_name=pv.metadata.name,
                ),
            ),
        )
    except ApiException as e:
        if e.reason == CONFLICT and json.loads(e.body)["reason"] == ALREADY_EXISTS:
            logger.info(f"using existing pvc: {name}")
            return api.read_namespaced_persistent_volume_claim(name, namespace)
        raise


def _bind_pvc(
    api: CoreV1Api, pv: V1PersistentVolume, pvc: V1PersistentVolumeClaim
) -> V1PersistentVolume:
    logger.info(f"binding pv to pvc: {pv.metadata.name}, {pvc.metadata.name}")
    return api.patch_persistent_volume(
        name=pv.metadata.name,
        body=V1PersistentVolume(
            spec=V1PersistentVolumeSpec(
                claim_ref=V1ObjectReference(
                    api_version="v1",
                    kind="PersistentVolumeClaim",
                    name=pvc.metadata.name,
                    namespace=pvc.metadata.namespace,
                    resource_version=pvc.metadata.resource_version,
                    uid=pvc.metadata.uid,
                )
            )
        ),
    )


def _create_flush_job(
    batch_api: BatchV1Api,
    command: List[str],
    env: List[V1EnvVar],
    image: str,
    name: str,
    namespace: str,
) -> V1Job:
    logger.info(f"creating job: {name}")
    try:
        return batch_api.create_namespaced_job(
            namespace=namespace,
            body=V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=V1ObjectMeta(name=name, namespace=namespace),
                spec=V1JobSpec(
                    template=V1PodTemplateSpec(
                        spec=V1PodSpec(
                            containers=[
                                V1Container(
                                    image=image,
                                    command=command,
                                    name="flush",
                                    volume_mounts=[
                                        V1VolumeMount(mount_path="/data", name="queue")
                                    ],
                                    env=env,
                                )
                            ],
                            restart_policy="OnFailure",
                            volumes=[
                                V1Volume(
                                    name="queue",
                                    persistent_volume_claim=(
                                        V1PersistentVolumeClaimVolumeSource(
                                            claim_name=name
                                        )
                                    ),
                                )
                            ],
                        )
                    )
                ),
            ),
        )
    except ApiException as e:
        if e.reason == CONFLICT and json.loads(e.body)["reason"] == ALREADY_EXISTS:
            logger.info(f"using existing job: {name}")
            return batch_api.read_namespaced_job(name, namespace)
        raise


def flush_released_pvs(
    api: CoreV1Api,
    batch_api: BatchV1Api,
    command: List[str],
    env: List[V1EnvVar],
    image: str,
    namespace: str,
):
    """
    Flush persistent volumes.

    Gracefully handle resuming after an interruption, because this is not atomic.
    """
    existing_jobs = {
        job.metadata.name for job in batch_api.list_namespaced_job(namespace).items
    }
    for pv in api.list_persistent_volume().items:
        name = _job_and_pvc_name_from_pv(pv)
        if (
            name not in existing_jobs
            and pv.spec.claim_ref
            and pv.spec.claim_ref.namespace == namespace
            and pv.spec.persistent_volume_reclaim_policy != "Delete"
            and pv.status
            and (pv.status.phase == "Released" or pv.spec.claim_ref.name == name)
        ):
            logger.info(f"flushing unbound pv: {pv.metadata.name}")
            if pv.status.phase != "Bound":
                pvc = _create_pvc(api, name, namespace, pv)
                _bind_pvc(api, pv, pvc)
            _create_flush_job(batch_api, command, env, image, name, namespace)


def delete_complete_jobs(api: CoreV1Api, batch_api: BatchV1Api, namespace: str):
    """Delete complete jobs."""
    for job in batch_api.list_namespaced_job(namespace).items:
        if (
            job.status.conditions
            and job.status.conditions[0].type == "Complete"
            and not job.metadata.deletion_timestamp
            and _is_flush_job(job)
        ):
            logger.info(f"deleting complete job: {job.metadata.name}")
            # configure persistent volume claims to be deleted with the job
            pv_name = _pv_name_from_job(job)
            logger.info(f"including pv in pvc delete: {pv_name}")
            api.patch_persistent_volume(
                name=pv_name,
                body=V1PersistentVolume(
                    spec=V1PersistentVolumeSpec(
                        persistent_volume_reclaim_policy="Delete",
                    )
                ),
            )
            logger.info(f"including pvc in job delete: {job.metadata.name}")
            api.patch_namespaced_persistent_volume_claim(
                name=job.metadata.name,
                namespace=namespace,
                body=V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        owner_references=[
                            V1OwnerReference(
                                api_version="batch/v1",
                                kind="Job",
                                name=job.metadata.name,
                                uid=job.metadata.uid,
                                block_owner_deletion=True,
                            )
                        ]
                    )
                ),
            )
            try:
                batch_api.delete_namespaced_job(
                    name=job.metadata.name,
                    namespace=namespace,
                    body=V1DeleteOptions(
                        grace_period_seconds=0,
                        propagation_policy="Foreground",
                        preconditions=V1Preconditions(
                            resource_version=job.metadata.resource_version,
                            uid=job.metadata.uid,
                        ),
                    ),
                )
            except ApiException as e:
                if e.reason not in (CONFLICT, NOT_FOUND):
                    raise
                logger.info(f"job already deleted or updated: {job.metadata.name}")


def flush_released_pvs_and_delete_complete_jobs(
    api: CoreV1Api,
    batch_api: BatchV1Api,
    command: List[str],
    env: List[V1EnvVar],
    image: str,
    namespace: str,
):
    """Flush released persistent volumes then delete complete jobs.

    Run sequentially to avoid race conditions.
    """
    flush_released_pvs(api, batch_api, command, env, image, namespace)
    delete_complete_jobs(api, batch_api, namespace)


def _unschedulable_due_to_pvc(pod: V1Pod):
    return (
        pod.status
        and pod.status.phase == "Pending"
        and (condition := (pod.status.conditions or [None])[0])
        and condition.reason == "Unschedulable"
        and condition.message
        and condition.message.startswith('persistentvolumeclaim "')
        and condition.message.endswith('" not found')
        and pod.metadata.owner_references
        and any(ref.kind == "StatefulSet" for ref in pod.metadata.owner_references)
    )


def delete_detached_pvcs(api: CoreV1Api, namespace: str, claim_prefix: str):
    """
    Delete persistent volume claims that are not attached to any pods.

    If a persistent volume claim is deleted while attached to a pod, then the
    underlying persistent volume will remain bound until the delete is
    finalized, and the delete will not be finalized until the pod is also
    deleted.

    If a stateful set immediately recreates a pod (e.g. via `kubectl rollout
    restart`) that was attached to a persistent volume claim that was deleted,
    then the stateful set may still try to reuse the persistent volume claim
    after the delete is finalized. Delete the pod again to cause the stateful
    set to recreate the persistent volume claim when it next recreates the pod.
    """
    attached_pvcs = {
        volume.persistent_volume_claim.claim_name
        for pod in api.list_namespaced_pod(namespace).items
        if not _unschedulable_due_to_pvc(pod) and pod.spec and pod.spec.volumes
        for volume in pod.spec.volumes
        if volume.persistent_volume_claim
    }
    for pvc in api.list_namespaced_persistent_volume_claim(namespace).items:
        if (
            pvc.metadata.name.startswith(claim_prefix)
            and pvc.metadata.name not in attached_pvcs
            and not pvc.metadata.deletion_timestamp
        ):
            logger.info(f"deleting detached pvc: {pvc.metadata.name}")
            try:
                api.delete_namespaced_persistent_volume_claim(
                    name=pvc.metadata.name,
                    namespace=namespace,
                    body=V1DeleteOptions(
                        grace_period_seconds=0,
                        propagation_policy="Background",
                        preconditions=V1Preconditions(
                            resource_version=pvc.metadata.resource_version,
                            uid=pvc.metadata.uid,
                        ),
                    ),
                )
            except ApiException as e:
                if e.reason not in (CONFLICT, NOT_FOUND):
                    raise
                logger.info(f"pvc already deleted or updated: {pvc.metadata.name}")


def delete_unschedulable_pods(api: CoreV1Api, namespace: str):
    """
    Delete pods that are unschedulable due to a missing persistent volume claim.

    A stateful set may create a pod attached to a missing persistent volume
    claim if the pod is recreated while the persistent volume claim is pending
    delete.

    When this happens, delete the pod so that the stateful set will create a
    new persistent volume claim when it next creates the pod.
    """
    for pod in api.list_namespaced_pod(namespace).items:
        if _unschedulable_due_to_pvc(pod):
            logger.info(f"deleting unschedulable pod: {pod.metadata.name}")
            try:
                api.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=namespace,
                    body=V1DeleteOptions(
                        grace_period_seconds=0,
                        propagation_policy="Background",
                        preconditions=V1Preconditions(
                            resource_version=pod.metadata.resource_version,
                            uid=pod.metadata.uid,
                        ),
                    ),
                )
            except ApiException as e:
                if e.reason not in (CONFLICT, NOT_FOUND):
                    raise
                logger.info(f"pod already deleted or updated: {pod.metadata.name}")


def run_task(func: Callable[[], None]):
    """Continuously run func and print exceptions."""
    while True:
        try:
            func()
        except Exception:
            logger.exception("unhandled exception")
        else:
            sleep(1)


def main():
    """Continuously flush and delete detached persistent volumes."""
    args = parser.parse_args()
    load_incluster_config()
    api = CoreV1Api()
    batch_api = BatchV1Api()
    tasks = [
        partial(
            flush_released_pvs_and_delete_complete_jobs,
            api,
            batch_api,
            args.command,
            args.env,
            args.image,
            args.namespace,
        ),
        partial(delete_detached_pvcs, api, args.namespace, args.claim_prefix),
        partial(delete_unschedulable_pods, api, args.namespace),
    ]
    with ThreadPool(len(tasks)) as pool:
        pool.map(run_task, tasks, chunksize=1)


if __name__ == "__main__":  # pragma: no cover
    main()
