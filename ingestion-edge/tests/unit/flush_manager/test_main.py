from functools import partial
from unittest.mock import MagicMock, call, patch

import pytest

from ingestion_edge.flush_manager import (
    flush_released_pvs_and_delete_complete_jobs,
    run_task,
    main,
)


def _task(values):
    result = values.pop()
    if isinstance(result, BaseException):
        raise result
    return result


class SystemException(BaseException):
    pass


def test_run_task():
    with pytest.raises(SystemException):
        run_task(partial(_task, [SystemException("stop"), ValueError("ignored"), None]))


def test_flush_and_delete(api: MagicMock, batch_api: MagicMock):
    flush_released_pvs_and_delete_complete_jobs(
        api, batch_api, "command", "env", "image", "namespace"
    )
    api.list_persistent_volume.assert_called_once_with()
    assert [
        call("namespace") for _ in range(2)
    ] == batch_api.list_namespaced_job.call_args_list


def test_main():
    # mock run_task
    _cm_run_task = patch("ingestion_edge.flush_manager.run_task")
    _cm_sys_argv = patch("sys.argv", [])
    _cm_kube_config = patch("ingestion_edge.flush_manager.load_incluster_config")
    with _cm_run_task as _run_task, _cm_sys_argv, _cm_kube_config:
        main()
    assert 3 == _run_task.call_count
