# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from google.cloud.monitoring_v3 import MetricServiceClient
from google.cloud.monitoring_v3.types import Aggregation, TimeInterval
from typing import Any, Dict, Union
import time

stackdriver = MetricServiceClient()


def get_interval(seconds: int = 120) -> TimeInterval:
    interval = TimeInterval()
    interval.end_time.seconds = int(time.time())
    interval.start_time.seconds = interval.end_time.seconds - seconds
    return interval


def get_metric_value(*args, **kwargs) -> Union[int, float]:
    """Get the last value of the first time series returned by StackDriver"""
    for series in stackdriver.list_time_series(*args, **kwargs):
        sorted_points = sorted(series.points, key=lambda p: p.interval.end_time.seconds)
        value = sorted_points[-1].value
        return value.int64_value or value.double_value or 0
    return 0


def test_throughput(load_balancer: str, generator: None, options: Dict[str, Any]):
    # Initialize
    path = f"projects/{options['project']}"
    base = (
        'metric.type = "loadbalancing.googleapis.com/https/request_count"'
        f' AND resource.labels.url_map_name = "{load_balancer}"'
    )
    success = base + " AND metric.labels.response_code_class = 200"
    failure = base + " AND metric.labels.response_code_class != 200"
    view = "FULL"
    aggregation = Aggregation(group_by_fields=["metric.type"])
    aggregation.cross_series_reducer = Aggregation.REDUCE_SUM
    aggregation.per_series_aligner = Aggregation.ALIGN_RATE
    aggregation.alignment_period.seconds = 60

    # Wait for server to warm up
    assert options["warmup_threshold"] > 0
    assert options["warmup_timeout"] > 0
    start = int(time.time())
    while True:
        throughput = get_metric_value(path, success, get_interval(), view, aggregation)
        if throughput >= options["warmup_threshold"]:
            break
        if int(time.time()) - start > options["warmup_timeout"]:
            raise TimeoutError("Warmup timed out")
        time.sleep(15)

    # Wait for test_period of post-warmup data
    # TODO Check how long warmup_threshold was exceeded and subtract from sleep time
    time.sleep(options["test_period"])

    # Evaluate min_success_rate criteria
    interval = get_interval(options["test_period"])
    aggregation.alignment_period.seconds = options["test_period"]
    throughput = get_metric_value(path, base, interval, view, aggregation)
    assert throughput >= options["min_throughput"]

    # Evaluate min_success_ratio criteria
    aggregation.per_series_aligner = Aggregation.ALIGN_SUM
    failure_count = get_metric_value(path, failure, interval, view, aggregation)
    if failure_count > 0:
        success_count = get_metric_value(path, success, interval, view, aggregation)
        assert success_count // failure_count > options["min_success_rate"]
