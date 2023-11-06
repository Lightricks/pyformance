# Copyright (c) 2023 Lightricks. All rights reserved.
# -*- coding: utf-8 -*-
import logging
import os
import re
import time
import uuid

from six import iteritems

from pyformance.reporters.reporter import Reporter
from pyformance.reporters.utils import to_timestamp_in_precision, ReportingPrecision
from pyformance.registry import MetricsRegistry
from pyformance.mark_int import MarkInt
from copy import copy

LOG = logging.getLogger(__name__)


class LineProtocolReporter(Reporter):
    """
    Line Protocol reporter using InfluxDB format.
    """

    def __init__(
            self,
            registry: MetricsRegistry = None,
            reporting_interval: int = 30,
            prefix: str = "",
            path: str | None = None,
            path_suffix: str | None = None,
            clock: time = None,
            global_tags: dict = None,
            reporting_precision = ReportingPrecision.SECONDS,
    ):
        """
        :param reporting_precision: The precision in which the reporter reports.
        The default is seconds. This is a tradeoff between precision and performance. More
        coarse precision may result in significant improvements in compression and vice versa.
        """
        super(LineProtocolReporter, self).__init__(registry, reporting_interval, clock)
        self.path = self._set_path(path, path_suffix)
        self.prefix = prefix

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        if global_tags is None:
            self.global_tags = {}
        else:
            self.global_tags = global_tags

        self.reporting_precision = reporting_precision

    def report_now(self, registry=None, timestamp=None) -> None:
        timestamp = timestamp or self.clock.time()
        timestamp_in_reporting_precision = to_timestamp_in_precision(
            timestamp=timestamp,
            precision=self.reporting_precision
        )
        metrics = (registry or self.registry).dump_metrics(key_is_metric=True)
        influx_lines = self._get_influx_protocol_lines(metrics, timestamp_in_reporting_precision)

        if influx_lines:
            with open(f"{self.path}/{uuid.uuid4().hex}.txt", "a") as file:
                post_data = "\n".join(influx_lines)
                file.write(post_data)

    def _get_table_name(self, metric_key) -> str:
        if not self.prefix:
            return metric_key
        else:
            return "%s.%s" % (self.prefix, metric_key)

    def _get_influx_protocol_lines(self, metrics, timestamp) -> list:
        lines = []
        for key, metric_values in metrics.items():
            metric_name = key.get_key()
            table = self._get_table_name(metric_name)
            values = LineProtocolReporter._stringify_values(metric_values)
            tags = self._stringify_tags(key)

            # there's a special case where only events are present, which are skipped by
            # _stringify_values function
            if values:
                line = "%s%s %s %s" % (table, tags, values, timestamp)
                lines.append(line)

            for event in metric_values.get("events", []):
                values = LineProtocolReporter._stringify_values(event.values)

                event_timestamp = to_timestamp_in_precision(
                    timestamp=event.time,
                    precision=self.reporting_precision
                )
                line = "%s%s %s %s" % (
                    table,
                    tags,
                    values,
                    event_timestamp
                )

                lines.append(line)

        return lines

    @staticmethod
    def _stringify_values(metric_values) -> str:
        return ",".join(
            [
                "%s=%s" % (k, _format_field_value(v))
                for (k, v) in iteritems(metric_values) if k != "tags" and k != "events"
            ]
        )

    def _stringify_tags(self, metric) -> str:
        # start with the global reporter tags
        # (copy to avoid mutating to global values)
        all_tags = copy(self.global_tags)

        # add the local tags on top of those
        tags = metric.get_tags()
        all_tags.update(tags)

        if all_tags:
            return "," + ",".join(
                [
                    "%s=%s" % (k, _format_tag_value(v))
                    for (k, v) in iteritems(all_tags)
                ]
            )

        return ""

    @staticmethod
    def _set_path(path: str | None, path_suffix: str | None) -> str:
        if not path:
            path = f"/tmp/metrics"
        if path_suffix:
            path += f"/{path_suffix}"

        os.environ["METRICS_REPORTER_FOLDER_PATH"] = path
        return path

def _format_field_value(value) -> str:
    if isinstance(value, MarkInt):
        return f"{value.value}i"
    if type(value) is not str:
        return value
    else:
        return '"{}"'.format(value)


def _format_tag_value(value) -> str:
    if type(value) is not str:
        return value
    else:
        # Escape special characters
        return re.sub("([ ,=])", r"\\\1", value)
