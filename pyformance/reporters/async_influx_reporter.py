# -*- coding: utf-8 -*-
import json
import logging
import re
from enum import Enum
from typing import Dict, List, Optional
from uuid import uuid4

from boto3 import Session, client
from botocore.config import Config
from six import iteritems

try:
    from urllib2 import Request, URLError, quote, urlopen
except ImportError:
    from urllib.error import URLError
    from urllib.parse import quote
    from urllib.request import urlopen, Request

from copy import copy

from ..mark_int import MarkInt
from .reporter import Reporter

DEFAULT_BOTO_SESSION_DURATION_IN_SEC = 3600
PIPELINE_AWS_ACCOUNT = "588736812464"
SUPPORTED_REGIONS = {"us-west-2"}
DEFAULT_REGION = "us-west-2"

logger = logging.getLogger(__name__)


class ReportingPrecision(Enum):
    HOURS = "h"
    MINUTES = "m"
    SECONDS = "s"
    MILLISECONDS = "ms"
    MICROSECONDS = "u"
    NANOSECONDS = "ns"


class AsyncInfluxReporter(Reporter):
    """
    InfluxDB reporter using native http api
    (based on https://influxdb.com/docs/v1.1/guides/writing_data.html)
    """

    def __init__(
        self,
        registry=None,
        reporting_interval=5,
        prefix="",
        database="",
        region=DEFAULT_REGION,
        clock=None,
        global_tags=None,
        reporting_precision=ReportingPrecision.SECONDS,
        boto_sts_client: Optional[client] = None,
        async_influx_reporter_role_arn: str = "",
        external_id: str = "",
    ):
        """
        :param reporting_precision: The precision in which the reporter reports to influx.
        The default is seconds. This is a tradeoff between precision and performance. More
        coarse precision may result in significant improvements in compression and vice versa.
        """
        super(AsyncInfluxReporter, self).__init__(registry, reporting_interval, clock)
        self.region = region
        self.database = database
        self.prefix = prefix

        if global_tags is None:
            self.global_tags = {}
        else:
            self.global_tags = global_tags

        self.reporting_precision = reporting_precision
        self.sqs_client = self._get_sqs_client(
            region=self.region,
            role_arn=async_influx_reporter_role_arn,
            external_id=external_id,
            boto_sts_client=boto_sts_client,
        )

    def report_now(self, registry=None, timestamp=None):
        timestamp = timestamp or self.clock.time()
        timestamp_in_reporting_precision = _to_timestamp_in_precision(
            timestamp=timestamp, precision=self.reporting_precision
        )
        metrics = (registry or self.registry).dump_metrics(key_is_metric=True)

        lines = self._get_influx_protocol_lines(metrics, timestamp_in_reporting_precision)
        message = self._format_sqs_message(lines)
        self.sqs_client.send_message(
            QueueUrl=self._get_queue_url(self.region), MessageBody=json.dumps(message)
        )

    def _get_table_name(self, metric_key):
        if not self.prefix:
            return metric_key
        else:
            return "%s.%s" % (self.prefix, metric_key)

    def _get_influx_protocol_lines(self, metrics, timestamp):
        lines = []
        for key, metric_values in metrics.items():
            metric_name = key.get_key()
            table = self._get_table_name(metric_name)
            values = AsyncInfluxReporter._stringify_values(metric_values)
            tags = self._stringify_tags(key)

            # there's a special case where only events are present, which are skipped by
            # _stringify_values function
            if values:
                line = "%s%s %s %s" % (table, tags, values, timestamp)
                lines.append(line)

            for event in metric_values.get("events", []):
                values = AsyncInfluxReporter._stringify_values(event.values)

                event_timestamp = _to_timestamp_in_precision(
                    timestamp=event.time, precision=self.reporting_precision
                )
                line = "%s%s %s %s" % (table, tags, values, event_timestamp)

                lines.append(line)

        return lines

    @staticmethod
    def _stringify_values(metric_values):
        return ",".join(
            [
                "%s=%s" % (k, _format_field_value(v))
                for (k, v) in iteritems(metric_values)
                if k != "tags" and k != "events"
            ]
        )

    def _stringify_tags(self, metric):
        # start with the global reporter tags
        # (copy to avoid mutating to global values)
        all_tags = copy(self.global_tags)

        # add the local tags on top of those
        tags = metric.get_tags()
        all_tags.update(tags)

        if all_tags:
            return "," + ",".join(
                ["%s=%s" % (k, _format_tag_value(v)) for (k, v) in iteritems(all_tags)]
            )

        return ""

    @staticmethod
    def _get_sqs_client(
        region: str, role_arn: str, external_id: str, boto_sts_client: Optional[client] = None
    ) -> client:
        sts_client = boto_sts_client or client(
            service_name="sts",
            config=Config(
                connect_timeout=5,
                read_timeout=5,
                retries={"max_attempts": 5, "mode": "standard"},
                tcp_keepalive=True,
            ),
        )
        session_name = str(uuid4())
        assume_role_kwargs = {
            "RoleArn": role_arn,
            "RoleSessionName": session_name,
            "DurationSeconds": DEFAULT_BOTO_SESSION_DURATION_IN_SEC,
        }

        if external_id:
            assume_role_kwargs["ExternalId"] = external_id

        response = sts_client.assume_role(**assume_role_kwargs)
        credentials = response["Credentials"]
        assumed_role_session = Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region,
        )
        return assumed_role_session.client("sqs")

    def _format_sqs_message(self, lines: List[str]) -> Dict[str, str]:
        return {
            "database": self.database,
            "precision": self.reporting_precision.value,
            "reports": "\n".join(lines),
        }

    def _get_queue_url(self, region: str) -> str:
        if region not in SUPPORTED_REGIONS:
            region = DEFAULT_REGION

        return (
            f"https://sqs.{region}.amazonaws.com/"
            f"{PIPELINE_AWS_ACCOUNT}/px-poc-async-influx-reporter-queue"
        )


def _to_timestamp_in_precision(timestamp: float, precision: ReportingPrecision) -> int:
    if precision == ReportingPrecision.HOURS:
        return int(timestamp / 60 / 60)

    if precision == ReportingPrecision.MINUTES:
        return int(timestamp / 60)

    if precision == ReportingPrecision.SECONDS:
        return int(timestamp)

    if precision == ReportingPrecision.MILLISECONDS:
        return int(timestamp * 1e3)

    if precision == ReportingPrecision.MICROSECONDS:
        return int(timestamp * 1e6)

    if precision == ReportingPrecision.NANOSECONDS:
        return int(timestamp * 1e9)

    raise Exception("Unsupported ReportingPrecision")


def _format_field_value(value):
    if isinstance(value, MarkInt):
        return f"{value.value}i"
    if type(value) is not str:
        return value
    else:
        return '"{}"'.format(value)


def _format_tag_value(value):
    if type(value) is not str:
        return value
    else:
        # Escape special characters
        return re.sub("([ ,=])", r"\\\1", value)
