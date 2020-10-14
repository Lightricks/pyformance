# -*- coding: utf-8 -*-

import base64
import logging
import re

from six import iteritems

try:
    from urllib2 import quote, urlopen, Request, URLError
except ImportError:
    from urllib.error import URLError
    from urllib.parse import quote
    from urllib.request import urlopen, Request

from .reporter import Reporter
from ..mark_int import MarkInt
from copy import copy

LOG = logging.getLogger(__name__)

DEFAULT_INFLUX_SERVER = "127.0.0.1"
DEFAULT_INFLUX_PORT = 8086
DEFAULT_INFLUX_DATABASE = "metrics"
DEFAULT_INFLUX_USERNAME = None
DEFAULT_INFLUX_PASSWORD = None
DEFAULT_INFLUX_PROTOCOL = "http"


class InfluxReporter(Reporter):
    """
    InfluxDB reporter using native http api
    (based on https://influxdb.com/docs/v1.1/guides/writing_data.html)
    """

    def __init__(
            self,
            registry=None,
            reporting_interval=5,
            prefix="",
            database=DEFAULT_INFLUX_DATABASE,
            server=DEFAULT_INFLUX_SERVER,
            username=DEFAULT_INFLUX_USERNAME,
            password=DEFAULT_INFLUX_PASSWORD,
            port=DEFAULT_INFLUX_PORT,
            protocol=DEFAULT_INFLUX_PROTOCOL,
            autocreate_database=False,
            clock=None,
            global_tags=None,
    ):
        super(InfluxReporter, self).__init__(registry, reporting_interval, clock)
        self.prefix = prefix
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.protocol = protocol
        self.server = server
        self.autocreate_database = autocreate_database
        self._did_create_database = False

        if global_tags is None:
            self.global_tags = {}
        else:
            self.global_tags = global_tags

    def _create_database(self):
        url = "%s://%s:%s/query" % (self.protocol, self.server, self.port)
        q = quote("CREATE DATABASE %s" % self.database)
        request = Request(url + "?q=" + q)
        if self.username:
            auth = _encode_username(self.username, self.password)
            request.add_header("Authorization", "Basic %s" % auth.decode("utf-8"))
        try:
            response = urlopen(request)
            _result = response.read()
            # Only set if we actually were able to get a successful response
            self._did_create_database = True
        except URLError as err:
            LOG.warning(
                "Cannot create database %s to %s: %s",
                self.database,
                self.server,
                err.reason,
            )

    def report_now(self, registry=None, timestamp=None):
        if self.autocreate_database and not self._did_create_database:
            self._create_database()
        timestamp = timestamp or int(round(self.clock.time()))
        metrics = (registry or self.registry).dump_metrics(key_is_metric=True)

        influx_lines = self._get_influx_protocol_lines(metrics, timestamp)
        # If you don't have anything nice to say than don't say nothing
        if influx_lines:
            post_data = "\n".join(influx_lines)
            LOG.error("Reporting now: \n%s", post_data)
            url = self._get_url()
            self._try_send(url, post_data)

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
            values = InfluxReporter._stringify_values(metric_values)
            tags = self._stringify_tags(key)

            # there's a special case where only events are present, which are skipped by
            # _stringify_values function
            if values:
                line = "%s%s %s %s" % (table, tags, values, timestamp)
                lines.append(line)

            for event in metric_values.get("events", []):
                values = InfluxReporter._stringify_values(event.values)

                line = "%s%s %s %s" % (
                    table,
                    tags,
                    values,
                    int(round(event.time))
                )

                lines.append(line)

        return lines

    @staticmethod
    def _stringify_values(metric_values):
        return ",".join(
            [
                "%s=%s" % (k, _format_field_value(v))
                for (k, v) in iteritems(metric_values) if k != "tags" and k != "events"
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
                [
                    "%s=%s" % (k, _format_tag_value(v))
                    for (k, v) in iteritems(all_tags)
                ]
            )

        return ""

    def _get_url(self):
        path = "/write?db=%s&precision=s" % self.database
        return "%s://%s:%s%s" % (self.protocol, self.server, self.port, path)

    def _add_auth_data(self, request):
        auth = _encode_username(self.username, self.password)
        request.add_header("Authorization", "Basic %s" % auth.decode('utf-8'))

    def _try_send(self, url, data):
        request = Request(url, data.encode("utf-8"))
        if self.username:
            self._add_auth_data(request)
        try:
            response = urlopen(request)
            response.read()
        except URLError as err:
            response = err.read().decode("utf-8")

            LOG.warning(
                "Cannot write to %s: %s ,url: %s, data: %s, response: %s",
                self.server,
                err.reason,
                url,
                data,
                response
            )


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


def _encode_username(username, password):
    auth_string = ("%s:%s" % (username, password)).encode()
    return base64.b64encode(auth_string)
