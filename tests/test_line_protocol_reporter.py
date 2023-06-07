# Copyright (c) 2023 Lightricks. All rights reserved.
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from pyformance import MetricsRegistry
from pyformance.reporters.line_protocol_reporter import LineProtocolReporter
from tests import ManualClock


class LineProtocolReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        super(LineProtocolReporterTests, self).setUp()
        self.clock = ManualClock()
        self.path = tempfile.mktemp()
        self.registry = MetricsRegistry(clock=self.clock)
        self.maxDiff = None

    def tearDown(self) -> None:
        super(LineProtocolReporterTests, self).tearDown()
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def test_report_now_writes_to_file(self) -> None:
        reporter = LineProtocolReporter(registry=self.registry, clock=self.clock, path=self.path)
        timestamp = 1234567890
        counter = reporter.registry.counter("test-counter")
        counter.inc()
        reporter.report_now(timestamp=timestamp)

        files = [f for f in Path(reporter.path).glob("*.txt")]
        self.assertEqual(1, len(files))
        expected_file_path = f"{reporter.path}/{files[0].name}"
        expected_lines = open(expected_file_path).read()
        self.assertEqual(expected_lines, "test-counter count=1 1234567890")

    def test_get_table_name_returns_metric_key_when_prefix_empty(self) -> None:
        reporter = LineProtocolReporter()
        table_name = reporter._get_table_name("metric_key")
        self.assertEqual(table_name, "metric_key")

    def test_get_table_name_returns_prefixed_metric_key_when_prefix_not_empty(self) -> None:
        reporter = LineProtocolReporter(prefix="prefix")
        table_name = reporter._get_table_name("metric_key")
        self.assertEqual(table_name, "prefix.metric_key")

    def test_stringify_values_returns_correct_string(self) -> None:
        metric_values = {"field1": 10, "field2": "value"}
        expected_string = "field1=10,field2=\"value\""
        stringified_values = LineProtocolReporter._stringify_values(metric_values)
        self.assertEqual(stringified_values, expected_string)

    def test_stringify_tags_returns_correct_string(self) -> None:
        metric = MagicMock()
        metric.get_tags.return_value = {"tag1": "value1", "tag2": "value2"}
        global_tags = {"global_tag": "global_value"}
        reporter = LineProtocolReporter(global_tags=global_tags)
        expected_string = ",global_tag=global_value,tag1=value1,tag2=value2"
        stringified_tags = reporter._stringify_tags(metric)
        self.assertEqual(stringified_tags, expected_string)
