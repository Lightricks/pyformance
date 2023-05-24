import os
import shutil
import tempfile

from pyformance import MetricsRegistry
from pyformance.reporters.csv_reporter import CsvReporter
from tests import ManualClock, TimedTestCase


class TestCsvReporter(TimedTestCase):
    def setUp(self):
        super(TestCsvReporter, self).setUp()
        self.clock = ManualClock()
        self.path = tempfile.mktemp()
        self.registry = MetricsRegistry(clock=self.clock)
        self.maxDiff = None

    def tearDown(self):
        super(TestCsvReporter, self).tearDown()
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def test_report_now(self):
        g1 = self.registry.gauge("gauge1")
        g1.set_value(123)

        # CSV reporter is extremely simple, doesn't have a metric name or
        # field name definition, meaning it'll be useless for tracking events that may have several
        # different values for different fields for the same timestamp

        # So expected behaviour here is just ignore the events, doubt anyone will find such simple
        # implementation useful in any case.
        e1 = self.registry.event("e1")
        e1.add({"field": 1})

        with CsvReporter(
                registry=self.registry,
                reporting_interval=1,
                clock=self.clock,
                path=self.path,
        ) as r:
            r.report_now()

        output_filename = os.path.join(self.path, "gauge1.csv")

        output = open(output_filename).read()
        self.assertEqual(
            output.splitlines(), ["timestamp\ttags\tvalue", "1970-01-01 00:00:00\t{}\t123"]
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
