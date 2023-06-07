try:
    import mock
except ImportError:
    from unittest import mock

try:
    from urllib2 import Request
except ImportError:
    from urllib.request import Request

from pyformance import MetricsRegistry, MarkInt
from pyformance.reporters.influx import InfluxReporter, _format_tag_value
from tests import TimedTestCase


class TestInfluxReporter(TimedTestCase):
    def setUp(self):
        super(TestInfluxReporter, self).setUp()
        self.registry = MetricsRegistry()

    def tearDown(self):
        super(TestInfluxReporter, self).tearDown()

    def test_not_called_on_blank(self):
        influx_reporter = InfluxReporter(registry=self.registry)

        with mock.patch("pyformance.reporters.influx.urlopen") as patch:
            influx_reporter.report_now()
            patch.assert_not_called()

    def test_create_database(self):
        r1 = InfluxReporter(registry=self.registry, autocreate_database=True)
        with mock.patch("pyformance.reporters.influx.urlopen") as patch:
            r1.report_now()
            if patch.call_count != 1:
                raise AssertionError(
                    "Expected 1 calls to 'urlopen'. Received: {}".format(
                        patch.call_count
                    )
                )

    def test_gauge_without_tags(self):
        self.registry.gauge("cpu").set_value(65)
        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = "cpu value=65 " + self.clock.time_string()
            send_mock.assert_called_once_with(expected_url, expected_data)

    def test_gauge_with_tags(self):
        tags = {"region": "us - west"}
        self.registry.gauge(key="cpu", tags=tags).set_value(65)
        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = "cpu,region=us\\ -\\ west value=65 " + \
                            self.clock.time_string()
            send_mock.assert_called_once_with(expected_url, expected_data)

    def test_gauge_with_global_tags(self):
        tags = {"region": "us-west-2"}
        self.registry.gauge(key="cpu", tags=tags).set_value(65)
        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False,
            global_tags={"stage": "dev", "region": "override"}
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = "cpu,stage=dev,region=us-west-2 value=65 " + \
                            self.clock.time_string()
            send_mock.assert_called_once_with(expected_url, expected_data)

    def test_counter_with_tags(self):
        tags = {"host": "server1"}
        counter = self.registry.counter(key="cpu", tags=tags)

        for i in range(5):
            counter.inc(1)

        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = "cpu,host=server1 count=5 " + \
                            self.clock.time_string()
            send_mock.assert_called_once_with(expected_url, expected_data)

    def test_events_with_tags(self):
        tags = {"host": "server1"}
        self.registry._clock = self.clock
        event = self.registry.event(key="event", tags=tags)

        event.add({"field": 1, "float": 0.12, "int": MarkInt(1)})

        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = "event,host=server1 field=1,float=0.12,int=1i " + \
                            self.clock.time_string()
            send_mock.assert_called_once_with(expected_url, expected_data)

    def test_combined_events_with_counter(self):
        tags = {"host": "server1"}
        self.registry._clock = self.clock
        event = self.registry.event(key="event", tags=tags)

        event.add({"field": 1})

        counter = self.registry.counter("event", tags=tags)
        counter.inc(5)

        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = [
                "event,host=server1 count=5 " + self.clock.time_string(),
                "event,host=server1 field=1 " + self.clock.time_string()
            ]

            send_mock.assert_called_once_with(expected_url, "\n".join(expected_data))

    def test_count_calls_with_tags(self):
        tags = {"host": "server1"}
        counter = self.registry.counter(key="cpu", tags=tags)

        for i in range(5):
            counter.inc(1)

        influx_reporter = InfluxReporter(
            registry=self.registry,
            clock=self.clock,
            autocreate_database=False
        )

        with mock.patch.object(influx_reporter, "_try_send") as send_mock:
            influx_reporter.report_now()

            expected_url = "http://127.0.0.1:8086/write?db=metrics&precision=s&rp=autogen"
            expected_data = "cpu,host=server1 count=5 " + \
                            self.clock.time_string()
            send_mock.assert_called_once_with(expected_url, expected_data)

    def test__format_tag_value(self):
        self.assertEqual(_format_tag_value("no_special_chars"), "no_special_chars")
        self.assertEqual(_format_tag_value("has space"), "has\\ space")
        self.assertEqual(_format_tag_value("has,comma"), "has\\,comma")
        self.assertEqual(_format_tag_value("has=equals"), "has\\=equals")
