# -*- coding: utf-8 -*-
import datetime
import os

from .reporter import Reporter


class CsvReporter(Reporter):
    """
    Show metrics in comma-separated-files.
    Each metrics gets its own file

    This reporter is too primitive to support events because a single event
    can have multiple fields, as it stands right now the values of the events will interleave
    making the output completely useless.

    For this reason events are ignored from the output of this reporter.
    """

    def __init__(
            self,
            registry=None,
            reporting_interval=30,
            path=None,
            separator="\t",
            clock=None,
    ):
        super(CsvReporter, self).__init__(registry, reporting_interval, clock)
        self.path = path or os.getcwd()
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.separator = separator
        self.files = {}

    def report_now(self, registry=None, timestamp=None):
        self._save_metrics(registry or self.registry, timestamp)

    def _save_metrics(self, registry, timestamp=None):
        timestamp = timestamp or int(round(self.clock.time()))
        dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=timestamp)
        date = dt.strftime("%Y-%m-%d %H:%M:%S")
        metrics = registry.dump_metrics(key_is_metric=True)
        for key in metrics.keys():
            values = metrics[key]
            values["tags"] = key.tags
            value_keys = list(sorted(values.keys()))
            target = os.path.join(self.path, "%s.csv" % key.key)
            f = self.files.get(target, None)
            if f is None:
                if not os.path.exists(target):
                    f = open(target, "w")
                    f.write("%s\n" % self.separator.join(["timestamp"] + value_keys))
                else:
                    f = open(target, "a")
                self.files[target] = f
            cols = [date]
            for vk in value_keys:
                cols.append(values[vk])
            f.write("%s\n" % self.separator.join(map(str, cols)))
            f.flush()


    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for f in self.files.values():
            f.close()
