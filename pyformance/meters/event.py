import copy
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict

from .base_metric import BaseMetric


@dataclass
class EventPoint:
    time: float
    values: Dict[str, Any]


class Event(BaseMetric):
    """
    Report events as specific data points in specific timestamps

    This meter is outside of DropWizard's models and is here to support a specific use case of
    infrequently running cron like operations that trigger once in a while, do a bunch of work
    and dump the metric results for a single timestamp. Unlike all the other meter types, this one
    doesn't repeat itself if no activity occurs leading you to think everything is running
    constantly and producing data when it is not.

    The closest you can get to the same effect without this class is by using a Gauge, setting the
    value, invoking report_now, than clearing it right after.
    Since those operations above are not within a lock shared by scheduled reporters , it can still
    report the gauge twice.

    Additionally when using gauges you don't have any control over the name of the field writen to
    (just metric name and tags), and can't write a bunch of
    values at once but resort to writing values to separate Gauges which will make the lack of
    lock condition more likely to be an issue.

    Another problem that will pop in such usage is that the metric will still be written, it will
    just be written with the initial value of 0, so you won't be able to tell when was the last
    successful run with ease.
    """

    def __init__(self, clock, key, tags=None):
        super(Event, self).__init__(key, tags)
        self.lock = Lock()
        self.points = []
        self.clock = clock

    def add(self, values: Dict[str, Any]):
        with self.lock:
            self.points.append(EventPoint(
                time=self.clock.time(),
                values=values
            ))

    def clear(self):
        with self.lock:
            self.points = []

    def get_events(self):
        with self.lock:
            return copy.copy(self.points)
