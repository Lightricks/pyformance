# Copyright (c) 2023 Lightricks. All rights reserved.
from enum import Enum


class ReportingPrecision(Enum):
    HOURS = "h"
    MINUTES = "m"
    SECONDS = "s"
    MILLISECONDS = "ms"
    MICROSECONDS = "u"
    NANOSECONDS = "ns"

def to_timestamp_in_precision(timestamp: float, precision: ReportingPrecision) -> int:
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
