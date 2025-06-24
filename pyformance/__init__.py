# Maintain backwards compatibility while avoiding deprecation warnings
import sys
if sys.version_info < (3, 3):
    # Only use pkg_resources for Python < 3.3 which doesn't support PEP 420
    try:
        __import__("pkg_resources").declare_namespace(__name__)
    except ImportError:
        pass
# For Python 3.3+, namespace packages work automatically (PEP 420)

from .registry import MetricsRegistry, global_registry, set_global_registry
from .registry import timer, counter, meter, histogram, gauge, event
from .registry import dump_metrics, clear
from .decorators import count_calls, meter_calls, hist_calls, time_calls
from .meters.timer import call_too_long
from .mark_int import MarkInt
