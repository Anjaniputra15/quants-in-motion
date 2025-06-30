"""
Quants-in-Motion: Async market data pump with GIL-aware concurrency.

A self-contained repository showcasing async I/O, GIL-aware concurrency,
memory-safe design, and performance tuning in a realistic trading application.
"""

__version__ = "1.0.0"
__author__ = "Quants-in-Motion Team"

from .feed import Tick, MarketDataFeed
from .vwap import VWAPCalculator
from .store import TickStore
from .risk import RiskService
from .pools import TickPool
from .monitor import PerformanceMonitor

__all__ = [
    "Tick",
    "MarketDataFeed", 
    "VWAPCalculator",
    "TickStore",
    "RiskService",
    "TickPool",
    "PerformanceMonitor"
] 