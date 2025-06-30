"""
Performance monitoring and metrics collection.

Demonstrates real-time monitoring of:
- Garbage collection statistics
- Memory usage (RSS, VMS)
- GC pause histograms
- HTTP metrics endpoint
"""

import asyncio
import gc
import json
import time
import statistics
from collections import deque
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import psutil
import aiohttp
from aiohttp import web


@dataclass
class GCMetrics:
    """Garbage collection metrics."""
    collections: int
    objects_collected: int
    pause_time_ms: float
    timestamp: float


@dataclass
class MemoryMetrics:
    """Memory usage metrics."""
    rss_mb: float
    vms_mb: float
    cpu_percent: float
    timestamp: float


@dataclass
class PerformanceMetrics:
    """Aggregated performance metrics."""
    gc_collections: int
    gc_pause_p99_ms: float
    gc_pause_p95_ms: float
    gc_pause_avg_ms: float
    memory_rss_mb: float
    memory_vms_mb: float
    cpu_percent: float
    uptime_seconds: float
    ticks_processed: int
    vwap_latency_p99_ms: float
    storage_latency_p99_ms: float
    risk_latency_p99_ms: float


class PerformanceMonitor:
    """
    Real-time performance monitor with GC and memory tracking.
    
    This demonstrates continuous monitoring of system performance
    and garbage collection behavior.
    """
    
    def __init__(self, sample_interval: float = 1.0, history_size: int = 1000):
        self.sample_interval = sample_interval
        self.history_size = history_size
        
        # Metrics storage
        self.gc_history: deque = deque(maxlen=history_size)
        self.memory_history: deque = deque(maxlen=history_size)
        self.latency_history: Dict[str, deque] = {
            'vwap': deque(maxlen=history_size),
            'storage': deque(maxlen=history_size),
            'risk': deque(maxlen=history_size)
        }
        
        # Statistics
        self.start_time = time.time()
        self.total_ticks_processed = 0
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Get initial GC stats
        self.last_gc_stats = gc.get_stats()
        self.last_gc_time = time.time()
    
    async def start(self):
        """Start the monitoring task."""
        if self.is_running:
            return
        
        self.is_running = True
        self.monitoring_task = asyncio.create_task(self._monitor_loop())
        print("Performance monitoring started")
    
    async def stop(self):
        """Stop the monitoring task."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        print("Performance monitoring stopped")
    
    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self.is_running:
            try:
                self._collect_metrics()
                await asyncio.sleep(self.sample_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Monitoring error: {e}")
                await asyncio.sleep(self.sample_interval)
    
    def _collect_metrics(self):
        """Collect current metrics."""
        # Get current time
        current_time = time.time()
        
        # Collect GC metrics
        current_gc_stats = gc.get_stats()
        gc_metrics = self._calculate_gc_metrics(current_gc_stats, self.last_gc_stats, current_time)
        if gc_metrics:
            self.gc_history.append(gc_metrics)
        
        self.last_gc_stats = current_gc_stats
        self.last_gc_time = current_time
        
        # Collect memory metrics
        memory_metrics = self._collect_memory_metrics(current_time)
        self.memory_history.append(memory_metrics)
    
    def _calculate_gc_metrics(self, current_stats: List[Dict], last_stats: List[Dict], timestamp: float) -> Optional[GCMetrics]:
        """Calculate GC metrics between two snapshots."""
        if not last_stats:
            return None
        
        # Calculate differences
        total_collections = sum(stat['collections'] for stat in current_stats)
        last_total_collections = sum(stat['collections'] for stat in last_stats)
        collections_diff = total_collections - last_total_collections
        
        if collections_diff == 0:
            return None
        
        # Calculate pause time
        total_pause_time = 0.0
        total_objects = 0
        
        for i, (current, last) in enumerate(zip(current_stats, last_stats)):
            collections_diff_gen = current['collections'] - last['collections']
            if collections_diff_gen > 0:
                # Estimate pause time (GC stats don't always include exact timing)
                avg_pause = current.get('avg_time', 0.001)  # Default 1ms
                total_pause_time += collections_diff_gen * avg_pause
                total_objects += current.get('objects', 0) - last.get('objects', 0)
        
        return GCMetrics(
            collections=collections_diff,
            objects_collected=total_objects,
            pause_time_ms=total_pause_time * 1000,  # Convert to milliseconds
            timestamp=timestamp
        )
    
    def _collect_memory_metrics(self, timestamp: float) -> MemoryMetrics:
        """Collect current memory usage metrics."""
        process = psutil.Process()
        memory_info = process.memory_info()
        
        return MemoryMetrics(
            rss_mb=memory_info.rss / 1024 / 1024,
            vms_mb=memory_info.vms / 1024 / 1024,
            cpu_percent=process.cpu_percent(),
            timestamp=timestamp
        )
    
    def record_latency(self, component: str, latency_ms: float):
        """Record latency for a component."""
        if component in self.latency_history:
            self.latency_history[component].append(latency_ms)
    
    def increment_ticks(self, count: int = 1):
        """Increment the tick counter."""
        self.total_ticks_processed += count
    
    def get_current_metrics(self) -> PerformanceMetrics:
        """Get current aggregated metrics."""
        current_time = time.time()
        uptime = current_time - self.start_time
        
        # Calculate GC pause percentiles
        gc_pauses = [m.pause_time_ms for m in self.gc_history if m.pause_time_ms > 0]
        gc_pause_p99 = statistics.quantiles(gc_pauses, n=100)[-1] if len(gc_pauses) >= 100 else 0
        gc_pause_p95 = statistics.quantiles(gc_pauses, n=20)[-1] if len(gc_pauses) >= 20 else 0
        gc_pause_avg = statistics.mean(gc_pauses) if gc_pauses else 0
        
        # Get latest memory metrics
        latest_memory = self.memory_history[-1] if self.memory_history else MemoryMetrics(0, 0, 0, 0)
        
        # Calculate latency percentiles
        vwap_latencies = list(self.latency_history['vwap'])
        storage_latencies = list(self.latency_history['storage'])
        risk_latencies = list(self.latency_history['risk'])
        
        vwap_p99 = statistics.quantiles(vwap_latencies, n=100)[-1] if len(vwap_latencies) >= 100 else 0
        storage_p99 = statistics.quantiles(storage_latencies, n=100)[-1] if len(storage_latencies) >= 100 else 0
        risk_p99 = statistics.quantiles(risk_latencies, n=100)[-1] if len(risk_latencies) >= 100 else 0
        
        return PerformanceMetrics(
            gc_collections=sum(m.collections for m in self.gc_history),
            gc_pause_p99_ms=gc_pause_p99,
            gc_pause_p95_ms=gc_pause_p95,
            gc_pause_avg_ms=gc_pause_avg,
            memory_rss_mb=latest_memory.rss_mb,
            memory_vms_mb=latest_memory.vms_mb,
            cpu_percent=latest_memory.cpu_percent,
            uptime_seconds=uptime,
            ticks_processed=self.total_ticks_processed,
            vwap_latency_p99_ms=vwap_p99,
            storage_latency_p99_ms=storage_p99,
            risk_latency_p99_ms=risk_p99
        )
    
    def get_detailed_metrics(self) -> Dict:
        """Get detailed metrics for debugging."""
        current_metrics = self.get_current_metrics()
        
        return {
            'current': asdict(current_metrics),
            'gc_history_count': len(self.gc_history),
            'memory_history_count': len(self.memory_history),
            'latency_history_counts': {
                component: len(history) 
                for component, history in self.latency_history.items()
            },
            'recent_gc_pauses': [
                {'pause_ms': m.pause_time_ms, 'timestamp': m.timestamp}
                for m in list(self.gc_history)[-10:]
            ],
            'recent_memory_usage': [
                {'rss_mb': m.rss_mb, 'timestamp': m.timestamp}
                for m in list(self.memory_history)[-10:]
            ]
        }
    
    def print_status(self):
        """Print current status to console."""
        metrics = self.get_current_metrics()
        
        print(f"\n=== Performance Status ===")
        print(f"Uptime: {metrics.uptime_seconds:.1f}s")
        print(f"Ticks processed: {metrics.ticks_processed:,}")
        print(f"Memory RSS: {metrics.memory_rss_mb:.1f}MB")
        print(f"CPU: {metrics.cpu_percent:.1f}%")
        print(f"GC collections: {metrics.gc_collections}")
        print(f"GC pause P99: {metrics.gc_pause_p99_ms:.2f}ms")
        print(f"VWAP latency P99: {metrics.vwap_latency_p99_ms:.2f}ms")
        print(f"Storage latency P99: {metrics.storage_latency_p99_ms:.2f}ms")
        print(f"Risk latency P99: {metrics.risk_latency_p99_ms:.2f}ms")


class MetricsServer:
    """
    HTTP metrics server for Prometheus-style monitoring.
    
    This demonstrates exposing metrics via HTTP for external monitoring.
    """
    
    def __init__(self, monitor: PerformanceMonitor, port: int = 8080):
        self.monitor = monitor
        self.port = port
        self.app = web.Application()
        self.runner: Optional[web.AppRunner] = None
        
        # Setup routes
        self.app.router.add_get('/metrics', self.metrics_handler)
        self.app.router.add_get('/health', self.health_handler)
        self.app.router.add_get('/detailed', self.detailed_handler)
    
    async def start(self):
        """Start the metrics server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        site = web.TCPSite(self.runner, 'localhost', self.port)
        await site.start()
        
        print(f"Metrics server running at http://localhost:{self.port}")
    
    async def stop(self):
        """Stop the metrics server."""
        if self.runner:
            await self.runner.cleanup()
    
    async def metrics_handler(self, request):
        """Handle /metrics endpoint (Prometheus-style)."""
        metrics = self.monitor.get_current_metrics()
        
        # Format as Prometheus metrics
        prometheus_metrics = [
            f"# HELP quants_ticks_processed Total number of ticks processed",
            f"# TYPE quants_ticks_processed counter",
            f"quants_ticks_processed {metrics.ticks_processed}",
            "",
            f"# HELP quants_memory_rss_mb Memory usage in MB",
            f"# TYPE quants_memory_rss_mb gauge",
            f"quants_memory_rss_mb {metrics.memory_rss_mb}",
            "",
            f"# HELP quants_cpu_percent CPU usage percentage",
            f"# TYPE quants_cpu_percent gauge",
            f"quants_cpu_percent {metrics.cpu_percent}",
            "",
            f"# HELP quants_gc_pause_p99_ms 99th percentile GC pause time",
            f"# TYPE quants_gc_pause_p99_ms gauge",
            f"quants_gc_pause_p99_ms {metrics.gc_pause_p99_ms}",
            "",
            f"# HELP quants_vwap_latency_p99_ms 99th percentile VWAP latency",
            f"# TYPE quants_vwap_latency_p99_ms gauge",
            f"quants_vwap_latency_p99_ms {metrics.vwap_latency_p99_ms}",
            "",
            f"# HELP quants_storage_latency_p99_ms 99th percentile storage latency",
            f"# TYPE quants_storage_latency_p99_ms gauge",
            f"quants_storage_latency_p99_ms {metrics.storage_latency_p99_ms}",
            "",
            f"# HELP quants_risk_latency_p99_ms 99th percentile risk latency",
            f"# TYPE quants_risk_latency_p99_ms gauge",
            f"quants_risk_latency_p99_ms {metrics.risk_latency_p99_ms}"
        ]
        
        return web.Response(
            text='\n'.join(prometheus_metrics),
            content_type='text/plain'
        )
    
    async def health_handler(self, request):
        """Handle /health endpoint."""
        return web.json_response({
            'status': 'healthy',
            'uptime_seconds': self.monitor.get_current_metrics().uptime_seconds,
            'ticks_processed': self.monitor.get_current_metrics().ticks_processed
        })
    
    async def detailed_handler(self, request):
        """Handle /detailed endpoint for detailed metrics."""
        detailed_metrics = self.monitor.get_detailed_metrics()
        return web.json_response(detailed_metrics)


class GCController:
    """
    Garbage collection controller for performance tuning.
    
    This demonstrates GC tuning techniques like gc.freeze().
    """
    
    def __init__(self, monitor: PerformanceMonitor):
        self.monitor = monitor
        self.gc_enabled = True
        self.freeze_after_warmup = False
        self.warmup_ticks = 10000
        self.warmup_complete = False
    
    def enable_gc(self):
        """Enable garbage collection."""
        gc.enable()
        self.gc_enabled = True
        print("Garbage collection enabled")
    
    def disable_gc(self):
        """Disable garbage collection."""
        gc.disable()
        self.gc_enabled = False
        print("Garbage collection disabled")
    
    def freeze_gc(self):
        """Freeze garbage collection (Python 3.7+)."""
        try:
            gc.freeze()
            print("Garbage collection frozen")
        except AttributeError:
            print("gc.freeze() not available in this Python version")
    
    def unfreeze_gc(self):
        """Unfreeze garbage collection."""
        try:
            gc.unfreeze()
            print("Garbage collection unfrozen")
        except AttributeError:
            print("gc.unfreeze() not available in this Python version")
    
    def set_thresholds(self, gen0: int = 700, gen1: int = 10, gen2: int = 10):
        """Set garbage collection thresholds."""
        gc.set_threshold(gen0, gen1, gen2)
        print(f"GC thresholds set to: gen0={gen0}, gen1={gen1}, gen2={gen2}")
    
    def check_warmup(self):
        """Check if warmup is complete and apply optimizations."""
        if not self.warmup_complete and self.monitor.total_ticks_processed >= self.warmup_ticks:
            self.warmup_complete = True
            print(f"Warmup complete after {self.warmup_ticks} ticks")
            
            if self.freeze_after_warmup:
                self.freeze_gc()
    
    def get_gc_stats(self) -> Dict:
        """Get current garbage collection statistics."""
        return {
            'enabled': self.gc_enabled,
            'counts': gc.get_count(),
            'thresholds': gc.get_threshold(),
            'stats': gc.get_stats(),
            'warmup_complete': self.warmup_complete,
            'freeze_after_warmup': self.freeze_after_warmup
        }


if __name__ == "__main__":
    # Demo: Test monitoring
    async def demo():
        # Create monitor
        monitor = PerformanceMonitor(sample_interval=0.5)
        await monitor.start()
        
        # Create metrics server
        server = MetricsServer(monitor, port=8080)
        await server.start()
        
        try:
            # Simulate some activity
            for i in range(100):
                monitor.increment_ticks(100)
                monitor.record_latency('vwap', 0.001 + i * 0.0001)
                monitor.record_latency('storage', 0.002 + i * 0.0001)
                monitor.record_latency('risk', 0.005 + i * 0.0001)
                
                # Print status every 10 iterations
                if i % 10 == 0:
                    monitor.print_status()
                
                await asyncio.sleep(0.1)
            
            print("\nFinal metrics:")
            monitor.print_status()
            
            # Test GC controller
            gc_controller = GCController(monitor)
            gc_controller.set_thresholds(1000, 10, 10)
            
        finally:
            await monitor.stop()
            await server.stop()
    
    asyncio.run(demo()) 