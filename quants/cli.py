"""
Main CLI module for Quants-in-Motion.

Orchestrates all components and provides the command-line interface.
"""

import asyncio
import signal
import sys
import time
from typing import Optional
import click
import uvloop

from .feed import MarketDataFeed, start_mock_server
from .vwap import VWAPCalculator
from .store import TickStore, AsyncTickProcessor
from .risk import RiskService, AsyncRiskProcessor
from .pools import TickPool
from .monitor import PerformanceMonitor, MetricsServer, GCController


class QuantsApplication:
    """
    Main application that orchestrates all components.
    
    This demonstrates how to coordinate async I/O, multiprocessing,
    thread pools, and monitoring in a real application.
    """
    
    def __init__(self, config: dict):
        self.config = config
        self.running = False
        
        # Components
        self.feed: Optional[MarketDataFeed] = None
        self.vwap_calculator: Optional[VWAPCalculator] = None
        self.tick_store: Optional[TickStore] = None
        self.tick_processor: Optional[AsyncTickProcessor] = None
        self.risk_service: Optional[RiskService] = None
        self.risk_processor: Optional[AsyncRiskProcessor] = None
        self.tick_pool: Optional[TickPool] = None
        self.monitor: Optional[PerformanceMonitor] = None
        self.metrics_server: Optional[MetricsServer] = None
        self.gc_controller: Optional[GCController] = None
        
        # Mock server
        self.mock_server = None
        self.mock_runner = None
        
        # Statistics
        self.start_time = time.time()
        self.total_ticks_processed = 0
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()
    
    async def initialize(self):
        """Initialize all components."""
        print("Initializing Quants-in-Motion...")
        
        # Initialize performance monitor
        self.monitor = PerformanceMonitor(
            sample_interval=self.config.get('monitor_interval', 1.0)
        )
        await self.monitor.start()
        
        # Initialize GC controller
        self.gc_controller = GCController(self.monitor)
        if self.config.get('freeze_gc'):
            self.gc_controller.freeze_after_warmup = True
        
        # Initialize object pool
        self.tick_pool = TickPool(
            max_size=self.config.get('pool_size', 1000),
            enable_ref_cycles=self.config.get('enable_ref_cycles', False)
        )
        
        # Initialize VWAP calculator
        self.vwap_calculator = VWAPCalculator(
            window_size=self.config.get('vwap_window', 100),
            use_multiprocessing=self.config.get('cores', 1) > 1,
            num_cores=self.config.get('cores', 1)
        )
        
        # Initialize tick store
        self.tick_store = TickStore(
            log_file=self.config.get('log_file', 'ticks.log')
        )
        self.tick_processor = AsyncTickProcessor(
            self.tick_store,
            batch_size=self.config.get('batch_size', 100)
        )
        
        # Initialize risk service
        self.risk_service = RiskService(
            max_workers=self.config.get('risk_workers', 4)
        )
        self.risk_processor = AsyncRiskProcessor(
            self.risk_service,
            batch_size=self.config.get('risk_batch_size', 50)
        )
        
        # Start mock WebSocket server if needed
        if self.config.get('start_mock_server', True):
            self.mock_server, self.mock_runner = await start_mock_server(
                tick_rate=self.config.get('tick_rate', 1000)
            )
        
        # Initialize market data feed
        self.feed = MarketDataFeed()
        await self.feed.connect()
        
        # Initialize metrics server
        self.metrics_server = MetricsServer(
            self.monitor,
            port=self.config.get('metrics_port', 8080)
        )
        await self.metrics_server.start()
        
        print("Initialization complete!")
        print(f"Tick rate: {self.config.get('tick_rate', 1000)}/sec")
        print(f"CPU cores: {self.config.get('cores', 1)}")
        print(f"Metrics: http://localhost:{self.config.get('metrics_port', 8080)}/metrics")
    
    async def shutdown(self):
        """Shutdown all components."""
        print("\nShutting down Quants-in-Motion...")
        
        self.running = False
        
        # Stop components in reverse order
        if self.metrics_server:
            await self.metrics_server.stop()
        
        if self.feed:
            await self.feed.disconnect()
        
        if self.mock_runner:
            await self.mock_server.stop(self.mock_runner)
        
        if self.risk_processor:
            await self.risk_processor.flush()
        
        if self.tick_processor:
            await self.tick_processor.close()
        
        if self.vwap_calculator:
            await self.vwap_calculator.__aexit__(None, None, None)
        
        if self.risk_service:
            await self.risk_service.__aexit__(None, None, None)
        
        if self.monitor:
            await self.monitor.stop()
        
        print("Shutdown complete!")
    
    async def process_tick(self, tick):
        """
        Process a single tick through all pipelines.
        
        This demonstrates the fan-out pattern where each tick
        flows through multiple processing paths concurrently.
        """
        start_time = time.perf_counter()
        
        # Fan out to all processing pipelines concurrently
        tasks = [
            self._process_vwap(tick),
            self._process_storage(tick),
            self._process_risk(tick)
        ]
        
        # Wait for all pipelines to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Record latencies
        end_time = time.perf_counter()
        total_latency = (end_time - start_time) * 1000
        
        # Update statistics
        self.total_ticks_processed += 1
        self.monitor.increment_ticks()
        
        # Handle results
        vwap_result, storage_result, risk_alerts = results
        
        # Record component latencies
        if isinstance(vwap_result, Exception):
            print(f"VWAP error: {vwap_result}")
        elif vwap_result:
            self.monitor.record_latency('vwap', vwap_result.latency_ms)
        
        if isinstance(storage_result, Exception):
            print(f"Storage error: {storage_result}")
        elif storage_result:
            self.monitor.record_latency('storage', total_latency)
        
        if isinstance(risk_alerts, Exception):
            print(f"Risk error: {risk_alerts}")
        elif risk_alerts:
            self.monitor.record_latency('risk', total_latency)
        
        return results
    
    async def _process_vwap(self, tick):
        """Process tick through VWAP calculator."""
        return await self.vwap_calculator.process_tick(tick)
    
    async def _process_storage(self, tick):
        """Process tick through storage pipeline."""
        await self.tick_processor.process_tick(tick)
        return True
    
    async def _process_risk(self, tick):
        """Process tick through risk service."""
        return await self.risk_processor.process_tick(tick)
    
    async def run(self):
        """Main application loop."""
        print("Starting Quants-in-Motion...")
        print("Press Ctrl+C to stop")
        
        self.running = True
        
        # Set up signal handlers
        def signal_handler(signum, frame):
            print(f"\nReceived signal {signum}, shutting down...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Status printing task
        status_task = asyncio.create_task(self._status_loop())
        
        try:
            # Main processing loop
            async for tick in self.feed.ticks():
                if not self.running:
                    break
                
                await self.process_tick(tick)
                
                # Check for warmup completion
                if self.gc_controller:
                    self.gc_controller.check_warmup()
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error in main loop: {e}")
        finally:
            status_task.cancel()
            try:
                await status_task
            except asyncio.CancelledError:
                pass
    
    async def _status_loop(self):
        """Periodic status printing loop."""
        while self.running:
            try:
                await asyncio.sleep(5.0)  # Print status every 5 seconds
                if self.running:
                    self._print_status()
            except asyncio.CancelledError:
                break
    
    def _print_status(self):
        """Print current application status."""
        runtime = time.time() - self.start_time
        
        print(f"\n=== Quants-in-Motion Status ===")
        print(f"Runtime: {runtime:.1f}s")
        print(f"Ticks processed: {self.total_ticks_processed:,}")
        print(f"Processing rate: {self.total_ticks_processed / runtime:.0f} ticks/sec")
        
        if self.monitor:
            self.monitor.print_status()
        
        if self.vwap_calculator:
            vwap_stats = self.vwap_calculator.get_statistics()
            print(f"VWAP symbols tracked: {vwap_stats['symbols_tracked']}")
        
        if self.risk_service:
            risk_stats = self.risk_service.get_statistics()
            print(f"Risk alerts: {risk_stats['total_alerts']}")
        
        if self.tick_pool:
            pool_stats = self.tick_pool.get_statistics()
            print(f"Object pool reuse rate: {pool_stats['reuse_rate']:.1%}")


@click.group()
def cli():
    """Quants-in-Motion: Async market data pump with GIL-aware concurrency."""
    pass


@cli.command()
@click.option('--tick-rate', default=1000, help='Ticks per second')
@click.option('--cores', default=1, help='Number of CPU cores for VWAP')
@click.option('--freeze-gc', is_flag=True, help='Freeze garbage collection after warmup')
@click.option('--pool-size', default=1000, help='Object pool size')
@click.option('--log-file', default='ticks.log', help='Log file path')
@click.option('--metrics-port', default=8080, help='Metrics server port')
@click.option('--batch-size', default=100, help='Storage batch size')
@click.option('--risk-workers', default=4, help='Risk service thread workers')
@click.option('--risk-batch-size', default=50, help='Risk processing batch size')
@click.option('--vwap-window', default=100, help='VWAP calculation window size')
@click.option('--monitor-interval', default=1.0, help='Monitoring sample interval')
@click.option('--enable-ref-cycles', is_flag=True, help='Enable reference cycles demo')
@click.option('--start-mock-server', default=True, help='Start mock WebSocket server')
def run(**kwargs):
    """Run the Quants-in-Motion application."""
    # Use uvloop for better performance
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    
    async def main():
        async with QuantsApplication(kwargs) as app:
            await app.run()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
    except Exception as e:
        print(f"Application error: {e}")
        sys.exit(1)


@cli.command()
@click.option('--tick-rate', default=100, help='Ticks per second for demo')
def demo(**kwargs):
    """Run a quick demo of the application."""
    print("Running Quants-in-Motion demo...")
    
    async def demo_main():
        # Use shorter intervals for demo
        demo_config = {
            'tick_rate': kwargs['tick_rate'],
            'cores': 2,
            'pool_size': 100,
            'batch_size': 10,
            'risk_workers': 2,
            'risk_batch_size': 5,
            'monitor_interval': 0.5,
            'start_mock_server': True
        }
        
        async with QuantsApplication(demo_config) as app:
            # Run for 30 seconds
            demo_task = asyncio.create_task(app.run())
            
            try:
                await asyncio.wait_for(demo_task, timeout=30.0)
            except asyncio.TimeoutError:
                print("\nDemo completed after 30 seconds")
                app.running = False
    
    asyncio.run(demo_main())


@cli.command()
def test():
    """Run performance tests."""
    print("Running performance tests...")
    
    async def test_main():
        # Test VWAP implementations
        from .vwap import compare_vwap_implementations
        from .feed import Tick
        import random
        
        # Generate test data
        test_ticks = [
            Tick('AAPL', 150.0 + i * 0.1, random.randint(100, 1000), time.time())
            for i in range(1000)
        ]
        
        print("Testing VWAP implementations...")
        vwap_comparison = await compare_vwap_implementations(test_ticks)
        print(f"VWAP speedup: {vwap_comparison['speedup']:.1f}x")
        
        # Test storage methods
        from .store import compare_storage_methods
        print("\nTesting storage methods...")
        storage_comparison = await compare_storage_methods(test_ticks)
        print(f"Storage speedup: {storage_comparison['individual_writes']['time_ms'] / storage_comparison['batch_writes']['time_ms']:.1f}x")
        
        # Test object pooling
        from .pools import compare_pooling_methods
        print("\nTesting object pooling...")
        pooling_comparison = await compare_pooling_methods(5000)
        print(f"Memory reduction: {pooling_comparison['no_pooling']['memory_growth_mb'] / max(pooling_comparison['with_pooling']['memory_growth_mb'], 0.1):.1f}x")
    
    asyncio.run(test_main())


if __name__ == "__main__":
    cli() 