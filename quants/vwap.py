"""
VWAP (Volume Weighted Average Price) calculator.

Demonstrates GIL bottlenecks and solutions:
1. Pure Python implementation (bottlenecks due to GIL)
2. NumPy vectorized implementation (releases GIL)
3. Multiprocessing for CPU-bound work
"""

import asyncio
import time
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Optional
from dataclasses import dataclass
from .feed import Tick


@dataclass
class VWAPResult:
    """VWAP calculation result."""
    symbol: str
    vwap: float
    total_volume: int
    tick_count: int
    latency_ms: float


class VWAPCalculator:
    """VWAP calculator with multiple implementation strategies."""
    
    def __init__(self, window_size: int = 100, use_multiprocessing: bool = False, num_cores: int = 1):
        self.window_size = window_size
        self.use_multiprocessing = use_multiprocessing
        self.num_cores = num_cores
        
        # In-memory tick storage per symbol
        self.tick_buffers: Dict[str, List[Tick]] = {}
        
        # Statistics
        self.total_ticks_processed = 0
        self.total_latency_ms = 0.0
        
        # Process pool for multiprocessing
        self.process_pool: Optional[ProcessPoolExecutor] = None
        if use_multiprocessing and num_cores > 1:
            self.process_pool = ProcessPoolExecutor(max_workers=num_cores)
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.process_pool:
            self.process_pool.shutdown(wait=True)
    
    def _add_tick_to_buffer(self, tick: Tick):
        """Add tick to the symbol's buffer, maintaining window size."""
        if tick.symbol not in self.tick_buffers:
            self.tick_buffers[tick.symbol] = []
        
        buffer = self.tick_buffers[tick.symbol]
        buffer.append(tick)
        
        # Keep only the last window_size ticks
        if len(buffer) > self.window_size:
            buffer.pop(0)
    
    def _calculate_vwap_pure_python(self, ticks: List[Tick]) -> VWAPResult:
        """
        Pure Python VWAP calculation.
        
        This implementation will bottleneck due to GIL when processing
        many ticks in a tight loop.
        """
        if not ticks:
            return VWAPResult(
                symbol="",
                vwap=0.0,
                total_volume=0,
                tick_count=0,
                latency_ms=0.0
            )
        
        start_time = time.perf_counter()
        
        # Pure Python loop - this is where GIL contention occurs
        total_price_volume = 0.0
        total_volume = 0
        
        for tick in ticks:
            total_price_volume += tick.price * tick.volume
            total_volume += tick.volume
        
        vwap = total_price_volume / total_volume if total_volume > 0 else 0.0
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        return VWAPResult(
            symbol=ticks[0].symbol,
            vwap=round(vwap, 4),
            total_volume=total_volume,
            tick_count=len(ticks),
            latency_ms=latency_ms
        )
    
    def _calculate_vwap_numpy(self, ticks: List[Tick]) -> VWAPResult:
        """
        NumPy vectorized VWAP calculation.
        
        This implementation releases the GIL during computation,
        allowing other threads to run concurrently.
        """
        if not ticks:
            return VWAPResult(
                symbol="",
                vwap=0.0,
                total_volume=0,
                tick_count=0,
                latency_ms=0.0
            )
        
        start_time = time.perf_counter()
        
        # Extract data into NumPy arrays
        prices = np.array([tick.price for tick in ticks], dtype=np.float64)
        volumes = np.array([tick.volume for tick in ticks], dtype=np.int64)
        
        # Vectorized computation - releases GIL
        total_price_volume = np.sum(prices * volumes)
        total_volume = np.sum(volumes)
        
        vwap = total_price_volume / total_volume if total_volume > 0 else 0.0
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        return VWAPResult(
            symbol=ticks[0].symbol,
            vwap=round(vwap, 4),
            total_volume=int(total_volume),
            tick_count=len(ticks),
            latency_ms=latency_ms
        )
    
    async def _calculate_vwap_multiprocess(self, ticks: List[Tick]) -> VWAPResult:
        """
        Multiprocessing VWAP calculation.
        
        This implementation offloads CPU-bound work to separate processes,
        completely avoiding GIL contention.
        """
        if not self.process_pool:
            # Fallback to NumPy if no process pool
            return self._calculate_vwap_numpy(ticks)
        
        if not ticks:
            return VWAPResult(
                symbol="",
                vwap=0.0,
                total_volume=0,
                tick_count=0,
                latency_ms=0.0
            )
        
        start_time = time.perf_counter()
        
        # Convert ticks to serializable format
        tick_data = [(t.symbol, t.price, t.volume) for t in ticks]
        
        # Submit to process pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.process_pool,
            _calculate_vwap_worker,
            tick_data
        )
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        return VWAPResult(
            symbol=result['symbol'],
            vwap=result['vwap'],
            total_volume=result['total_volume'],
            tick_count=result['tick_count'],
            latency_ms=latency_ms
        )
    
    async def process_tick(self, tick: Tick) -> Optional[VWAPResult]:
        """
        Process a single tick and return VWAP if calculation is triggered.
        
        This method demonstrates the different approaches to handling
        CPU-bound work in an async context.
        """
        self.total_ticks_processed += 1
        
        # Add tick to buffer
        self._add_tick_to_buffer(tick)
        
        # Calculate VWAP based on implementation strategy
        buffer = self.tick_buffers[tick.symbol]
        
        if self.use_multiprocessing:
            result = await self._calculate_vwap_multiprocess(buffer)
        else:
            # Use NumPy by default (releases GIL)
            result = self._calculate_vwap_numpy(buffer)
        
        # Update statistics
        self.total_latency_ms += result.latency_ms
        
        return result
    
    def get_statistics(self) -> Dict:
        """Get VWAP calculator statistics."""
        avg_latency = (
            self.total_latency_ms / self.total_ticks_processed 
            if self.total_ticks_processed > 0 else 0.0
        )
        
        return {
            'total_ticks_processed': self.total_ticks_processed,
            'average_latency_ms': round(avg_latency, 3),
            'total_latency_ms': round(self.total_latency_ms, 3),
            'symbols_tracked': len(self.tick_buffers),
            'implementation': 'multiprocess' if self.use_multiprocessing else 'numpy'
        }


def _calculate_vwap_worker(tick_data: List[tuple]) -> Dict:
    """
    Worker function for multiprocessing VWAP calculation.
    
    This function runs in a separate process, completely avoiding GIL.
    """
    if not tick_data:
        return {
            'symbol': '',
            'vwap': 0.0,
            'total_volume': 0,
            'tick_count': 0
        }
    
    # Convert back to NumPy arrays
    prices = np.array([price for _, price, _ in tick_data], dtype=np.float64)
    volumes = np.array([volume for _, _, volume in tick_data], dtype=np.int64)
    
    # Vectorized computation in separate process
    total_price_volume = np.sum(prices * volumes)
    total_volume = np.sum(volumes)
    
    vwap = total_price_volume / total_volume if total_volume > 0 else 0.0
    
    return {
        'symbol': tick_data[0][0],
        'vwap': round(vwap, 4),
        'total_volume': int(total_volume),
        'tick_count': len(tick_data)
    }


# Performance comparison function
async def compare_vwap_implementations(ticks: List[Tick]) -> Dict:
    """Compare performance of different VWAP implementations."""
    results = {}
    
    # Test pure Python implementation
    calculator_python = VWAPCalculator(window_size=len(ticks))
    start_time = time.perf_counter()
    
    for tick in ticks:
        calculator_python._add_tick_to_buffer(tick)
    
    result_python = calculator_python._calculate_vwap_pure_python(ticks)
    python_time = (time.perf_counter() - start_time) * 1000
    
    # Test NumPy implementation
    calculator_numpy = VWAPCalculator(window_size=len(ticks))
    start_time = time.perf_counter()
    
    for tick in ticks:
        calculator_numpy._add_tick_to_buffer(tick)
    
    result_numpy = calculator_numpy._calculate_vwap_numpy(ticks)
    numpy_time = (time.perf_counter() - start_time) * 1000
    
    results = {
        'python': {
            'time_ms': python_time,
            'vwap': result_python.vwap,
            'latency_ms': result_python.latency_ms
        },
        'numpy': {
            'time_ms': numpy_time,
            'vwap': result_numpy.vwap,
            'latency_ms': result_numpy.latency_ms
        },
        'speedup': python_time / numpy_time if numpy_time > 0 else float('inf')
    }
    
    return results


if __name__ == "__main__":
    # Demo: Compare implementations
    async def demo():
        # Generate test data
        from .feed import Tick
        import random
        
        test_ticks = [
            Tick('AAPL', 150.0 + i * 0.1, random.randint(100, 1000), time.time())
            for i in range(1000)
        ]
        
        print("Comparing VWAP implementations...")
        comparison = await compare_vwap_implementations(test_ticks)
        
        print(f"Pure Python: {comparison['python']['time_ms']:.2f}ms")
        print(f"NumPy: {comparison['numpy']['time_ms']:.2f}ms")
        print(f"Speedup: {comparison['speedup']:.1f}x")
    
    asyncio.run(demo()) 