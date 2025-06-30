"""
Object pooling for memory management.

Demonstrates weak references, object pooling, and memory optimization
to reduce garbage collection pressure.
"""

import asyncio
import gc
import sys
import time
import weakref
from typing import List, Optional, Set, Dict
from dataclasses import dataclass
from .feed import Tick


@dataclass
class PooledTick:
    """
    Pooled tick object with reference counting.
    
    This demonstrates object pooling to reduce allocations.
    """
    symbol: str
    price: float
    volume: int
    timestamp: float
    _pool_ref: Optional[weakref.ref] = None
    
    def reset(self, symbol: str, price: float, volume: int, timestamp: float):
        """Reset the tick object for reuse."""
        self.symbol = symbol
        self.price = price
        self.volume = volume
        self.timestamp = timestamp
    
    def return_to_pool(self):
        """Return this object to its pool."""
        if self._pool_ref:
            pool = self._pool_ref()
            if pool:
                pool.return_object(self)


class TickPool:
    """
    Object pool for Tick objects using weak references.
    
    This demonstrates memory management techniques to reduce GC pressure.
    """
    
    def __init__(self, max_size: int = 1000, enable_ref_cycles: bool = False):
        self.max_size = max_size
        self.enable_ref_cycles = enable_ref_cycles
        
        # Pool of available objects
        self.available: List[PooledTick] = []
        
        # Track all objects for debugging
        self.all_objects: Set[PooledTick] = set()
        
        # Statistics
        self.objects_created = 0
        self.objects_reused = 0
        self.objects_destroyed = 0
    
    def get_object(self, symbol: str, price: float, volume: int, timestamp: float) -> PooledTick:
        """
        Get a tick object from the pool or create a new one.
        
        This reduces allocations by reusing objects.
        """
        if self.available:
            # Reuse existing object
            tick = self.available.pop()
            tick.reset(symbol, price, volume, timestamp)
            self.objects_reused += 1
        else:
            # Create new object
            tick = PooledTick(symbol, price, volume, timestamp)
            tick._pool_ref = weakref.ref(self)
            self.objects_created += 1
            self.all_objects.add(tick)
        
        # Optionally create reference cycles for demo
        if self.enable_ref_cycles:
            self._create_ref_cycle(tick)
        
        return tick
    
    def return_object(self, tick: PooledTick):
        """
        Return a tick object to the pool.
        
        This allows the object to be reused instead of being garbage collected.
        """
        if len(self.available) < self.max_size:
            self.available.append(tick)
        else:
            # Pool is full, let object be garbage collected
            self.objects_destroyed += 1
            self.all_objects.discard(tick)
    
    def _create_ref_cycle(self, tick: PooledTick):
        """
        Create a deliberate reference cycle for demonstration.
        
        This shows how reference cycles can prevent garbage collection.
        """
        # Create a cycle: tick -> cycle_data -> tick
        cycle_data = {'tick': tick, 'metadata': 'cycle_demo'}
        tick._cycle_data = cycle_data  # This creates a cycle
    
    def clear_ref_cycles(self):
        """Clear any reference cycles to help garbage collection."""
        for tick in list(self.all_objects):
            if hasattr(tick, '_cycle_data'):
                del tick._cycle_data
    
    def get_statistics(self) -> Dict:
        """Get pool statistics."""
        return {
            'objects_created': self.objects_created,
            'objects_reused': self.objects_reused,
            'objects_destroyed': self.objects_destroyed,
            'available_in_pool': len(self.available),
            'total_tracked': len(self.all_objects),
            'reuse_rate': self.objects_reused / (self.objects_created + self.objects_reused) if (self.objects_created + self.objects_reused) > 0 else 0,
            'max_size': self.max_size
        }
    
    def cleanup(self):
        """Clean up the pool and all tracked objects."""
        self.available.clear()
        self.all_objects.clear()


class MemoryTracker:
    """
    Memory usage tracker for monitoring object allocations.
    
    This demonstrates how to monitor memory usage and garbage collection.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.initial_memory = self.get_memory_usage()
        self.peak_memory = self.initial_memory
        self.memory_samples = []
    
    def get_memory_usage(self) -> Dict:
        """Get current memory usage statistics."""
        import psutil
        
        process = psutil.Process()
        memory_info = process.memory_info()
        
        # Get garbage collection statistics
        gc_stats = gc.get_stats()
        
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'gc_collections': sum(stat['collections'] for stat in gc_stats),
            'gc_objects': sum(stat['objects'] for stat in gc_stats),
            'gc_time_ms': sum(stat['collections'] * stat.get('avg_time', 0) for stat in gc_stats)
        }
    
    def sample_memory(self):
        """Take a memory usage sample."""
        current = self.get_memory_usage()
        current['timestamp'] = time.time()
        self.memory_samples.append(current)
        
        # Update peak memory
        if current['rss_mb'] > self.peak_memory['rss_mb']:
            self.peak_memory = current
    
    def get_memory_growth(self) -> Dict:
        """Calculate memory growth over time."""
        if not self.memory_samples:
            return {}
        
        runtime = time.time() - self.start_time
        memory_growth = self.memory_samples[-1]['rss_mb'] - self.initial_memory['rss_mb']
        
        return {
            'runtime_seconds': runtime,
            'memory_growth_mb': memory_growth,
            'growth_rate_mb_per_second': memory_growth / runtime if runtime > 0 else 0,
            'peak_memory_mb': self.peak_memory['rss_mb'],
            'samples_taken': len(self.memory_samples)
        }


class AsyncTickPoolProcessor:
    """
    Async processor that uses object pooling for memory efficiency.
    
    This demonstrates how to use object pooling in an async context.
    """
    
    def __init__(self, pool: TickPool, batch_size: int = 100):
        self.pool = pool
        self.batch_size = batch_size
        self.pending_ticks = []
        self.total_processed = 0
        self.memory_tracker = MemoryTracker()
    
    async def process_tick(self, tick_data: Dict) -> PooledTick:
        """
        Process tick data using pooled objects.
        """
        # Get object from pool
        tick = self.pool.get_object(
            tick_data['symbol'],
            tick_data['price'],
            tick_data['volume'],
            tick_data['timestamp']
        )
        
        self.pending_ticks.append(tick)
        
        # Process batch when it reaches the batch size
        if len(self.pending_ticks) >= self.batch_size:
            await self.process_batch()
        
        return tick
    
    async def process_batch(self):
        """Process the current batch of ticks."""
        if not self.pending_ticks:
            return
        
        # Simulate some processing
        await asyncio.sleep(0.001)  # 1ms processing time
        
        # Return objects to pool
        for tick in self.pending_ticks:
            tick.return_to_pool()
        
        self.total_processed += len(self.pending_ticks)
        self.pending_ticks.clear()
        
        # Sample memory usage
        self.memory_tracker.sample_memory()
    
    async def flush(self):
        """Flush any remaining ticks."""
        await self.process_batch()
    
    def get_statistics(self) -> Dict:
        """Get processor statistics."""
        pool_stats = self.pool.get_statistics()
        memory_stats = self.memory_tracker.get_memory_growth()
        
        return {
            'total_processed': self.total_processed,
            'pending_ticks': len(self.pending_ticks),
            'pool_statistics': pool_stats,
            'memory_statistics': memory_stats
        }


# Performance comparison functions
async def compare_pooling_methods(num_ticks: int = 10000) -> Dict:
    """Compare object pooling vs regular allocation."""
    results = {}
    
    # Test without pooling
    print("Testing without object pooling...")
    start_time = time.perf_counter()
    
    # Force garbage collection before test
    gc.collect()
    
    memory_tracker_no_pool = MemoryTracker()
    memory_tracker_no_pool.sample_memory()
    
    # Create many tick objects without pooling
    ticks_no_pool = []
    for i in range(num_ticks):
        tick = Tick(
            f"SYMBOL_{i % 10}",
            100.0 + i * 0.01,
            (i % 1000) + 100,
            time.time()
        )
        ticks_no_pool.append(tick)
        
        if i % 1000 == 0:
            memory_tracker_no_pool.sample_memory()
    
    memory_tracker_no_pool.sample_memory()
    no_pool_time = (time.perf_counter() - start_time) * 1000
    
    # Clear references to allow garbage collection
    del ticks_no_pool
    gc.collect()
    
    # Test with pooling
    print("Testing with object pooling...")
    start_time = time.perf_counter()
    
    # Force garbage collection before test
    gc.collect()
    
    pool = TickPool(max_size=1000, enable_ref_cycles=False)
    processor = AsyncTickProcessor(pool, batch_size=100)
    memory_tracker_pool = MemoryTracker()
    memory_tracker_pool.sample_memory()
    
    # Process ticks using pooling
    for i in range(num_ticks):
        tick_data = {
            'symbol': f"SYMBOL_{i % 10}",
            'price': 100.0 + i * 0.01,
            'volume': (i % 1000) + 100,
            'timestamp': time.time()
        }
        await processor.process_tick(tick_data)
        
        if i % 1000 == 0:
            memory_tracker_pool.sample_memory()
    
    await processor.flush()
    memory_tracker_pool.sample_memory()
    pool_time = (time.perf_counter() - start_time) * 1000
    
    # Cleanup
    pool.cleanup()
    gc.collect()
    
    results = {
        'no_pooling': {
            'time_ms': no_pool_time,
            'memory_growth_mb': memory_tracker_no_pool.get_memory_growth()['memory_growth_mb'],
            'peak_memory_mb': memory_tracker_no_pool.peak_memory['rss_mb']
        },
        'with_pooling': {
            'time_ms': pool_time,
            'memory_growth_mb': memory_tracker_pool.get_memory_growth()['memory_growth_mb'],
            'peak_memory_mb': memory_tracker_pool.peak_memory['rss_mb'],
            'pool_stats': processor.get_statistics()['pool_statistics']
        }
    }
    
    return results


class AsyncTickProcessor:
    """Async tick processor using object pooling."""
    
    def __init__(self, pool: TickPool, batch_size: int = 100):
        self.pool = pool
        self.batch_size = batch_size
        self.pending_ticks = []
        self.total_processed = 0
    
    async def process_tick(self, tick_data: Dict) -> PooledTick:
        """Process a tick using pooled objects."""
        tick = self.pool.get_object(
            tick_data['symbol'],
            tick_data['price'],
            tick_data['volume'],
            tick_data['timestamp']
        )
        
        self.pending_ticks.append(tick)
        
        if len(self.pending_ticks) >= self.batch_size:
            await self.process_batch()
        
        return tick
    
    async def process_batch(self):
        """Process the current batch."""
        if not self.pending_ticks:
            return
        
        # Simulate processing
        await asyncio.sleep(0.001)
        
        # Return objects to pool
        for tick in self.pending_ticks:
            tick.return_to_pool()
        
        self.total_processed += len(self.pending_ticks)
        self.pending_ticks.clear()
    
    async def flush(self):
        """Flush remaining ticks."""
        await self.process_batch()
    
    def get_statistics(self) -> Dict:
        """Get processor statistics."""
        return {
            'total_processed': self.total_processed,
            'pending_ticks': len(self.pending_ticks),
            'pool_statistics': self.pool.get_statistics()
        }


# Reference cycle demonstration
def demonstrate_ref_cycles():
    """Demonstrate reference cycles and their impact on garbage collection."""
    print("Demonstrating reference cycles...")
    
    # Create objects with reference cycles
    pool_with_cycles = TickPool(max_size=100, enable_ref_cycles=True)
    
    # Create some objects
    for i in range(50):
        tick = pool_with_cycles.get_object(f"SYMBOL_{i}", 100.0 + i, 100 + i, time.time())
        # Don't return to pool to create cycles
    
    print(f"Objects with cycles: {len(pool_with_cycles.all_objects)}")
    
    # Check garbage collection
    gc.collect()
    print(f"Objects after GC: {len(pool_with_cycles.all_objects)}")
    
    # Clear cycles manually
    pool_with_cycles.clear_ref_cycles()
    gc.collect()
    print(f"Objects after clearing cycles: {len(pool_with_cycles.all_objects)}")
    
    pool_with_cycles.cleanup()


if __name__ == "__main__":
    # Demo: Test object pooling
    async def demo():
        print("Testing object pooling performance...")
        comparison = await compare_pooling_methods(5000)
        
        print(f"Without pooling: {comparison['no_pooling']['time_ms']:.2f}ms")
        print(f"With pooling: {comparison['with_pooling']['time_ms']:.2f}ms")
        
        print(f"Memory growth without pooling: {comparison['no_pooling']['memory_growth_mb']:.1f}MB")
        print(f"Memory growth with pooling: {comparison['with_pooling']['memory_growth_mb']:.1f}MB")
        
        print(f"Pool reuse rate: {comparison['with_pooling']['pool_stats']['reuse_rate']:.1%}")
        
        # Demonstrate reference cycles
        demonstrate_ref_cycles()
    
    asyncio.run(demo()) 