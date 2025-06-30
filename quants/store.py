"""
Async file storage for market data ticks.

Demonstrates async file I/O that stays on the event loop.
Shows that disk persistence doesn't need separate threads.
"""

import asyncio
import json
import mmap
import os
import time
from typing import AsyncIterator, List, Optional
from dataclasses import asdict
import aiofiles
from .feed import Tick


class TickStore:
    """Async file storage for market data ticks."""
    
    def __init__(self, log_file: str = "ticks.log", buffer_size: int = 8192):
        self.log_file = log_file
        self.buffer_size = buffer_size
        self.total_ticks_written = 0
        self.total_bytes_written = 0
        self.start_time = time.time()
        
        # Ensure log file exists
        if not os.path.exists(log_file):
            with open(log_file, 'w') as f:
                f.write("")  # Create empty file
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.flush()
    
    async def write_tick(self, tick: Tick) -> int:
        """
        Write a single tick to the log file asynchronously.
        
        This demonstrates async file I/O that stays on the event loop.
        No threads needed for disk operations.
        """
        start_time = time.perf_counter()
        
        # Convert tick to JSON line
        tick_json = json.dumps(asdict(tick)) + '\n'
        tick_bytes = tick_json.encode('utf-8')
        
        # Write to file asynchronously
        async with aiofiles.open(self.log_file, mode='a') as f:
            await f.write(tick_json)
        
        # Update statistics
        self.total_ticks_written += 1
        self.total_bytes_written += len(tick_bytes)
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        return len(tick_bytes)
    
    async def write_ticks_batch(self, ticks: List[Tick]) -> int:
        """
        Write multiple ticks in a batch for better performance.
        
        This reduces the number of file system calls by batching writes.
        """
        if not ticks:
            return 0
        
        start_time = time.perf_counter()
        
        # Convert all ticks to JSON lines
        lines = []
        total_bytes = 0
        
        for tick in ticks:
            tick_json = json.dumps(asdict(tick)) + '\n'
            lines.append(tick_json)
            total_bytes += len(tick_json.encode('utf-8'))
        
        # Write all lines in one operation
        async with aiofiles.open(self.log_file, mode='a') as f:
            await f.writelines(lines)
        
        # Update statistics
        self.total_ticks_written += len(ticks)
        self.total_bytes_written += total_bytes
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        return total_bytes
    
    async def flush(self):
        """Flush any pending writes to disk."""
        # aiofiles handles this automatically, but we can add custom logic here
        pass
    
    async def read_ticks(self, limit: Optional[int] = None) -> AsyncIterator[Tick]:
        """
        Read ticks from the log file asynchronously.
        
        This demonstrates async file reading that stays on the event loop.
        """
        if not os.path.exists(self.log_file):
            return
        
        count = 0
        async with aiofiles.open(self.log_file, mode='r') as f:
            async for line in f:
                if limit and count >= limit:
                    break
                
                line = line.strip()
                if line:
                    try:
                        tick_data = json.loads(line)
                        tick = Tick(**tick_data)
                        yield tick
                        count += 1
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"Error parsing tick: {e}")
                        continue
    
    def get_statistics(self) -> dict:
        """Get storage statistics."""
        runtime_seconds = time.time() - self.start_time
        
        return {
            'total_ticks_written': self.total_ticks_written,
            'total_bytes_written': self.total_bytes_written,
            'average_tick_size_bytes': (
                self.total_bytes_written / self.total_ticks_written 
                if self.total_ticks_written > 0 else 0
            ),
            'write_rate_ticks_per_second': (
                self.total_ticks_written / runtime_seconds 
                if runtime_seconds > 0 else 0
            ),
            'write_rate_mb_per_second': (
                (self.total_bytes_written / 1024 / 1024) / runtime_seconds 
                if runtime_seconds > 0 else 0
            ),
            'log_file_size_mb': os.path.getsize(self.log_file) / 1024 / 1024
        }


class MemoryMappedTickStore:
    """
    Memory-mapped tick store for high-performance replay.
    
    Demonstrates using mmap for efficient file access.
    """
    
    def __init__(self, log_file: str = "ticks.log"):
        self.log_file = log_file
        self.file_handle = None
        self.mmap_handle = None
    
    def __enter__(self):
        """Context manager entry."""
        self.file_handle = open(self.log_file, 'rb')
        self.mmap_handle = mmap.mmap(
            self.file_handle.fileno(), 
            0, 
            access=mmap.ACCESS_READ
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.mmap_handle:
            self.mmap_handle.close()
        if self.file_handle:
            self.file_handle.close()
    
    def read_ticks_fast(self, limit: Optional[int] = None) -> List[Tick]:
        """
        Read ticks using memory mapping for maximum performance.
        
        This is useful for replaying historical data quickly.
        """
        if not self.mmap_handle:
            return []
        
        ticks = []
        count = 0
        
        # Read line by line from memory-mapped file
        for line in iter(self.mmap_handle.readline, b''):
            if limit and count >= limit:
                break
            
            line = line.decode('utf-8').strip()
            if line:
                try:
                    tick_data = json.loads(line)
                    tick = Tick(**tick_data)
                    ticks.append(tick)
                    count += 1
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error parsing tick: {e}")
                    continue
        
        return ticks
    
    def get_tick_count(self) -> int:
        """Get total number of ticks in the file."""
        if not self.mmap_handle:
            return 0
        
        count = 0
        for _ in iter(self.mmap_handle.readline, b''):
            count += 1
        
        # Reset file pointer
        self.mmap_handle.seek(0)
        return count


class AsyncTickProcessor:
    """
    Async tick processor that demonstrates efficient async I/O patterns.
    
    This class shows how to handle high-throughput tick processing
    with async file I/O while staying on the event loop.
    """
    
    def __init__(self, store: TickStore, batch_size: int = 100):
        self.store = store
        self.batch_size = batch_size
        self.pending_ticks = []
        self.total_processed = 0
    
    async def process_tick(self, tick: Tick):
        """
        Process a single tick, batching writes for efficiency.
        """
        self.pending_ticks.append(tick)
        
        # Flush batch when it reaches the batch size
        if len(self.pending_ticks) >= self.batch_size:
            await self.flush_batch()
    
    async def flush_batch(self):
        """Flush the current batch of ticks to disk."""
        if not self.pending_ticks:
            return
        
        await self.store.write_ticks_batch(self.pending_ticks)
        self.total_processed += len(self.pending_ticks)
        self.pending_ticks.clear()
    
    async def close(self):
        """Close the processor, flushing any remaining ticks."""
        await self.flush_batch()
    
    def get_statistics(self) -> dict:
        """Get processor statistics."""
        return {
            'total_processed': self.total_processed,
            'pending_ticks': len(self.pending_ticks),
            'batch_size': self.batch_size
        }


# Performance comparison functions
async def compare_storage_methods(ticks: List[Tick]) -> dict:
    """Compare different storage methods."""
    results = {}
    
    # Test individual writes
    store_individual = TickStore("test_individual.log")
    start_time = time.perf_counter()
    
    for tick in ticks:
        await store_individual.write_tick(tick)
    
    individual_time = (time.perf_counter() - start_time) * 1000
    
    # Test batch writes
    store_batch = TickStore("test_batch.log")
    start_time = time.perf_counter()
    
    await store_batch.write_ticks_batch(ticks)
    
    batch_time = (time.perf_counter() - start_time) * 1000
    
    # Test async processor
    store_processor = TickStore("test_processor.log")
    processor = AsyncTickProcessor(store_processor, batch_size=50)
    
    start_time = time.perf_counter()
    
    for tick in ticks:
        await processor.process_tick(tick)
    
    await processor.close()
    
    processor_time = (time.perf_counter() - start_time) * 1000
    
    results = {
        'individual_writes': {
            'time_ms': individual_time,
            'ticks_per_second': len(ticks) / (individual_time / 1000)
        },
        'batch_writes': {
            'time_ms': batch_time,
            'ticks_per_second': len(ticks) / (batch_time / 1000)
        },
        'async_processor': {
            'time_ms': processor_time,
            'ticks_per_second': len(ticks) / (processor_time / 1000)
        }
    }
    
    # Cleanup test files
    for filename in ["test_individual.log", "test_batch.log", "test_processor.log"]:
        if os.path.exists(filename):
            os.remove(filename)
    
    return results


if __name__ == "__main__":
    # Demo: Test storage methods
    async def demo():
        from .feed import Tick
        import random
        
        # Generate test data
        test_ticks = [
            Tick('AAPL', 150.0 + i * 0.1, random.randint(100, 1000), time.time())
            for i in range(1000)
        ]
        
        print("Comparing storage methods...")
        comparison = await compare_storage_methods(test_ticks)
        
        print(f"Individual writes: {comparison['individual_writes']['time_ms']:.2f}ms")
        print(f"Batch writes: {comparison['batch_writes']['time_ms']:.2f}ms")
        print(f"Async processor: {comparison['async_processor']['time_ms']:.2f}ms")
        
        print(f"Speedup (batch vs individual): {comparison['individual_writes']['time_ms'] / comparison['batch_writes']['time_ms']:.1f}x")
    
    asyncio.run(demo()) 