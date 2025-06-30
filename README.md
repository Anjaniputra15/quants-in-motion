# Quants-in-Motion 🚀

A self-contained repository showcasing async I/O, GIL-aware concurrency, memory-safe design, and performance tuning in a realistic trading-adjacent application.

## Elevator Pitch

Build an asynchronous market-data pump that:

- **Streams thousands of mock price-ticks/second** over WebSockets (asyncio)
- **Fan-outs each tick to three pipelines**:
  - CPU-bound VWAP calculator (shows the GIL bottleneck → fix with multiprocessing)
  - I/O-bound persistence to an on-disk log (kept on the event loop → no extra threads)
  - Risk alert service using a thread pool (safe because it's mostly waiting on Redis)
- **Continuously monitors memory** (refcounts, cycles, RSS) and prints a pause histogram
- **Exposes a /metrics HTTP endpoint** (Prometheus-style) for live profiling

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run with default settings (1 core, 1000 ticks/sec)
python -m quants run

# Run with high performance settings
python -m quants run --tick-rate 50000 --cores 6 --freeze-gc

# Monitor performance
curl http://localhost:8080/metrics
```

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   WebSocket     │    │   File Logger   │    │  Risk Service   │
│   Feed (async)  │───▶│   (async I/O)   │    │  (thread pool)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  VWAP Calc      │    │  Memory Pool    │    │  Performance    │
│ (multiprocess)  │    │ (weakref pool)  │    │   Monitor       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Key Concepts Demonstrated

### 1. Async I/O (feed.py)
- Pure asyncio WebSocket streaming
- No threads needed for network I/O
- Bypasses GIL for network operations

### 2. GIL & Multiprocessing (vwap.py)
- Pure Python VWAP (bottlenecks due to GIL)
- NumPy vectorized VWAP (releases GIL)
- ProcessPoolExecutor for CPU-bound work

### 3. Memory Management (pools.py)
- Weak-ref object pooling for Tick objects
- Reduces per-tick allocations
- Includes ref-cycle demo

### 4. Performance Monitoring (monitor.py)
- Real-time GC statistics
- Memory usage tracking
- GC pause histograms
- HTTP metrics endpoint

## CLI Options

```bash
python -m quants run [OPTIONS]

Options:
  --tick-rate INTEGER     Ticks per second [default: 1000]
  --cores INTEGER         Number of CPU cores for VWAP [default: 1]
  --freeze-gc            Freeze garbage collection after warmup
  --pool-size INTEGER     Object pool size [default: 1000]
  --log-file TEXT         Log file path [default: ticks.log]
  --metrics-port INTEGER  Metrics server port [default: 8080]
  --help                  Show this message and exit.
```

## Performance Tuning

The application demonstrates several performance optimization techniques:

1. **GIL Awareness**: CPU-bound work uses multiprocessing
2. **Memory Pooling**: Reduces GC pressure
3. **Async I/O**: Keeps disk operations on event loop
4. **Thread Pool**: For external service calls
5. **GC Tuning**: Optional gc.freeze() after warmup

## Monitoring

Access real-time metrics at `http://localhost:8080/metrics`:

```json
{
  "ticks_processed": 12345,
  "vwap_latency_p99": 0.0012,
  "memory_rss_mb": 45.2,
  "gc_collections": 12,
  "gc_pause_p99_ms": 2.1
}
```

## Development

```bash
# Run tests
python -m pytest tests/

# Run with debug logging
python -m quants run --debug

# Profile with cProfile
python -m cProfile -o profile.stats -m quants run
```

## Dependencies

- **aiohttp**: Async HTTP/WebSocket support
- **uvloop**: Fast event loop implementation
- **psutil**: System and process monitoring
- **numpy**: Vectorized computations (releases GIL)
- **click**: CLI framework
- **aiofiles**: Async file I/O

All other dependencies are from Python's standard library. 