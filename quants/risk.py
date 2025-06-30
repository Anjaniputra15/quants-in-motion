"""
Risk alert service using thread pool.

Demonstrates when to use ThreadPoolExecutor for external service calls
that are mostly I/O bound (like Redis, databases).
"""

import asyncio
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from .feed import Tick


@dataclass
class RiskAlert:
    """Risk alert with severity and details."""
    symbol: str
    alert_type: str
    severity: str  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    message: str
    timestamp: float
    price: float
    volume: int


class MockRedisClient:
    """
    Mock Redis client that simulates network latency.
    
    This demonstrates why we use ThreadPoolExecutor for external services.
    """
    
    def __init__(self, latency_ms: float = 1.0):
        self.latency_ms = latency_ms
        self.data = {}
        self.calls = 0
    
    def get(self, key: str) -> Optional[str]:
        """Simulate Redis GET with network latency."""
        self.calls += 1
        time.sleep(self.latency_ms / 1000)  # Simulate network delay
        return self.data.get(key)
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Simulate Redis SET with network latency."""
        self.calls += 1
        time.sleep(self.latency_ms / 1000)  # Simulate network delay
        self.data[key] = value
        return True
    
    def incr(self, key: str) -> int:
        """Simulate Redis INCR with network latency."""
        self.calls += 1
        time.sleep(self.latency_ms / 1000)  # Simulate network delay
        current = int(self.data.get(key, 0))
        new_value = current + 1
        self.data[key] = str(new_value)
        return new_value


class RiskService:
    """
    Risk alert service using ThreadPoolExecutor.
    
    Demonstrates when to use threads for external service calls:
    - Redis operations (network I/O)
    - Database queries
    - External API calls
    
    These operations are mostly waiting on network, not CPU.
    """
    
    def __init__(self, max_workers: int = 4, alert_threshold: float = 0.05):
        self.max_workers = max_workers
        self.alert_threshold = alert_threshold
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        
        # Mock external services
        self.redis_client = MockRedisClient(latency_ms=2.0)
        
        # Risk tracking
        self.symbol_alerts: Dict[str, List[RiskAlert]] = {}
        self.total_alerts = 0
        self.total_checks = 0
        
        # Price history for volatility calculation
        self.price_history: Dict[str, List[float]] = {}
        self.max_history = 100
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.thread_pool.shutdown(wait=True)
    
    def _update_price_history(self, tick: Tick):
        """Update price history for volatility calculation."""
        if tick.symbol not in self.price_history:
            self.price_history[tick.symbol] = []
        
        history = self.price_history[tick.symbol]
        history.append(tick.price)
        
        # Keep only recent prices
        if len(history) > self.max_history:
            history.pop(0)
    
    def _calculate_volatility(self, symbol: str) -> float:
        """Calculate price volatility for a symbol."""
        if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
            return 0.0
        
        prices = self.price_history[symbol]
        if len(prices) < 2:
            return 0.0
        
        # Calculate price changes
        changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
        # Calculate standard deviation of changes
        mean_change = sum(changes) / len(changes)
        variance = sum((c - mean_change) ** 2 for c in changes) / len(changes)
        volatility = variance ** 0.5
        
        return volatility
    
    async def _check_redis_limits(self, tick: Tick) -> Optional[RiskAlert]:
        """
        Check Redis for trading limits (runs in thread pool).
        
        This demonstrates using ThreadPoolExecutor for external service calls.
        """
        loop = asyncio.get_event_loop()
        
        # Submit Redis operations to thread pool
        daily_volume_key = f"daily_volume:{tick.symbol}"
        price_key = f"last_price:{tick.symbol}"
        
        # These operations are mostly waiting on network I/O
        daily_volume, last_price = await asyncio.gather(
            loop.run_in_executor(self.thread_pool, self.redis_client.incr, daily_volume_key),
            loop.run_in_executor(self.thread_pool, self.redis_client.get, price_key)
        )
        
        # Update last price
        await loop.run_in_executor(
            self.thread_pool, 
            self.redis_client.set, 
            price_key, 
            str(tick.price)
        )
        
        # Check for alerts
        alerts = []
        
        # Volume alert
        if daily_volume > 1000000:  # 1M shares
            alerts.append(RiskAlert(
                symbol=tick.symbol,
                alert_type="HIGH_VOLUME",
                severity="MEDIUM",
                message=f"High daily volume: {daily_volume:,} shares",
                timestamp=tick.timestamp,
                price=tick.price,
                volume=tick.volume
            ))
        
        # Price change alert
        if last_price:
            last_price_float = float(last_price)
            price_change_pct = abs(tick.price - last_price_float) / last_price_float
            
            if price_change_pct > self.alert_threshold:
                severity = "HIGH" if price_change_pct > 0.1 else "MEDIUM"
                alerts.append(RiskAlert(
                    symbol=tick.symbol,
                    alert_type="PRICE_SPIKE",
                    severity=severity,
                    message=f"Price change: {price_change_pct:.2%}",
                    timestamp=tick.timestamp,
                    price=tick.price,
                    volume=tick.volume
                ))
        
        return alerts[0] if alerts else None
    
    async def _check_volatility(self, tick: Tick) -> Optional[RiskAlert]:
        """
        Check for volatility-based alerts (CPU-bound, but lightweight).
        
        This stays on the event loop since it's fast CPU work.
        """
        volatility = self._calculate_volatility(tick.symbol)
        
        if volatility > 0.02:  # 2% volatility threshold
            return RiskAlert(
                symbol=tick.symbol,
                alert_type="HIGH_VOLATILITY",
                severity="HIGH",
                message=f"High volatility: {volatility:.3f}",
                timestamp=tick.timestamp,
                price=tick.price,
                volume=tick.volume
            )
        
        return None
    
    async def process_tick(self, tick: Tick) -> List[RiskAlert]:
        """
        Process a tick and return any risk alerts.
        
        This method demonstrates the hybrid approach:
        - CPU-bound work (volatility calculation) stays on event loop
        - I/O-bound work (Redis calls) goes to thread pool
        """
        self.total_checks += 1
        
        # Update price history (fast, stays on event loop)
        self._update_price_history(tick)
        
        # Check for alerts concurrently
        redis_alert, volatility_alert = await asyncio.gather(
            self._check_redis_limits(tick),
            self._check_volatility(tick),
            return_exceptions=True
        )
        
        alerts = []
        
        # Handle Redis alert
        if isinstance(redis_alert, Exception):
            print(f"Redis check error: {redis_alert}")
        elif redis_alert:
            alerts.append(redis_alert)
        
        # Handle volatility alert
        if isinstance(volatility_alert, Exception):
            print(f"Volatility check error: {volatility_alert}")
        elif volatility_alert:
            alerts.append(volatility_alert)
        
        # Store alerts
        for alert in alerts:
            if alert.symbol not in self.symbol_alerts:
                self.symbol_alerts[alert.symbol] = []
            self.symbol_alerts[alert.symbol].append(alert)
            self.total_alerts += 1
        
        return alerts
    
    def get_statistics(self) -> Dict:
        """Get risk service statistics."""
        return {
            'total_checks': self.total_checks,
            'total_alerts': self.total_alerts,
            'alert_rate': self.total_alerts / self.total_checks if self.total_checks > 0 else 0,
            'symbols_tracked': len(self.symbol_alerts),
            'redis_calls': self.redis_client.calls,
            'thread_pool_workers': self.max_workers
        }
    
    def get_alerts_by_severity(self) -> Dict[str, int]:
        """Get alert counts by severity."""
        severity_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for alerts in self.symbol_alerts.values():
            for alert in alerts:
                severity_counts[alert.severity] += 1
        
        return severity_counts


class AsyncRiskProcessor:
    """
    Async risk processor that demonstrates efficient risk checking patterns.
    
    This class shows how to handle risk checking with proper async/await
    and thread pool usage.
    """
    
    def __init__(self, risk_service: RiskService, batch_size: int = 50):
        self.risk_service = risk_service
        self.batch_size = batch_size
        self.pending_ticks = []
        self.total_processed = 0
        self.total_alerts = 0
    
    async def process_tick(self, tick: Tick) -> List[RiskAlert]:
        """
        Process a single tick for risk checking.
        """
        self.pending_ticks.append(tick)
        
        # Process batch when it reaches the batch size
        if len(self.pending_ticks) >= self.batch_size:
            return await self.process_batch()
        
        return []
    
    async def process_batch(self) -> List[RiskAlert]:
        """Process the current batch of ticks."""
        if not self.pending_ticks:
            return []
        
        # Process all ticks in the batch concurrently
        tasks = [
            self.risk_service.process_tick(tick) 
            for tick in self.pending_ticks
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect all alerts
        all_alerts = []
        for result in results:
            if isinstance(result, Exception):
                print(f"Risk processing error: {result}")
            else:
                all_alerts.extend(result)
        
        # Update statistics
        self.total_processed += len(self.pending_ticks)
        self.total_alerts += len(all_alerts)
        self.pending_ticks.clear()
        
        return all_alerts
    
    async def flush(self) -> List[RiskAlert]:
        """Flush any remaining ticks."""
        return await self.process_batch()
    
    def get_statistics(self) -> Dict:
        """Get processor statistics."""
        return {
            'total_processed': self.total_processed,
            'total_alerts': self.total_alerts,
            'pending_ticks': len(self.pending_ticks),
            'batch_size': self.batch_size,
            'alert_rate': self.total_alerts / self.total_processed if self.total_processed > 0 else 0
        }


# Performance comparison functions
async def compare_risk_methods(ticks: List[Tick]) -> Dict:
    """Compare different risk checking approaches."""
    results = {}
    
    # Test individual processing
    risk_service_individual = RiskService(max_workers=2)
    start_time = time.perf_counter()
    
    total_alerts = 0
    for tick in ticks:
        alerts = await risk_service_individual.process_tick(tick)
        total_alerts += len(alerts)
    
    individual_time = (time.perf_counter() - start_time) * 1000
    
    # Test batch processing
    risk_service_batch = RiskService(max_workers=4)
    processor = AsyncRiskProcessor(risk_service_batch, batch_size=50)
    
    start_time = time.perf_counter()
    
    batch_alerts = 0
    for tick in ticks:
        alerts = await processor.process_tick(tick)
        batch_alerts.extend(alerts)
    
    await processor.flush()
    
    batch_time = (time.perf_counter() - start_time) * 1000
    
    results = {
        'individual_processing': {
            'time_ms': individual_time,
            'ticks_per_second': len(ticks) / (individual_time / 1000),
            'alerts': total_alerts
        },
        'batch_processing': {
            'time_ms': batch_time,
            'ticks_per_second': len(ticks) / (batch_time / 1000),
            'alerts': len(batch_alerts)
        }
    }
    
    return results


if __name__ == "__main__":
    # Demo: Test risk service
    async def demo():
        from .feed import Tick
        import random
        
        # Generate test data with some volatility
        test_ticks = []
        base_price = 150.0
        
        for i in range(1000):
            # Add some volatility
            price_change = random.gauss(0, base_price * 0.01)
            base_price += price_change
            base_price = max(base_price, 1.0)
            
            tick = Tick(
                'AAPL',
                round(base_price, 2),
                random.randint(100, 5000),
                time.time()
            )
            test_ticks.append(tick)
        
        print("Testing risk service...")
        comparison = await compare_risk_methods(test_ticks)
        
        print(f"Individual processing: {comparison['individual_processing']['time_ms']:.2f}ms")
        print(f"Batch processing: {comparison['batch_processing']['time_ms']:.2f}ms")
        print(f"Speedup: {comparison['individual_processing']['time_ms'] / comparison['batch_processing']['time_ms']:.1f}x")
        print(f"Alerts generated: {comparison['individual_processing']['alerts']}")
    
    asyncio.run(demo()) 