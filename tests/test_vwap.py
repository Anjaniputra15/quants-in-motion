"""
Tests for VWAP calculator implementations.

Demonstrates testing different VWAP implementations and performance characteristics.
"""

import pytest
import asyncio
import time
import random
from quants.feed import Tick
from quants.vwap import VWAPCalculator, VWAPResult


class TestVWAPCalculator:
    """Test VWAP calculator implementations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = VWAPCalculator(window_size=10)
        self.test_ticks = [
            Tick('AAPL', 150.0, 100, time.time()),
            Tick('AAPL', 151.0, 200, time.time()),
            Tick('AAPL', 152.0, 150, time.time()),
            Tick('GOOGL', 2800.0, 50, time.time()),
            Tick('GOOGL', 2801.0, 75, time.time()),
        ]
    
    def test_vwap_calculation_pure_python(self):
        """Test pure Python VWAP calculation."""
        # Calculate expected VWAP manually
        total_price_volume = 0
        total_volume = 0
        
        for tick in self.test_ticks[:3]:  # First 3 AAPL ticks
            total_price_volume += tick.price * tick.volume
            total_volume += tick.volume
        
        expected_vwap = total_price_volume / total_volume
        
        # Test pure Python implementation
        result = self.calculator._calculate_vwap_pure_python(self.test_ticks[:3])
        
        assert result.symbol == 'AAPL'
        assert abs(result.vwap - expected_vwap) < 0.001
        assert result.total_volume == total_volume
        assert result.tick_count == 3
    
    def test_vwap_calculation_numpy(self):
        """Test NumPy VWAP calculation."""
        # Calculate expected VWAP manually
        total_price_volume = 0
        total_volume = 0
        
        for tick in self.test_ticks[:3]:  # First 3 AAPL ticks
            total_price_volume += tick.price * tick.volume
            total_volume += tick.volume
        
        expected_vwap = total_price_volume / total_volume
        
        # Test NumPy implementation
        result = self.calculator._calculate_vwap_numpy(self.test_ticks[:3])
        
        assert result.symbol == 'AAPL'
        assert abs(result.vwap - expected_vwap) < 0.001
        assert result.total_volume == total_volume
        assert result.tick_count == 3
    
    def test_empty_ticks(self):
        """Test VWAP calculation with empty tick list."""
        result_python = self.calculator._calculate_vwap_pure_python([])
        result_numpy = self.calculator._calculate_vwap_numpy([])
        
        assert result_python.vwap == 0.0
        assert result_python.total_volume == 0
        assert result_python.tick_count == 0
        
        assert result_numpy.vwap == 0.0
        assert result_numpy.total_volume == 0
        assert result_numpy.tick_count == 0
    
    def test_single_tick(self):
        """Test VWAP calculation with single tick."""
        single_tick = [self.test_ticks[0]]
        
        result_python = self.calculator._calculate_vwap_pure_python(single_tick)
        result_numpy = self.calculator._calculate_vwap_numpy(single_tick)
        
        expected_vwap = single_tick[0].price
        
        assert result_python.vwap == expected_vwap
        assert result_numpy.vwap == expected_vwap
    
    def test_window_size_limiting(self):
        """Test that VWAP respects window size."""
        # Add more ticks than window size
        for i in range(15):
            tick = Tick('AAPL', 150.0 + i, 100, time.time())
            self.calculator._add_tick_to_buffer(tick)
        
        # Check that only window_size ticks are kept
        buffer = self.calculator.tick_buffers['AAPL']
        assert len(buffer) == 10  # window_size
        
        # Check that oldest ticks were removed
        assert buffer[0].price == 150.0 + 5  # 6th tick (0-indexed)
        assert buffer[-1].price == 150.0 + 14  # 15th tick
    
    @pytest.mark.asyncio
    async def test_async_process_tick(self):
        """Test async tick processing."""
        tick = self.test_ticks[0]
        
        result = await self.calculator.process_tick(tick)
        
        assert result is not None
        assert result.symbol == 'AAPL'
        assert result.vwap == tick.price
        assert result.total_volume == tick.volume
        assert result.tick_count == 1
    
    def test_statistics(self):
        """Test statistics collection."""
        # Process some ticks
        for tick in self.test_ticks[:3]:
            self.calculator._add_tick_to_buffer(tick)
        
        stats = self.calculator.get_statistics()
        
        assert stats['total_ticks_processed'] == 0  # Not processed through async method
        assert stats['symbols_tracked'] == 1  # Only AAPL
        assert stats['implementation'] == 'numpy'  # Default implementation


class TestVWAPPerformance:
    """Test VWAP performance characteristics."""
    
    def test_python_vs_numpy_performance(self):
        """Compare performance of Python vs NumPy implementations."""
        calculator = VWAPCalculator()
        
        # Generate test data
        test_ticks = [
            Tick('AAPL', 150.0 + i * 0.1, random.randint(100, 1000), time.time())
            for i in range(1000)
        ]
        
        # Time Python implementation
        start_time = time.perf_counter()
        result_python = calculator._calculate_vwap_pure_python(test_ticks)
        python_time = (time.perf_counter() - start_time) * 1000
        
        # Time NumPy implementation
        start_time = time.perf_counter()
        result_numpy = calculator._calculate_vwap_numpy(test_ticks)
        numpy_time = (time.perf_counter() - start_time) * 1000
        
        # Both should give same result
        assert abs(result_python.vwap - result_numpy.vwap) < 0.001
        
        # NumPy should be faster (though this may vary)
        print(f"Python time: {python_time:.2f}ms")
        print(f"NumPy time: {numpy_time:.2f}ms")
        print(f"Speedup: {python_time / numpy_time:.1f}x")
        
        # NumPy should be at least as fast as Python
        assert numpy_time <= python_time * 1.5  # Allow some variance
    
    @pytest.mark.asyncio
    async def test_multiprocessing_performance(self):
        """Test multiprocessing VWAP performance."""
        # Test with multiprocessing enabled
        calculator_mp = VWAPCalculator(use_multiprocessing=True, num_cores=2)
        
        # Generate test data
        test_ticks = [
            Tick('AAPL', 150.0 + i * 0.1, random.randint(100, 1000), time.time())
            for i in range(100)
        ]
        
        # Process ticks
        for tick in test_ticks:
            result = await calculator_mp.process_tick(tick)
            assert result is not None
        
        stats = calculator_mp.get_statistics()
        assert stats['implementation'] == 'multiprocess'
        assert stats['total_ticks_processed'] == 100


class TestVWAPEdgeCases:
    """Test VWAP edge cases and error conditions."""
    
    def test_zero_volume(self):
        """Test VWAP calculation with zero volume."""
        zero_volume_ticks = [
            Tick('AAPL', 150.0, 0, time.time()),
            Tick('AAPL', 151.0, 0, time.time()),
        ]
        
        calculator = VWAPCalculator()
        
        result = calculator._calculate_vwap_numpy(zero_volume_ticks)
        assert result.vwap == 0.0
        assert result.total_volume == 0
    
    def test_negative_prices(self):
        """Test VWAP calculation with negative prices."""
        negative_price_ticks = [
            Tick('AAPL', -150.0, 100, time.time()),
            Tick('AAPL', -151.0, 200, time.time()),
        ]
        
        calculator = VWAPCalculator()
        
        result = calculator._calculate_vwap_numpy(negative_price_ticks)
        expected_vwap = (-150.0 * 100 + -151.0 * 200) / 300
        assert abs(result.vwap - expected_vwap) < 0.001
    
    def test_mixed_symbols(self):
        """Test VWAP calculation with mixed symbols."""
        mixed_ticks = [
            Tick('AAPL', 150.0, 100, time.time()),
            Tick('GOOGL', 2800.0, 50, time.time()),
            Tick('AAPL', 151.0, 200, time.time()),
        ]
        
        calculator = VWAPCalculator()
        
        # Should only calculate VWAP for the symbol of the last tick
        result = calculator._calculate_vwap_numpy(mixed_ticks)
        assert result.symbol == 'AAPL'  # Last tick symbol
        
        # Calculate expected VWAP for AAPL ticks only
        aapl_ticks = [tick for tick in mixed_ticks if tick.symbol == 'AAPL']
        expected_vwap = (150.0 * 100 + 151.0 * 200) / 300
        assert abs(result.vwap - expected_vwap) < 0.001


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"]) 