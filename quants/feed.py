"""
Async WebSocket market data feed.

Demonstrates pure async/await for network I/O without threads.
Bypasses GIL for network operations.
"""

import asyncio
import json
import random
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional
import aiohttp
from aiohttp import web


@dataclass
class Tick:
    """Market data tick with price, volume, and timestamp."""
    symbol: str
    price: float
    volume: int
    timestamp: float
    
    def __post_init__(self):
        # Ensure timestamp is set if not provided
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def to_json(self) -> str:
        """Serialize tick to JSON string."""
        return json.dumps({
            'symbol': self.symbol,
            'price': self.price,
            'volume': self.volume,
            'timestamp': self.timestamp
        })
    
    @classmethod
    def from_json(cls, data: str) -> 'Tick':
        """Deserialize tick from JSON string."""
        obj = json.loads(data)
        return cls(**obj)


class MockWebSocketServer:
    """Mock WebSocket server that generates market data ticks."""
    
    def __init__(self, tick_rate: int = 1000):
        self.tick_rate = tick_rate
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        self.base_prices = {
            'AAPL': 150.0,
            'GOOGL': 2800.0,
            'MSFT': 300.0,
            'AMZN': 3300.0,
            'TSLA': 800.0
        }
        self.app = web.Application()
        self.app.router.add_get('/ws', self.websocket_handler)
        self.running = False
    
    async def websocket_handler(self, request):
        """Handle WebSocket connections."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        print(f"WebSocket client connected")
        
        try:
            while self.running:
                tick = self._generate_tick()
                await ws.send_str(tick.to_json())
                await asyncio.sleep(1.0 / self.tick_rate)
        except Exception as e:
            print(f"WebSocket error: {e}")
        finally:
            await ws.close()
            print(f"WebSocket client disconnected")
        
        return ws
    
    def _generate_tick(self) -> Tick:
        """Generate a random market tick."""
        symbol = random.choice(self.symbols)
        base_price = self.base_prices[symbol]
        
        # Add some random walk to price
        price_change = random.gauss(0, base_price * 0.001)  # 0.1% std dev
        price = base_price + price_change
        price = max(price, 0.01)  # Ensure positive price
        
        # Update base price for next tick
        self.base_prices[symbol] = price
        
        # Random volume between 100 and 10000
        volume = random.randint(100, 10000)
        
        return Tick(
            symbol=symbol,
            price=round(price, 2),
            volume=volume,
            timestamp=time.time()
        )
    
    async def start(self, host: str = 'localhost', port: int = 8765):
        """Start the WebSocket server."""
        self.running = True
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        print(f"Mock WebSocket server running at ws://{host}:{port}")
        return runner
    
    async def stop(self, runner):
        """Stop the WebSocket server."""
        self.running = False
        await runner.cleanup()


class MarketDataFeed:
    """Async market data feed that connects to WebSocket and yields ticks."""
    
    def __init__(self, url: str = 'ws://localhost:8765/ws'):
        self.url = url
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
    
    async def connect(self):
        """Connect to the WebSocket feed."""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        self.ws = await self.session.ws_connect(self.url)
        print(f"Connected to market data feed: {self.url}")
    
    async def disconnect(self):
        """Disconnect from the WebSocket feed."""
        if self.ws:
            await self.ws.close()
            self.ws = None
        if self.session:
            await self.session.close()
            self.session = None
    
    async def ticks(self) -> AsyncIterator[Tick]:
        """Yield ticks from the WebSocket feed."""
        if not self.ws:
            await self.connect()
        
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    tick = Tick.from_json(msg.data)
                    yield tick
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f"WebSocket error: {self.ws.exception()}")
                    break
        except Exception as e:
            print(f"Error reading from WebSocket: {e}")
            raise


async def start_mock_server(tick_rate: int = 1000) -> MockWebSocketServer:
    """Start a mock WebSocket server for testing."""
    server = MockWebSocketServer(tick_rate)
    runner = await server.start()
    return server, runner


if __name__ == "__main__":
    # Demo: Start mock server and connect to it
    async def demo():
        # Start mock server
        server, runner = await start_mock_server(tick_rate=100)
        
        try:
            # Connect to feed
            async with MarketDataFeed() as feed:
                count = 0
                async for tick in feed.ticks():
                    print(f"Tick {count}: {tick}")
                    count += 1
                    if count >= 10:
                        break
        finally:
            await server.stop(runner)
    
    asyncio.run(demo()) 