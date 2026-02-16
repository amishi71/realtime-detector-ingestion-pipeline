# redis_producer.py

import redis
import json
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisProducer:
    """Produces messages to Redis streams."""
    
    def __init__(self, stream="sensor_packets"):
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.stream = stream
        self.client = None
        self._connect()
    
    def _connect(self):
        """Establish Redis connection with retry logic."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.client.ping()
            logger.info(f"✅ Connected to Redis at {self.host}:{self.port}")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            logger.warning("Will retry on next emit...")
            self.client = None
    
    def emit(self, packet: dict):
        """Send a packet to Redis stream."""
        if self.client is None:
            self._connect()  # Retry connection
            
        if self.client is None:
            logger.error("Cannot emit: Redis not connected")
            return False
        
        try:
            # Convert all values to JSON strings
            payload = {}
            for k, v in packet.items():
                if isinstance(v, (dict, list, bool)):
                    payload[k] = json.dumps(v)
                elif v is None:
                    payload[k] = "null"
                else:
                    payload[k] = str(v)
            
            # Add to stream
            self.client.xadd(self.stream, payload)
            return True
        except Exception as e:
            logger.error(f"Failed to emit packet: {e}")
            # Mark connection as dead to trigger reconnect
            self.client = None
            return False


class RedisConsumer:
    """Consumes messages from Redis streams with consumer groups."""
    
    def __init__(self, stream, group, consumer_name=None):
        self.host = os.getenv('REDIS_HOST', 'localhost')
        self.port = int(os.getenv('REDIS_PORT', 6379))
        self.stream = stream
        self.group = group
        self.consumer = consumer_name or f"{group}-{os.getpid()}"
        self.client = None
        self._connect()
    
    def _connect(self):
        """Establish Redis connection."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=True,
                socket_connect_timeout=2
            )
            # Test connection
            self.client.ping()
            
            # Create consumer group if not exists
            try:
                self.client.xgroup_create(self.stream, self.group, mkstream=True)
                logger.info(f"✅ Created consumer group '{self.group}' for stream '{self.stream}'")
            except redis.exceptions.ResponseError:
                # Group already exists
                logger.debug(f"Consumer group '{self.group}' already exists")
                
            logger.info(f"✅ Consumer '{self.consumer}' connected to {self.host}:{self.port}")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            self.client = None
    
    def read(self, count=10, block=1000):
        """
        Read messages from the stream.
        
        Args:
            count: Maximum number of messages to read
            block: Milliseconds to block waiting for messages
        
        Returns:
            List of messages or empty list
        """
        if self.client is None:
            self._connect()
            
        if self.client is None:
            logger.warning("Redis not connected, cannot read")
            return []
        
        try:
            # Read from consumer group
            messages = self.client.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams={self.stream: '>'},
                count=count,
                block=block
            )
            
            result = []
            for stream, msgs in messages:
                for msg_id, msg_data in msgs:
                    # Parse JSON values back to original types
                    parsed = {}
                    for k, v in msg_data.items():
                        try:
                            parsed[k] = json.loads(v)
                        except (json.JSONDecodeError, TypeError):
                            parsed[k] = v
                    
                    result.append({
                        'id': msg_id,
                        'data': parsed
                    })
            
            return result
        except Exception as e:
            logger.error(f"Error reading from Redis: {e}")
            self.client = None
            return []
    
    def ack(self, message_id):
        """Acknowledge a message was processed."""
        if self.client is None:
            return False
        
        try:
            self.client.xack(self.stream, self.group, message_id)
            return True
        except Exception as e:
            logger.error(f"Failed to ack message {message_id}: {e}")
            return False
    
    def pending(self):
        """Get pending messages for this consumer."""
        if self.client is None:
            return []
        
        try:
            pending = self.client.xpending_range(
                self.stream, self.group, min='-', max='+', count=100
            )
            return pending
        except Exception as e:
            logger.error(f"Failed to get pending messages: {e}")
            return []
    
    def claim_stale(self, min_idle_time=60000):
        """
        Claim stale pending messages from other consumers.
        
        Args:
            min_idle_time: Minimum idle time in milliseconds
        """
        if self.client is None:
            return []
        
        try:
            # Get pending messages summary
            pending_info = self.client.xpending(self.stream, self.group)
            if not pending_info or pending_info['pending'] == 0:
                return []
            
            # Find stale messages from any consumer
            stale = self.client.xpending_range(
                self.stream, self.group,
                min='-', max='+',
                count=100
            )
            
            # Filter stale messages
            import time
            now = int(time.time() * 1000)
            stale_ids = [
                msg['message_id'] for msg in stale
                if (now - msg['time_since_delivered']) > min_idle_time
            ]
            
            if stale_ids:
                claimed = self.client.xclaim(
                    self.stream, self.group, self.consumer,
                    min_idle_time, stale_ids
                )
                return claimed
            
            return []
        except Exception as e:
            logger.error(f"Failed to claim stale messages: {e}")
            return []


# Example usage (when run directly)
if __name__ == "__main__":
    # Test producer
    producer = RedisProducer()
    test_packet = {
        "sensor_id": "test_001",
        "value": 42.5,
        "timestamp": "2026-02-15T12:00:00Z",
        "status": "NOMINAL"
    }
    producer.emit(test_packet)
    print("✅ Test packet emitted")
    
    # Test consumer
    consumer = RedisConsumer(stream="sensor_packets", group="test_group")
    msgs = consumer.read(block=2000)
    print(f"📨 Received {len(msgs)} messages")
    for msg in msgs:
        print(f"  - {msg['data']}")
        consumer.ack(msg['id'])