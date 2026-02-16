# redis_consumer.py

import redis
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisConsumer:
    """
    Consumes messages from Redis streams with consumer groups.
    Supports both callback-based (consume) and manual (read) patterns.
    """
    
    def __init__(
        self,
        stream="sensor_packets",
        group="pipeline",
        consumer=None,
    ):
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.stream = stream
        self.group = group
        self.consumer = consumer or f"{group}-{os.getpid()}"
        self.client = None
        self.running = True
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
            
            # Create consumer group if it doesn't exist
            try:
                self.client.xgroup_create(
                    self.stream,
                    self.group,
                    id="0",
                    mkstream=True,
                )
                logger.info(f"✅ Created consumer group '{self.group}' for stream '{self.stream}'")
            except redis.exceptions.ResponseError:
                # Group already exists
                logger.debug(f"Consumer group '{self.group}' already exists")
            
            logger.info(f"✅ RedisConsumer '{self.consumer}' connected to {self.host}:{self.port}")
            return True
            
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            self.client = None
            return False
    
    def _ensure_connection(self):
        """Ensure we have a working connection."""
        if self.client is None:
            return self._connect()
        
        try:
            self.client.ping()
            return True
        except redis.exceptions.ConnectionError:
            logger.warning("Redis connection lost, reconnecting...")
            return self._connect()
    
    def read(self, count=10, block=1000):
        """
        Read messages from the stream (manual mode).
        
        Args:
            count: Maximum number of messages to read
            block: Milliseconds to block waiting for messages
        
        Returns:
            List of messages or empty list
        """
        if not self._ensure_connection():
            return []
        
        try:
            messages = self.client.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams={self.stream: ">"},
                count=count,
                block=block,
            )
            
            result = []
            for stream, entries in messages:
                for msg_id, fields in entries:
                    # Parse JSON values
                    packet = {}
                    for k, v in fields.items():
                        try:
                            packet[k] = json.loads(v)
                        except (json.JSONDecodeError, TypeError):
                            packet[k] = v
                    
                    result.append({
                        'id': msg_id,
                        'data': packet
                    })
            
            return result
            
        except redis.exceptions.ConnectionError:
            logger.error("Redis connection lost during read")
            self.client = None
            return []
        except Exception as e:
            logger.error(f"Error reading from Redis: {e}")
            return []
    
    def consume(self, handler):
        """
        Continuously consume messages and call handler (callback mode).
        
        Args:
            handler: Function that takes a packet dict and processes it
        """
        logger.info(f"🔄 Starting consumer '{self.consumer}' on stream '{self.stream}'")
        
        while self.running:
            if not self._ensure_connection():
                logger.warning("Waiting 5s before reconnecting...")
                time.sleep(5)
                continue
            
            try:
                messages = self.client.xreadgroup(
                    self.group,
                    self.consumer,
                    {self.stream: ">"},
                    count=1,  # Process one at a time for fairness
                    block=1000,  # Block for 1 second
                )
                
                if not messages:
                    continue
                
                _, entries = messages[0]
                for msg_id, fields in entries:
                    # Parse packet
                    packet = {}
                    for k, v in fields.items():
                        try:
                            packet[k] = json.loads(v)
                        except (json.JSONDecodeError, TypeError):
                            packet[k] = v
                    
                    # Process with handler
                    try:
                        handler(packet)
                        # Acknowledge success
                        self.client.xack(self.stream, self.group, msg_id)
                        logger.debug(f"✅ Processed and acked message {msg_id}")
                    except Exception as e:
                        logger.error(f"❌ Handler failed for message {msg_id}: {e}")
                        # Don't ack - will be retried
                        
            except redis.exceptions.ConnectionError:
                logger.error("Redis connection lost")
                self.client = None
            except Exception as e:
                logger.error(f"Unexpected error in consume loop: {e}")
                time.sleep(1)  # Prevent tight loop on error
    
    def ack(self, message_id):
        """Acknowledge a specific message was processed."""
        if not self._ensure_connection():
            return False
        
        try:
            self.client.xack(self.stream, self.group, message_id)
            return True
        except Exception as e:
            logger.error(f"Failed to ack message {message_id}: {e}")
            return False
    
    def pending(self, count=100):
        """Get pending messages for this consumer."""
        if not self._ensure_connection():
            return []
        
        try:
            pending = self.client.xpending_range(
                self.stream, 
                self.group, 
                min='-', 
                max='+', 
                count=count
            )
            return pending
        except Exception as e:
            logger.error(f"Failed to get pending messages: {e}")
            return []
    
    def stop(self):
        """Stop the consume loop."""
        self.running = False
        logger.info("Consumer stopping...")


# Example usage (when run directly)
if __name__ == "__main__":
    def test_handler(packet):
        print(f"📨 Received: {packet}")
    
    # Test with manual read
    print("Testing manual read mode...")
    consumer = RedisConsumer(
        stream="sensor_packets",
        group="test_group",
        consumer="test_worker"
    )
    
    # Read a few messages
    msgs = consumer.read(count=5, block=2000)
    print(f"Read {len(msgs)} messages")
    for msg in msgs:
        print(f"  - {msg['data']}")
        consumer.ack(msg['id'])
    
    # Test consume mode (runs forever - uncomment to test)
    # print("Starting consume mode (Ctrl+C to stop)...")
    # try:
    #     consumer.consume(test_handler)
    # except KeyboardInterrupt:
    #     consumer.stop()
    #     print("Stopped")