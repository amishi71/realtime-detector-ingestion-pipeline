# validator_service.py

"""
Validator service that runs continuously, validating packets from Redis.
"""

import time
import logging
import signal
import sys
import os
import yaml
from prometheus_client import start_http_server

from src.validator import Validator
from src.redis_consumer import RedisConsumer
from src.redis_producer import RedisProducer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path=None):
    """Load validator configuration from YAML file."""
    if config_path is None:
        possible_paths = [
            'config/validator.yaml',
            '/app/config/validator.yaml',
            '../config/validator.yaml',
            os.path.join(os.path.dirname(__file__), '../config/validator.yaml')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
    
    if not config_path or not os.path.exists(config_path):
        logger.warning("Config file not found, using defaults")
        return {}
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"✅ Loaded config from {config_path}")
            return config or {}
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


def setup_signal_handlers():
    """Handle graceful shutdown on Ctrl+C."""
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutting down validator service...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    return lambda: running


def main():
    """Main entry point for validator service."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validator Service')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--input-stream', default='sensor_packets',
                       help='Redis stream to consume from')
    parser.add_argument('--output-stream', default='validated_packets',
                       help='Redis stream to publish validated packets to')
    parser.add_argument('--group', default='validator',
                       help='Consumer group name')
    parser.add_argument('--metrics-port', type=int, default=8001,
                       help='Port for Prometheus metrics')
    args = parser.parse_args()
    
    # Start metrics server
    try:
        start_http_server(args.metrics_port)
        logger.info(f"📊 Metrics server started on port {args.metrics_port}")
    except Exception as e:
        logger.warning(f"Could not start metrics server: {e}")
    
    # Load config
    config = load_config(args.config)
    
    # Create validator
    validator = Validator(config)
    
    # Create Redis consumer and producer
    max_retries = 5
    for attempt in range(max_retries):
        try:
            consumer = RedisConsumer(
                stream=args.input_stream,
                group=args.group,
                consumer=f"{args.group}-worker"
            )
            producer = RedisProducer(stream=args.output_stream)
            logger.info(f"✅ Connected to Redis")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Redis connection failed (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(3)
            else:
                logger.error("❌ Could not connect to Redis after multiple attempts")
                sys.exit(1)
    
    # Check if we should keep running
    is_running = setup_signal_handlers()
    
    logger.info(f"🔄 Validator service started. Listening to '{args.input_stream}'")
    
    # Main validation loop
    while is_running():
        try:
            messages = consumer.read(count=10, block=1000)
            
            for msg in messages:
                packet = msg['data']
                
                # Validate packet
                validated = validator.validate(packet)
                
                # Forward to next stream
                producer.emit(validated)
                
                # Acknowledge processing
                consumer.ack(msg['id'])
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(1)
    
    logger.info("Validator service stopped")


if __name__ == "__main__":
    main()