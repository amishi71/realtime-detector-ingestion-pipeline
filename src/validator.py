# validator.py

from datetime import datetime, timedelta, timezone
from prometheus_client import Counter
import logging
import time
import signal
import sys
import os
import yaml

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
VALIDATOR_VIOLATIONS = Counter(
    "validator_violations_total",
    "Total validator violations",
    ["type"]
)

VALIDATOR_PACKETS_PROCESSED = Counter(
    "validator_packets_total",
    "Total packets processed by validator"
)


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


class Validator:
    """
    Declares physical invariants of the telemetry universe.
    Detects violations but never mutates or drops packets.
    """

    def __init__(self, config=None):
        config = config or {}
        
        self.max_future_skew_seconds = config.get('max_future_skew_seconds', 5)
        self.max_backward_skew_seconds = config.get('max_backward_skew_seconds', 2)
        
        self.max_future_skew = timedelta(seconds=self.max_future_skew_seconds)
        self.max_backward_skew = timedelta(seconds=self.max_backward_skew_seconds)
        
        # Valid status values (configurable)
        self.valid_statuses = config.get('valid_statuses', {
            "NOMINAL", "DEGRADED", "RECOVERING", "OK", "MISSING"
        })

        self.last_sequence = {}   # per sensor
        self.last_timestamp = {}  # per sensor
        self.violation_count = 0
        
        logger.info(f"✅ Validator initialized: "
                   f"future_skew={self.max_future_skew_seconds}s, "
                   f"backward_skew={self.max_backward_skew_seconds}s")

    def validate(self, packet: dict):
        """
        Validate a packet and return it unchanged.
        All violations are logged, not raised.
        """
        VALIDATOR_PACKETS_PROCESSED.inc()
        
        self._check_schema(packet)
        self._check_sequence(packet)
        self._check_timestamp(packet)

        return packet

    # Checks 

    def _check_schema(self, packet):
        REQUIRED_FIELDS = {
            "sensor_id": str,
            "sequence_number": int,
            "timestamp": str,
            "value": (int, float),
            "status": str,
        }

        for field, expected_type in REQUIRED_FIELDS.items():
            if field not in packet:
                self._violation(
                    f"Missing field: {field}",
                    vtype="schema_missing_field",
                )
                return

            if not isinstance(packet[field], expected_type):
                self._violation(
                    f"Field '{field}' has invalid type "
                    f"(expected {expected_type}, got {type(packet[field])})",
                    vtype="schema_invalid_type",
                )

        # Check status value
        status = packet["status"]
        if status not in self.valid_statuses:
            self._violation(
                f"Unknown status value: '{status}' (expected one of {self.valid_statuses})",
                vtype="schema_unknown_status",
            )

    def _check_sequence(self, packet):
        sensor = packet["sensor_id"]
        seq = packet["sequence_number"]

        last = self.last_sequence.get(sensor)
        if last is not None and seq < last:
            self._violation(
                f"Sequence went backwards for {sensor} "
                f"(last={last}, got={seq})",
                vtype="sequence_backwards",
            )

        self.last_sequence[sensor] = max(seq, last) if last is not None else seq

    def _check_timestamp(self, packet):
        sensor = packet["sensor_id"]

        try:
            # Handle Z suffix properly
            ts_str = packet["timestamp"].replace("Z", "+00:00")
            ts = datetime.fromisoformat(ts_str)
        except Exception as e:
            self._violation(
                f"Invalid timestamp format: {packet['timestamp']} ({e})",
                vtype="timestamp_parse_error",
            )
            return

        now = datetime.now(timezone.utc)

        if ts - now > self.max_future_skew:
            self._violation(
                f"Timestamp too far in future for {sensor}: {ts.isoformat()} "
                f"(skew={(ts-now).total_seconds():.1f}s > {self.max_future_skew_seconds}s)",
                vtype="future_timestamp",
            )

        last_ts = self.last_timestamp.get(sensor)
        if last_ts and last_ts - ts > self.max_backward_skew:
            self._violation(
                f"Timestamp went backwards for {sensor} "
                f"(last={last_ts.isoformat()}, got={ts.isoformat()}, "
                f"delta={(last_ts-ts).total_seconds():.1f}s)",
                vtype="timestamp_backwards",
            )

        self.last_timestamp[sensor] = max(ts, last_ts) if last_ts else ts

    # Reporting 
    def _violation(self, message, vtype="unknown"):
        self.violation_count += 1
        VALIDATOR_VIOLATIONS.labels(type=vtype).inc()
        logger.warning(f"🔴 VALIDATION VIOLATION [{vtype}]: {message}")
    
    def get_stats(self):
        """Return current validator statistics."""
        return {
            'violations': self.violation_count,
            'sensors_tracked': len(self.last_sequence),
            'last_sequences': self.last_sequence,
        }


def setup_signal_handlers():
    """Handle graceful shutdown on Ctrl+C."""
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutting down validator...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    return lambda: running


def process_packet(validator, packet):
    """Process a single packet through the validator."""
    try:
        validated = validator.validate(packet)
        logger.debug(f"✅ Validated: {packet['sensor_id']} seq={packet['sequence_number']}")
        return validated
    except Exception as e:
        logger.error(f"Error validating packet: {e}")
        return packet  # Return original on error


def main():
    """Main entry point for validator service."""
    import argparse
    from src.redis_consumer import RedisConsumer
    from src.redis_producer import RedisProducer
    
    parser = argparse.ArgumentParser(description='Telemetry Validator')
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
        from prometheus_client import start_http_server
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
    
    logger.info(f"🔄 Validator started. Listening to '{args.input_stream}' -> '{args.output_stream}'")
    
    # Main validation loop
    while is_running():
        try:
            # Read messages from Redis
            messages = consumer.read(count=10, block=1000)
            
            for msg in messages:
                packet = msg['data']
                
                # Validate packet
                validated = process_packet(validator, packet)
                
                # Forward to next stream
                producer.emit(validated)
                
                # Acknowledge processing
                consumer.ack(msg['id'])
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(1)
    
    logger.info("Validator stopped")


if __name__ == "__main__":
    main()