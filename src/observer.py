# observer.py

from datetime import datetime, timezone
from prometheus_client import Counter, Gauge
import logging
import yaml
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
observer_missing_packets = Counter(
    "observer_missing_packets_total",
    "Total missing packets detected by observer",
)

observer_degraded_packets = Counter(
    "observer_degraded_packets_total",
    "Total degraded packets observed",
)

observer_recovery_events = Counter(
    "observer_recovery_events_total",
    "Number of recovery periods entered",
)

observer_packets_processed = Counter(
    "observer_packets_total",
    "Total packets processed by observer",
)

observer_current_status = Gauge(
    "observer_sensor_status",
    "Current status of each sensor",
    ["sensor_id", "status"]
)


def load_config(config_path=None):
    """Load observer configuration from YAML file."""
    if config_path is None:
        possible_paths = [
            'config/observer.yaml',
            '/app/config/observer.yaml',
            '../config/observer.yaml',
            os.path.join(os.path.dirname(__file__), '../config/observer.yaml')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
    
    if not config_path or not os.path.exists(config_path):
        logger.warning("Config file not found, using defaults")
        return {'max_time_gap': 1.5}
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"✅ Loaded config from {config_path}")
            return config or {}
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


class Observer:
    """Observes sensor packets and tracks stream health metrics per sensor."""
    
    def __init__(self, config=None):
        config = config or {}
        
        # Per-sensor state (FIXED!)
        self.last_sequence = {}      # sensor_id -> last sequence
        self.last_timestamp = {}     # sensor_id -> last timestamp
        self.in_recovery = {}        # sensor_id -> bool
        self.recovery_length = {}    # sensor_id -> count
        
        # Global counters
        self.missing_packets = 0
        self.degraded_count = 0
        
        # Thresholds from config
        self.max_time_gap = config.get('max_time_gap', 1.5)
        
        logger.info(f"✅ Observer initialized: max_time_gap={self.max_time_gap}s")

    def observe(self, packet: dict):
        """
        Observe a single sensor packet and log operational insights.
        Returns the packet unchanged.
        """
        observer_packets_processed.inc()
        
        # Extract packet fields
        sensor = packet["sensor_id"]
        seq = packet["sequence_number"]
        status = packet["status"]
        
        # Parse timestamp safely
        try:
            ts_str = packet["timestamp"].replace("Z", "+00:00")
            ts = datetime.fromisoformat(ts_str)
        except Exception as e:
            logger.warning(f"Bad timestamp for {sensor}: {e}")
            return packet  # Skip timestamp checks, but still track sequence

        # Initialize sensor state if needed
        if sensor not in self.last_sequence:
            self.last_sequence[sensor] = seq
            self.last_timestamp[sensor] = ts
            self.in_recovery[sensor] = False
            self.recovery_length[sensor] = 0
            observer_current_status.labels(sensor_id=sensor, status=status).set(1)
            return packet

        # Sequence integrity 
        last_seq = self.last_sequence[sensor]
        expected = last_seq + 1

        if seq != expected:
            missed = max(0, seq - expected)
            if missed > 0:
                self.missing_packets += missed
                observer_missing_packets.inc(missed)
                logger.warning(f"⚠️ Sensor {sensor}: Missing {missed} packets "
                              f"(expected {expected}, got {seq})")

        # Timestamp spacing
        last_ts = self.last_timestamp[sensor]
        delta = (ts - last_ts).total_seconds()
        
        if delta > self.max_time_gap:
            logger.warning(f"⚠️ Sensor {sensor}: Time gap of {delta:.2f}s detected "
                          f"(threshold: {self.max_time_gap}s)")
        # Status transitions
        # Initialize state tracker if needed
        if not hasattr(self, '_last_status'):
            self._last_status = {}
        
        # Only log when status changes
        if status == "DEGRADED":
            self.degraded_count += 1
            observer_degraded_packets.inc()
            
            # Log only on transition into DEGRADED
            if self._last_status.get(sensor) != "DEGRADED":
                logger.info(f"⚠️ Sensor {sensor} entered DEGRADED state")
                observer_current_status.labels(sensor_id=sensor, status="DEGRADED").set(1)

        elif status == "RECOVERING":
            if not self.in_recovery.get(sensor, False):
                self.in_recovery[sensor] = True
                self.recovery_length[sensor] = 0
                observer_recovery_events.inc()
                
                # Log only on transition into RECOVERING
                if self._last_status.get(sensor) != "RECOVERING":
                    logger.info(f"ℹ️ Sensor {sensor} entered RECOVERING state")
                    observer_current_status.labels(sensor_id=sensor, status="RECOVERING").set(1)
            
            self.recovery_length[sensor] += 1
        
        elif status == "NOMINAL":
            if self.in_recovery.get(sensor, False):
                logger.info(f"✅ Sensor {sensor} recovered after "
                           f"{self.recovery_length[sensor]} packets")
                self.in_recovery[sensor] = False
            
            # Log only on transition into NOMINAL
            if self._last_status.get(sensor) != "NOMINAL":
                observer_current_status.labels(sensor_id=sensor, status="NOMINAL").set(1)
        
        # Store current status for next comparison
        self._last_status[sensor] = status

        # Update memory 
        self.last_sequence[sensor] = seq
        self.last_timestamp[sensor] = ts

        return packet

    def get_stats(self):
        """Return current observer statistics."""
        return {
            'total_missing_packets': self.missing_packets,
            'total_degraded': self.degraded_count,
            'sensors_tracked': len(self.last_sequence),
            'in_recovery': [s for s, v in self.in_recovery.items() if v],
        }

        #  Update memory 
        self.last_sequence[sensor] = seq
        self.last_timestamp[sensor] = ts

        return packet

    def get_stats(self):
        """Return current observer statistics."""
        return {
            'total_missing_packets': self.missing_packets,
            'total_degraded': self.degraded_count,
            'sensors_tracked': len(self.last_sequence),
            'in_recovery': [s for s, v in self.in_recovery.items() if v],
        }
# Service entry point (if run directly)
if __name__ == "__main__":
    import time
    import sys
    import signal
    import argparse
    from src.redis_consumer import RedisConsumer
    from src.redis_producer import RedisProducer
    from prometheus_client import start_http_server
    
    class ObserverService:
        def __init__(self):
            self.running = True
            
        def signal_handler(self, sig, frame):
            logger.info("Shutting down observer...")
            self.running = False
    
    # Parse args
    parser = argparse.ArgumentParser(description='Observer Service')
    parser.add_argument('--input-stream', default='validated_packets')
    parser.add_argument('--output-stream', default='observed_packets')
    parser.add_argument('--group', default='observer')
    parser.add_argument('--metrics-port', type=int, default=8002)
    args = parser.parse_args()
    
    # Start metrics
    try:
        start_http_server(args.metrics_port)
        logger.info(f"📊 Metrics server started on port {args.metrics_port}")
    except Exception as e:
        logger.warning(f"Could not start metrics server: {e}")
    
    # Load config
    config = load_config()
    
    # Create observer
    observer = Observer(config)
    
    # Redis connections
    max_retries = 5
    for attempt in range(max_retries):
        try:
            consumer = RedisConsumer(stream=args.input_stream, group=args.group)
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
    
    # Create service
    service = ObserverService()
    signal.signal(signal.SIGINT, service.signal_handler)
    signal.signal(signal.SIGTERM, service.signal_handler)
    
    logger.info(f"🔄 Observer listening to {args.input_stream} → {args.output_stream}")
    
    # Main loop
    while service.running:
        try:
            messages = consumer.read(count=10, block=1000)
            for msg in messages:
                observed = observer.observe(msg['data'])
                producer.emit(observed)
                consumer.ack(msg['id'])
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(1)
    
    # Print final stats
    stats = observer.get_stats()
    logger.info(f" Final stats: {stats}")
    logger.info("Observer stopped")