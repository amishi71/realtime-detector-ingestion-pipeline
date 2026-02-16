# downstream_model.py

from collections import deque
from prometheus_client import Gauge, Counter, start_http_server
import logging
import time
import signal
import sys
import os
import yaml
from threading import Lock

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
raw_error = Gauge(
    "downstream_raw_prediction_error",
    "Absolute prediction error on raw telemetry",
    ["sensor"],
)

clean_error = Gauge(
    "downstream_clean_prediction_error",
    "Absolute prediction error on cleaned telemetry (in raw units)",
    ["sensor"],
)

raw_predictions = Counter(
    "downstream_raw_predictions_total",
    "Total predictions made on raw stream",
    ["sensor"]
)

clean_predictions = Counter(
    "downstream_clean_predictions_total",
    "Total predictions made on clean stream",
    ["sensor"]
)

model_errors = Counter(
    "downstream_model_errors_total",
    "Total errors in model processing",
    ["stream_type", "error_type"]
)

current_window_size = Gauge(
    "downstream_window_size",
    "Current window size for moving average",
    ["sensor", "stream_type"]
)


def load_config(config_path=None):
    """Load model configuration from YAML file."""
    if config_path is None:
        possible_paths = [
            'config/model.yaml',
            '/app/config/model.yaml',
            '../config/model.yaml',
            os.path.join(os.path.dirname(__file__), '../config/model.yaml')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
    
    if not config_path or not os.path.exists(config_path):
        logger.warning("Config file not found, using defaults")
        return {'window_size': 5}
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"✅ Loaded config from {config_path}")
            return config or {}
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


class MovingAverageModel:
    def __init__(self, config=None):
        config = config or {}
        self.window_size = config.get('window_size', 5)
        
        # Per-sensor state with thread safety
        self.raw_windows = {}      # sensor_id -> deque of raw values
        self.clean_windows = {}    # sensor_id -> deque of z-scores
        self.locks = {}             # sensor_id -> Lock
        
        # Statistics
        self.stats = {
            'raw_predictions': {},
            'clean_predictions': {},
            'raw_errors': {},
            'clean_errors': {}
        }
        
        logger.info(f"✅ Model initialized with window_size={self.window_size}")

    def _get_lock(self, sensor):
        """Get or create a lock for a sensor."""
        if sensor not in self.locks:
            self.locks[sensor] = Lock()
        return self.locks[sensor]

    # RAW STREAM
    def handle_raw(self, packet):
        """Process a packet from the raw stream."""
        try:
            sensor = packet["sensor_id"]
            value = float(packet["value"])
            
            with self._get_lock(sensor):
                # Get or create window for this sensor
                if sensor not in self.raw_windows:
                    self.raw_windows[sensor] = deque(maxlen=self.window_size)
                    current_window_size.labels(sensor=sensor, stream_type='raw').set(self.window_size)
                
                window = self.raw_windows[sensor]
                
                # Make prediction if window is full
                if len(window) == self.window_size:
                    prediction = sum(window) / len(window)
                    error = abs(prediction - value)
                    
                    # Update metrics
                    raw_error.labels(sensor=sensor).set(error)
                    raw_predictions.labels(sensor=sensor).inc()
                    
                    # Track stats
                    if sensor not in self.stats['raw_predictions']:
                        self.stats['raw_predictions'][sensor] = 0
                        self.stats['raw_errors'][sensor] = []
                    
                    self.stats['raw_predictions'][sensor] += 1
                    self.stats['raw_errors'][sensor].append(error)
                    
                    logger.debug(f"RAW[{sensor}]: pred={prediction:.3f}, "
                                f"actual={value:.3f}, error={error:.3f}")
                
                # Update window
                window.append(value)
                
        except KeyError as e:
            logger.error(f"Missing key in raw packet: {e}")
            model_errors.labels(stream_type='raw', error_type='missing_key').inc()
        except ValueError as e:
            logger.error(f"Invalid value in raw packet: {e}")
            model_errors.labels(stream_type='raw', error_type='invalid_value').inc()
        except Exception as e:
            logger.error(f"Unexpected error in raw handler: {e}")
            model_errors.labels(stream_type='raw', error_type='unknown').inc()

    # CLEAN STREAM (z-score → real units → compare)
    def handle_clean(self, frame):
        """Process a frame from the clean stream."""
        try:
            sensor = frame["sensor_id"]
            
            z_value = float(frame["normalized"])
            mean = float(frame["mean"])
            std = float(frame["std"])
            actual_raw = float(frame["value"])
            
            with self._get_lock(sensor):
                # Get or create window for this sensor
                if sensor not in self.clean_windows:
                    self.clean_windows[sensor] = deque(maxlen=self.window_size)
                    current_window_size.labels(sensor=sensor, stream_type='clean').set(self.window_size)
                
                window = self.clean_windows[sensor]
                
                # Make prediction if window is full
                if len(window) == self.window_size:
                    # Predict in z-score space
                    z_pred = sum(window) / len(window)
                    
                    # Convert back to physical units
                    raw_pred = mean + z_pred * std
                    
                    # Compare in real units
                    error = abs(raw_pred - actual_raw)
                    
                    # Update metrics
                    clean_error.labels(sensor=sensor).set(error)
                    clean_predictions.labels(sensor=sensor).inc()
                    
                    # Track stats
                    if sensor not in self.stats['clean_predictions']:
                        self.stats['clean_predictions'][sensor] = 0
                        self.stats['clean_errors'][sensor] = []
                    
                    self.stats['clean_predictions'][sensor] += 1
                    self.stats['clean_errors'][sensor].append(error)
                    
                    logger.debug(f"CLEAN[{sensor}]: z_pred={z_pred:.3f}, "
                                f"raw_pred={raw_pred:.3f}, actual={actual_raw:.3f}, "
                                f"error={error:.3f}")
                
                # Update window
                window.append(z_value)
                
        except KeyError as e:
            logger.error(f"Missing key in clean frame: {e}")
            model_errors.labels(stream_type='clean', error_type='missing_key').inc()
        except ValueError as e:
            logger.error(f"Invalid value in clean frame: {e}")
            model_errors.labels(stream_type='clean', error_type='invalid_value').inc()
        except Exception as e:
            logger.error(f"Unexpected error in clean handler: {e}")
            model_errors.labels(stream_type='clean', error_type='unknown').inc()

    def get_stats(self):
        """Return model statistics."""
        stats = {
            'window_size': self.window_size,
            'sensors_tracked_raw': len(self.raw_windows),
            'sensors_tracked_clean': len(self.clean_windows),
            'raw_predictions': dict(self.stats['raw_predictions']),
            'clean_predictions': dict(self.stats['clean_predictions']),
        }
        
        # Calculate average errors
        if self.stats['raw_errors']:
            stats['avg_raw_error'] = {
                s: sum(errors[-100:])/len(errors[-100:])  # rolling avg of last 100
                for s, errors in self.stats['raw_errors'].items()
            }
        
        if self.stats['clean_errors']:
            stats['avg_clean_error'] = {
                s: sum(errors[-100:])/len(errors[-100:])
                for s, errors in self.stats['clean_errors'].items()
            }
        
        return stats


def setup_signal_handlers():
    """Handle graceful shutdown on Ctrl+C."""
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutting down model...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    return lambda: running


def main():
    """Main entry point for model service."""
    import argparse
    from src.redis_consumer import RedisConsumer
    
    parser = argparse.ArgumentParser(description='Downstream Model')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--raw-stream', default='sensor_packets',
                       help='Redis stream for raw packets')
    parser.add_argument('--clean-stream', default='clean_packets',
                       help='Redis stream for clean frames')
    parser.add_argument('--raw-group', default='model-raw',
                       help='Consumer group for raw stream')
    parser.add_argument('--clean-group', default='model-clean',
                       help='Consumer group for clean stream')
    parser.add_argument('--metrics-port', type=int, default=8004,
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
    
    # Create model
    model = MovingAverageModel(config)
    
    # Create Redis consumers for both streams
    max_retries = 5
    raw_consumer = None
    clean_consumer = None
    
    for attempt in range(max_retries):
        try:
            raw_consumer = RedisConsumer(
                stream=args.raw_stream,
                group=args.raw_group,
                consumer=f"{args.raw_group}-worker"
            )
            clean_consumer = RedisConsumer(
                stream=args.clean_stream,
                group=args.clean_group,
                consumer=f"{args.clean_group}-worker"
            )
            logger.info(f"✅ Connected to Redis")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Redis connection failed (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(3)
            else:
                logger.error("❌ Could not connect to Redis after multiple attempts")
                sys.exit(1)
    
    # Check- if keep running
    is_running = setup_signal_handlers()
    
    logger.info(f"🔄 Model started. Listening to raw='{args.raw_stream}', clean='{args.clean_stream}'")
    
    # Main processing loop
    while is_running():
        try:
            # Read from raw stream
            raw_messages = raw_consumer.read(count=5, block=500)
            for msg in raw_messages:
                model.handle_raw(msg['data'])
                raw_consumer.ack(msg['id'])
            
            # Read from clean stream
            clean_messages = clean_consumer.read(count=5, block=500)
            for msg in clean_messages:
                model.handle_clean(msg['data'])
                clean_consumer.ack(msg['id'])
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.01)
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(1)
    
    # Print final stats
    stats = model.get_stats()
    logger.info(f"📊 Final stats: {stats}")
    logger.info("Model stopped")


if __name__ == "__main__":
    main()