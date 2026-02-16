# preprocessor.py

from datetime import datetime, timedelta, timezone
from prometheus_client import Counter, start_http_server
import math
import yaml
import os
import logging
import time
import signal
import sys
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
preprocessor_output_frames = Counter(
    "preprocessor_output_frames_total",
    "Total frames emitted by preprocessor",
)

preprocessor_imputed_frames = Counter(
    "preprocessor_imputed_frames_total",
    "Total frames filled via imputation",
)

preprocessor_unusable_frames = Counter(
    "preprocessor_unusable_frames_total",
    "Total frames marked unusable",
)

preprocessor_input_packets = Counter(
    "preprocessor_input_packets_total",
    "Total packets received by preprocessor",
)


def load_config(config_path=None):
    """Load preprocessor configuration from YAML file."""
    if config_path is None:
        possible_paths = [
            'config/preprocessor.yaml',
            '/app/config/preprocessor.yaml',
            '../config/preprocessor.yaml',
            os.path.join(os.path.dirname(__file__), '../config/preprocessor.yaml')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
    
    if not config_path or not os.path.exists(config_path):
        logger.warning(f"Config file not found, using defaults")
        return {
            'target_rate': 1,
            'normalization_window': 100,
            'unusable_after': 5,
            'imputation': {
                'max_linear_gap': 3,
                'max_spline_gap': 10,
                'fallback': 'forward_fill'
            }
        }
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"✅ Loaded config from {config_path}")
            return config or {}
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


class Preprocessor:
    """
    Enforces downstream data contracts:
    - regular time cadence
    - monotonic logical sequence
    - explicit values
    - explicit quality labels
    """

    def __init__(self, config=None):
        config = config or {}
        
        # Use config values
        self.cadence_seconds = config.get('target_rate', 1)
        self.cadence = timedelta(seconds=self.cadence_seconds)
        self.unusable_after = config.get('unusable_after', 5)
        self.window_size = config.get('normalization_window', 100)

        # Get imputation config
        imputation_config = config.get('imputation', {})
        self.max_linear_gap = imputation_config.get('max_linear_gap', 3)
        self.max_spline_gap = imputation_config.get('max_spline_gap', 10)
        self.fallback = imputation_config.get('fallback', 'forward_fill')

        # Per-sensor state
        self.last_output_time = None
        self.last_value = {}           # sensor -> last valid value
        self.stats = {}                # sensor -> rolling stats
        self.sensor_windows = {}        # sensor -> value window
        self.missing_streak = {}        # sensor -> consecutive missing
        self.packet_buffer = {}         # sensor -> buffered packets

        self.logical_sequence = 0

        logger.info(f"✅ Preprocessor initialized: cadence={self.cadence_seconds}s, "
                   f"window={self.window_size}, unusable_after={self.unusable_after}")

    def process(self, packet):
        """
        Consume a raw packet and emit one or more clean frames.
        Returns a list of output frames.
        """
        preprocessor_input_packets.inc()
        outputs = []

        sensor = packet["sensor_id"]
        value = packet["value"]
        
        # Handle timestamp parsing safely
        try:
            ts_str = packet["timestamp"].replace("Z", "+00:00")
            packet_time = datetime.fromisoformat(ts_str)
        except (ValueError, AttributeError) as e:
            logger.warning(f"⚠️ Corrupt timestamp for {sensor}: {e}, using current time")
            packet_time = datetime.now(timezone.utc)

        packet_used = False

        # Initialize sensor state if needed
        if sensor not in self.last_value:
            self.last_value[sensor] = value
            self.missing_streak[sensor] = 0
            self.stats[sensor] = {"count": 0, "mean": 0.0, "M2": 0.0}
            self.sensor_windows[sensor] = []
            self.packet_buffer[sensor] = []

        # Buffer out-of-order packets
        if packet_time < self.last_output_time if self.last_output_time else False:
            logger.debug(f"Out-of-order packet for {sensor}, buffering")
            self.packet_buffer[sensor].append((packet_time, value))
            return outputs

        # First packet initializes the timeline
        if self.last_output_time is None:
            self.last_output_time = packet_time
            frame = self._emit_frame(packet_time, sensor, value, "VALID")
            outputs.append(frame)
            return outputs

        next_time = self.last_output_time + self.cadence

        # Determine total number of missing frames before the incoming packet
        # total_imputes = number of cadence steps between last_output_time and packet_time minus 1
        total_imputes = 0
        if self.last_output_time is not None:
            total_imputes = int((packet_time - self.last_output_time) / self.cadence) - 1
            if total_imputes < 0:
                total_imputes = 0

        impute_index = 0

        # Generate frames until we catch up to packet time
        while next_time <= packet_time or not packet_used:
            if not packet_used and packet_time <= next_time:
                # Use real packet for this frame
                self.missing_streak[sensor] = 0
                frame = self._emit_frame(next_time, sensor, value, "VALID")
                packet_used = True
            else:
                # Need to impute missing data
                impute_index += 1
                self.missing_streak[sensor] += 1

                if self.missing_streak[sensor] > self.unusable_after:
                    quality = "UNUSABLE"
                    preprocessor_unusable_frames.inc()
                    imputed_value = self._impute_value(sensor, next_time)
                else:
                    quality = "IMPUTED"
                    preprocessor_imputed_frames.inc()

                    # Imputation strategy based on gap length
                    # If total_imputes == 0, fallback to forward-fill
                    if total_imputes == 0:
                        imputed_value = self._impute_value(sensor, next_time)
                    elif total_imputes <= self.max_linear_gap:
                        # Linear interpolation between last known and incoming packet value
                        imputed_value = self._linear_interpolate(sensor, value, impute_index, total_imputes)
                    elif total_imputes <= self.max_spline_gap:
                        # Cubic/polynomial fit using recent window + endpoint
                        imputed_value = self._polynomial_impute(sensor, value, impute_index, total_imputes)
                    else:
                        # Fallback strategy
                        if self.fallback == 'forward_fill':
                            imputed_value = self._impute_value(sensor, next_time)
                        else:
                            imputed_value = self._impute_value(sensor, next_time)

                frame = self._emit_frame(next_time, sensor, imputed_value, quality)

            outputs.append(frame)
            self.last_output_time = next_time
            next_time += self.cadence

        # Process any buffered packets
        if sensor in self.packet_buffer and self.packet_buffer[sensor]:
            for buf_time, buf_val in sorted(self.packet_buffer[sensor]):
                if buf_time > self.last_output_time:
                    # Recursively process buffered packets
                    dummy_packet = {
                        "sensor_id": sensor,
                        "value": buf_val,
                        "timestamp": buf_time.isoformat() + "Z"
                    }
                    outputs.extend(self.process(dummy_packet))
            self.packet_buffer[sensor] = []

        return outputs

    def _impute_value(self, sensor, timestamp):
        """
        Impute missing value based on configured strategy.
        Currently uses forward fill (last known value).
        """
        # Simple forward fill imputation
        # TODO: Add linear interpolation, spline fit based on config
        return self.last_value.get(sensor, 0.0)

    def _linear_interpolate(self, sensor, next_packet_value, impute_index, total_imputes):
        """Linear interpolation between last known value and next real packet value.

        impute_index: 1..total_imputes
        total_imputes: number of imputed frames before the real packet
        """
        last = self.last_value.get(sensor, None)
        if last is None:
            return float(next_packet_value)

        # fraction along interval (i / (total_imputes+1))
        frac = impute_index / (total_imputes + 1)
        return float(last + (next_packet_value - last) * frac)

    def _polynomial_impute(self, sensor, next_packet_value, impute_index, total_imputes):
        """Polynomial (cubic) fit using recent valid window plus next packet as endpoint.

        Falls back to linear if insufficient anchor points.
        """
        # Collect anchor points: use up to 4 most recent values from sensor_windows (excluding very old ones)
        anchors = []  # list of (x, y) where x is relative time index
        window = self.sensor_windows.get(sensor, [])

        # Use last known value as x=0
        last = self.last_value.get(sensor, None)
        if last is None:
            return float(next_packet_value)

        # Prepare x positions: negative indices for past points, 0 for last, total_imputes+1 for next packet
        # Choose up to 3 past points plus last (ensure at least 2 points)
        past_vals = []
        # Pull up to 3 previous values from window (most recent at end)
        for v in reversed(window[-4:]):
            past_vals.append(v)

        # Ensure last is included (may duplicate)
        if not past_vals or past_vals[0] != last:
            past_vals.insert(0, last)

        # Build anchors with positions
        # positions: use -k ... -1, 0 (last), and endpoint at total_imputes+1
        anchors = []
        n_past = len(past_vals)
        for i, v in enumerate(past_vals):
            # position: i - (n_past - 1)
            pos = i - (n_past - 1)
            anchors.append((pos, float(v)))

        endpoint_pos = total_imputes + 1
        anchors.append((endpoint_pos, float(next_packet_value)))

        xs = np.array([a[0] for a in anchors], dtype=float)
        ys = np.array([a[1] for a in anchors], dtype=float)

        # If insufficient unique points, fallback to linear
        if len(np.unique(xs)) < 2 or len(anchors) < 2:
            return self._linear_interpolate(sensor, next_packet_value, impute_index, total_imputes)

        # Fit polynomial of degree up to 3
        deg = min(3, len(anchors) - 1)
        try:
            coeffs = np.polyfit(xs, ys, deg)
            poly = np.poly1d(coeffs)
            x_impute = impute_index
            return float(poly(x_impute))
        except Exception:
            return self._linear_interpolate(sensor, next_packet_value, impute_index, total_imputes)

    # Rolling statistics for normalization
    def _update_stats(self, sensor, x):
        """Update running mean and variance using Welford's algorithm."""
        stats = self.stats[sensor]
        stats["count"] += 1

        delta = x - stats["mean"]
        stats["mean"] += delta / stats["count"]
        delta2 = x - stats["mean"]
        stats["M2"] += delta * delta2

        # Maintain rolling window
        if sensor in self.sensor_windows:
            self.sensor_windows[sensor].append(x)
            if len(self.sensor_windows[sensor]) > self.window_size:
                self.sensor_windows[sensor].pop(0)

    def _std(self, sensor):
        """Calculate standard deviation from running stats."""
        stats = self.stats[sensor]
        if stats["count"] < 2:
            return 1.0
        variance = stats["M2"] / (stats["count"] - 1)
        return math.sqrt(max(variance, 1e-6))

    def _rolling_mean(self, sensor):
        """Calculate mean from rolling window or global stats."""
        if sensor in self.sensor_windows and len(self.sensor_windows[sensor]) > 0:
            return sum(self.sensor_windows[sensor]) / len(self.sensor_windows[sensor])
        return self.stats[sensor]["mean"]

    def _rolling_std(self, sensor):
        """Calculate std from rolling window or global stats."""
        if sensor in self.sensor_windows and len(self.sensor_windows[sensor]) > 1:
            mean = self._rolling_mean(sensor)
            variance = sum((x - mean) ** 2 for x in self.sensor_windows[sensor]) / (len(self.sensor_windows[sensor]) - 1)
            return math.sqrt(max(variance, 1e-6))
        return self._std(sensor)

    def _emit_frame(self, timestamp, sensor, value, quality):
        """Create a clean frame with normalized value."""
        # Update stats only for usable values
        if quality != "UNUSABLE":
            self._update_stats(sensor, value)
            self.last_value[sensor] = value

        # Get statistics for normalization
        mean = self._rolling_mean(sensor)
        std = self._rolling_std(sensor)
        
        # Calculate z-score
        if std > 1e-6:
            z_score = (value - mean) / std
        else:
            z_score = 0.0

        frame = {
            "timestamp": timestamp.isoformat().replace('+00:00', 'Z'),
            "sequence": self.logical_sequence,
            "sensor_id": sensor,
            "value": round(value, 3),
            "normalized": round(z_score, 4),
            "mean": round(mean, 3),
            "std": round(std, 3),
            "quality": quality,
        }

        self.logical_sequence += 1
        preprocessor_output_frames.inc()

        return frame

    def get_stats(self):
        """Return current preprocessor statistics."""
        return {
            'logical_sequence': self.logical_sequence,
            'sensors_tracked': len(self.last_value),
            'total_frames': preprocessor_output_frames._value.get(),
            'imputed_frames': preprocessor_imputed_frames._value.get(),
            'unusable_frames': preprocessor_unusable_frames._value.get(),
        }


def setup_signal_handlers():
    """Handle graceful shutdown on Ctrl+C."""
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutting down preprocessor...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    return lambda: running


def main():
    """Main entry point for preprocessor service."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Telemetry Preprocessor')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--input-stream', default='observed_packets',
                       help='Redis stream to consume from')
    parser.add_argument('--output-stream', default='clean_packets',
                       help='Redis stream to publish clean frames to')
    parser.add_argument('--group', default='preprocessor',
                       help='Consumer group name')
    parser.add_argument('--metrics-port', type=int, default=8003,
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
    
    # Create preprocessor
    pre = Preprocessor(config)
    
    # Import Redis classes
    from src.redis_consumer import RedisConsumer
    from src.redis_producer import RedisProducer
    
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
    
    # Check- keep running
    is_running = setup_signal_handlers()
    
    logger.info(f"🔄 Preprocessor started. Listening to '{args.input_stream}' -> '{args.output_stream}'")
    
    # Main processing loop
    while is_running():
        try:
            # Read messages from Redis
            messages = consumer.read(count=10, block=1000)
            
            for msg in messages:
                packet = msg['data']
                
                # Process packet through preprocessor
                frames = pre.process(packet)
                
                # Emit each frame
                for frame in frames:
                    producer.emit(frame)
                
                # Acknowledge processing
                consumer.ack(msg['id'])
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(1)
    
    # Print final stats
    stats = pre.get_stats()
    logger.info(f"📊 Final stats: {stats}")
    logger.info("Preprocessor stopped")


if __name__ == "__main__":
    main()