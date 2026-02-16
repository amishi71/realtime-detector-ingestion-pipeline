# sensor_simulator.py

"""
Simulator that generates realistic sensor telemetry with configurable corruption:
- Noise, drift, packet loss, timestamp jitter, sequence errors
- Emits packets to Redis stream
"""

import time
import random
import signal
import sys
import os
import itertools
from datetime import datetime, timezone
import yaml
import logging
from prometheus_client import Counter, Gauge, start_http_server

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
packets_emitted = Counter('simulator_packets_total', 'Total packets emitted')
packets_corrupted = Counter('simulator_corrupted_total', 'Total corrupted packets', 
                           ['corruption_type'])
sensor_value = Gauge('simulator_sensor_value', 'Current sensor value', 
                    ['sensor_id', 'status'])
sequence_gaps = Counter('simulator_sequence_gaps_total', 'Sequence number gaps')
loss_events = Counter('simulator_loss_events_total', 'Packet loss events', 
                     ['loss_type'])


def load_config(config_path=None):
    """Load simulator configuration from YAML file."""
    if config_path is None:
        # Try multiple possible locations
        possible_paths = [
            'config/simulator.yaml',
            '/app/config/simulator.yaml',
            '../config/simulator.yaml',
            os.path.join(os.path.dirname(__file__), '../config/simulator.yaml')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
    
    if not config_path or not os.path.exists(config_path):
        logger.warning(f"Config file not found, using defaults")
        return {}  # Return empty dict, defaults will be used
    
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
        logger.info("Shutting down simulator...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    return lambda: running


def run_simulator(emit_func, config):
    """Run simulator with configuration from YAML."""
    
    # Sensor configuration
    sensors_config = config.get('sensors_config', {
        "sensor_001": {"baseline": 100.0, "drift": 0.01},
        "sensor_002": {"baseline": 200.0, "drift": -0.005},
        "sensor_003": {"baseline": 300.0, "drift": 0.02},
    })
    
    # Simulation parameters with defaults
    params = {
        'burst_prob': config.get('burst_prob', 0.02),
        'single_loss_prob': config.get('single_loss_prob', 0.05),
        'duplicate_prob': config.get('duplicate_prob', 0.01),
        'gap_prob': config.get('gap_prob', 0.01),
        'backwards_prob': config.get('backwards_prob', 0.05),
        'timestamp_corruption_prob': config.get('timestamp_corruption_prob', 0.05),
        'noise_range': config.get('noise_range', 0.5),
        'noise_threshold': config.get('degraded_noise_threshold', 0.4),
        'drift_threshold': config.get('degraded_drift_threshold', 1.0),
        'recovery_duration': config.get('recovery_duration', 3),
        'frequency': config.get('frequency', 1),  # Hz per sensor
    }
    
    # Calculate sleep time based on number of sensors
    num_sensors = len(sensors_config)
    sleep_time = 1.0 / (params['frequency'] * num_sensors)
    
    logger.info(f"✅ Simulator started with params: {params}")
    logger.info(f"📊 Sensors: {list(sensors_config.keys())} ({num_sensors} total)")
    logger.info(f"⏱️  Target rate: {params['frequency']} Hz per sensor = {params['frequency'] * num_sensors} Hz total")
    logger.info(f"⏱️  Sleep time between packets: {sleep_time:.3f}s")
    
    # per-sensor state
    sequences = {sid: 0 for sid in sensors_config}
    burst_remaining = {sid: 0 for sid in sensors_config}
    recovering_remaining = {sid: 0 for sid in sensors_config}
    last_sequence_emitted = {sid: None for sid in sensors_config}
    initial_baselines = {sid: cfg["baseline"] for sid, cfg in sensors_config.items()}
    
    # Use round-robin for fair distribution
    sensor_cycle = itertools.cycle(sensors_config.keys())
    
    # Statistics for monitoring
    packet_count = 0
    start_time = time.time()
    
    # Check if we should keep running
    is_running = setup_signal_handlers()
    
    # Main simulation loop
    while is_running():
        try:
            # Pick next sensor in round-robin
            sensor_id = next(sensor_cycle)
            sensor = sensors_config[sensor_id]
            
            #  noise 
            noise = random.uniform(-params['noise_range'], params['noise_range'])
            observed_value = sensor["baseline"] + noise
            
            # drift 
            sensor["baseline"] += sensor["drift"]
            
            seq = sequences[sensor_id]
            
            #  missing packets 
            if burst_remaining[sensor_id] > 0:
                burst_remaining[sensor_id] -= 1
                sequences[sensor_id] += 1
                loss_events.labels(loss_type='burst').inc()
                # Skip emission - this is a lost packet
                time.sleep(sleep_time)
                continue
            
            # Start a new burst loss event
            if random.random() < params['burst_prob']:
                burst_len = random.randint(2, 5)
                burst_remaining[sensor_id] = burst_len
                recovering_remaining[sensor_id] = params['recovery_duration']
                sequences[sensor_id] += 1
                loss_events.labels(loss_type='burst_start').inc()
                logger.debug(f"Burst loss started: {burst_len} packets on {sensor_id}")
                time.sleep(sleep_time)
                continue
            
            # Single packet loss
            if random.random() < params['single_loss_prob']:
                sequences[sensor_id] += 1
                loss_events.labels(loss_type='single').inc()
                logger.debug(f"Single packet loss on {sensor_id}")
                time.sleep(sleep_time)
                continue
            
            # sequence corruption
            emit_sequence = seq
            corruption_type = None
            
            # Chance of repeating last sequence (duplicate)
            if (random.random() < params['duplicate_prob'] and 
                last_sequence_emitted[sensor_id] is not None):
                emit_sequence = last_sequence_emitted[sensor_id]
                corruption_type = 'duplicate'
                sequence_gaps.inc()
            
            # Chance of jumping ahead (gap)
            elif random.random() < params['gap_prob']:
                emit_sequence = seq + random.randint(1, 3)  # Jump ahead 1-3
                sequences[sensor_id] += (emit_sequence - seq)
                corruption_type = 'gap'
                sequence_gaps.inc()
            
            # status determination 
            if recovering_remaining[sensor_id] > 0:
                status = "RECOVERING"
                recovering_remaining[sensor_id] -= 1
            elif (abs(noise) > params['noise_threshold'] or 
                  abs(sensor["baseline"] - initial_baselines[sensor_id]) > params['drift_threshold']):
                status = "DEGRADED"
            else:
                status = "NOMINAL"
            
            # Build the packet
            packet = {
                "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                "sensor_id": sensor_id,
                "sequence_number": emit_sequence,
                "value": round(observed_value, 3),
                "status": status,
            }
            
            # deliberate corruption 
            # Backwards sequence
            if random.random() < params['backwards_prob']:
                packet["sequence_number"] = max(0, packet["sequence_number"] - random.randint(1, 5))
                packets_corrupted.labels(corruption_type='backwards').inc()
                logger.debug(f"Backwards sequence on {sensor_id}")
            
            # Corrupt timestamp
            if random.random() < params['timestamp_corruption_prob']:
                packet["timestamp"] = "BAD_TIMESTAMP"
                packets_corrupted.labels(corruption_type='timestamp').inc()
                logger.debug(f"Bad timestamp on {sensor_id}")
            
            # Emit the packet
            try:
                emit_func(packet)
                packets_emitted.inc()
                sensor_value.labels(sensor_id=sensor_id, status=status).set(observed_value)
                
                # Update packet count and log rate periodically
                packet_count += 1
                if packet_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = packet_count / elapsed
                    logger.info(f"📊 Rate: {rate:.1f} packets/sec total "
                               f"({rate/num_sensors:.1f} per sensor)")
                
                logger.debug(f"EMITTED: {packet}")
            except Exception as e:
                logger.error(f"Failed to emit packet: {e}")
                # Wait a bit longer if Redis is down
                time.sleep(5)
                continue
            
            # Update state
            last_sequence_emitted[sensor_id] = emit_sequence
            sequences[sensor_id] = emit_sequence + 1  # Next sequence number
            
            # Wait according to desired frequency
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Unexpected error in simulation loop: {e}")
            time.sleep(1)
    
    # Final statistics
    elapsed = time.time() - start_time
    logger.info(f"📊 Final stats: {packet_count} packets in {elapsed:.1f}s "
                f"({packet_count/elapsed:.1f} packets/sec)")
    logger.info("Simulator stopped")


if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Sensor Simulator')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--metrics-port', type=int, default=8000, 
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
    
    # Import producer (with retry)
    from src.redis_producer import RedisProducer
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = RedisProducer(stream="sensor_packets")
            # Test connection
            producer.client.ping()
            logger.info("✅ Connected to Redis")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Redis connection failed (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(3)
            else:
                logger.error("❌ Could not connect to Redis after multiple attempts")
                sys.exit(1)
    
    # Run simulator
    try:
        run_simulator(producer.emit, config)
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)