# WARNING: This runs everything in one process!
# For production, use: docker-compose up
# main.py - FOR LOCAL DEVELOPMENT ONLY
# Use Docker Compose for production deployment

import logging
import threading
import time
import signal
import sys
from prometheus_client import start_http_server

from src.validator import Validator
from src.observer import Observer
from src.preprocessor import Preprocessor
from src.sensor_simulator import run_simulator
from src.redis_producer import RedisProducer
from src.redis_consumer import RedisConsumer
from src.downstream_model import MovingAverageModel

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for shutdown
running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutting down...")
    running = False

def run_with_retry(target, args=(), name=""):
    """Run a thread with automatic retry on failure."""
    while running:
        try:
            target(*args)
        except Exception as e:
            logger.error(f"Thread {name} crashed: {e}")
            if running:
                logger.info(f"Restarting {name} in 5 seconds...")
                time.sleep(5)
            else:
                break

def main():
    global running
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start metrics server
    try:
        start_http_server(8000, addr="0.0.0.0")
        logger.info("📊 Metrics available at http://localhost:8000/metrics")
    except Exception as e:
        logger.warning(f"Could not start metrics server: {e}")

    # Core pipeline components
    validator = Validator()
    observer = Observer()
    preprocessor = Preprocessor()
    model = MovingAverageModel(window_size=5)

    # Redis connections
    try:
        producer = RedisProducer(stream="sensor_packets")
        clean_producer = RedisProducer(stream="clean_packets")  # Fixed name!
        logger.info("✅ Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    # Simulator emit function
    def emit(packet):
        producer.emit(packet)

    # Main pipeline handler
    def handle_packet(packet):
        try:
            packet = validator.validate(packet)
            observer.observe(packet)
            frames = preprocessor.process(packet)
            for frame in frames:
                logger.debug(f"CLEAN: {frame}")
                clean_producer.emit(frame)
            logger.debug(f"VALIDATOR SAW: {packet}")
        except Exception as e:
            logger.error(f"PIPELINE ERROR: {e}")

    # Create consumers
    consumer = RedisConsumer(stream="sensor_packets", group="pipeline")
    raw_consumer = RedisConsumer(
        stream="sensor_packets",
        group="raw-model",
        consumer="raw-1",
    )
    clean_consumer = RedisConsumer(
        stream="clean_packets",  # Fixed name!
        group="clean-model",
        consumer="clean-1",
    )

    # Start all threads
    threads = [
        threading.Thread(target=run_with_retry, args=(run_simulator, (emit,)), kwargs={"name": "simulator"}, daemon=True),
        threading.Thread(target=run_with_retry, args=(consumer.consume, (handle_packet,)), kwargs={"name": "pipeline"}, daemon=True),
        threading.Thread(target=run_with_retry, args=(raw_consumer.consume, (model.handle_raw,)), kwargs={"name": "raw-model"}, daemon=True),
        threading.Thread(target=run_with_retry, args=(clean_consumer.consume, (model.handle_clean,)), kwargs={"name": "clean-model"}, daemon=True),
    ]

    for t in threads:
        t.start()
        logger.info(f"Started {t.name} thread")

    logger.info("✅ All threads started. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received interrupt")
    finally:
        running = False
        logger.info("Shutting down...")
        time.sleep(2)  # Give threads time to clean up

if __name__ == "__main__":
    main()