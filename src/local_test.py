# local_test.py
# Run this for local development: python src/local_test.py

import logging
from src.redis_producer import RedisProducer
from src.redis_consumer import RedisConsumer
from src.sensor_simulator import run_simulator
from src.validator import Validator
from src.observer import Observer
from src.preprocessor import Preprocessor
from src.downstream_model import MovingAverageModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pipeline():
    
    # Create components
    producer = RedisProducer(stream="test_packets")
    consumer = RedisConsumer(stream="test_packets", group="test")
    validator = Validator()
    observer = Observer()
    preprocessor = Preprocessor()
    model = MovingAverageModel()
    
    def handle_packet(packet):
        logger.info(f"Received: {packet}")
        # Test logic 
    
    # Run simulator for 10 seconds
    import threading
    import time
    
    def run_test():
        for i in range(10):
            packet = {
                "sensor_id": "test_sensor",
                "sequence_number": i,
                "timestamp": "2026-01-01T00:00:00Z",
                "value": 100.0 + i,
                "status": "NOMINAL"
            }
            producer.emit(packet)
            time.sleep(1)
    
    thread = threading.Thread(target=run_test)
    thread.start()
    
    # Consume for 15 seconds
    consumer.consume(handle_packet)
    
if __name__ == "__main__":
    test_pipeline()