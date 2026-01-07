# src/main.py

from src.validator import Validator
from src.observer import Observer
from src.preprocessor import Preprocessor
from src.sensor_simulator import run_simulator

from src.redis_producer import RedisProducer
from src.redis_consumer import RedisConsumer

from prometheus_client import start_http_server
import threading


def main():
    start_http_server(8000)
    print("Metrics available at http://localhost:8000/metrics")

    validator = Validator()
    observer = Observer()
    preprocessor = Preprocessor()

    producer = RedisProducer(stream="sensor_packets")

    def emit(packet):
        producer.emit(packet)

    consumer = RedisConsumer(stream="sensor_packets")

    def handle_packet(packet):
        packet = validator.validate(packet)
        observer.observe(packet)
        frames = preprocessor.process(packet)
        for frame in frames:
            print("CLEAN:", frame)
    
    def emit(packet):
        producer.emit(packet)

   
    sim_thread = threading.Thread(
        target=run_simulator,
        args=(emit,),
        daemon=True,
    )
    sim_thread.start()

 
    consumer.consume(handle_packet)


if __name__ == "__main__":
    main()
