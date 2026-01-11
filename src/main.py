from src.validator import Validator
from src.observer import Observer
from src.preprocessor import Preprocessor
from src.sensor_simulator import run_simulator

from src.redis_producer import RedisProducer
from src.redis_consumer import RedisConsumer

from prometheus_client import start_http_server
import threading
from src.downstream_model import MovingAverageModel


def main():
    start_http_server(8000, addr="0.0.0.0")
    print("Metrics available at http://localhost:8000/metrics")

    # Core pipeline components
    validator = Validator()
    observer = Observer()
    preprocessor = Preprocessor()

    producer = RedisProducer(stream="sensor_packets")
    clean_producer = RedisProducer(stream="sensor_clean")

    # Simulator emits into Redis
    def emit(packet):
        producer.emit(packet)

    # Main pipeline consumer (raw → clean)
    consumer = RedisConsumer(stream="sensor_packets")

    # THIS is the only handler that must exist
    def handle_packet(packet):
        try:
            packet = validator.validate(packet)
            observer.observe(packet)
            frames = preprocessor.process(packet)
            for frame in frames:
                print("CLEAN:", frame)
                clean_producer.emit(frame)
            print("VALIDATOR SAW:", packet)
        except Exception as e:
            print("PIPELINE ERROR:", e)

    # Downstream model
    model = MovingAverageModel(window_size=5)

    raw_consumer = RedisConsumer(
        stream="sensor_packets",
        group="raw-model",
        consumer="raw-1",
    )

    clean_consumer = RedisConsumer(
        stream="sensor_clean",
        group="clean-model",
        consumer="clean-1",
    )

    # Sensor simulator
    sim_thread = threading.Thread(
        target=run_simulator,
        args=(emit,),
        daemon=True,
    )
    sim_thread.start()

    # Raw stream → model
    raw_thread = threading.Thread(
        target=raw_consumer.consume,
        args=(model.handle_raw,),
        daemon=True,
    )
    raw_thread.start()

    # Clean stream → model
    clean_thread = threading.Thread(
        target=clean_consumer.consume,
        args=(model.handle_clean,),
        daemon=True,
    )
    clean_thread.start()

    # Main pipeline (blocking)
    consumer.consume(handle_packet)


if __name__ == "__main__":
    main()
