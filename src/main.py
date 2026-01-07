# src/main.py

from src.message_bus import MessageBus
from src.validator import Validator
from src.observer import Observer
from src.preprocessor import Preprocessor
from src.consumer import Consumer
from src.sensor_simulator import run_simulator
from prometheus_client import start_http_server


def main():
    start_http_server(8000)
    print("Metrics available at http://localhost:8000/metrics")

    bus = MessageBus()
    validator = Validator()
    observer = Observer()
    preprocessor = Preprocessor()

    bus = MessageBus()

    validator = Validator()
    observer = Observer()
    preprocessor = Preprocessor()

    consumer = Consumer(
        validator=validator,
        observer=observer,
        preprocessor=preprocessor,
    )

    bus.subscribe(consumer.handle)

    run_simulator(bus)


if __name__ == "__main__":
    main()