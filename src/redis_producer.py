

# redis_producer.py

import redis
import json
import os


class RedisProducer:
    def __init__(self, stream="sensor_packets"):
        host = os.getenv("REDIS_HOST", "localhost")

        self.client = redis.Redis(
            host=host,
            port=6379,
            decode_responses=True,
        )
        self.stream = stream

    def emit(self, packet: dict):
        payload = {k: json.dumps(v) for k, v in packet.items()}
        self.client.xadd(self.stream, payload)
