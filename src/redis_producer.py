import redis
import json

class RedisProducer:
    def __init__(self, stream="sensor_packets"):
        self.client = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
        )
        self.stream = stream

    def emit(self, packet: dict):
        payload = {k: json.dumps(v) for k, v in packet.items()}
        self.client.xadd(self.stream, payload)
