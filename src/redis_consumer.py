import redis
import json
import os


class RedisConsumer:
    def __init__(
        self,
        stream="sensor_packets",
        group="pipeline",
        consumer="worker-1",
    ):
        host = os.getenv("REDIS_HOST", "localhost")

        self.client = redis.Redis(
            host=host,
            port=6379,
            decode_responses=True,
        )
        self.stream = stream
        self.group = group
        self.consumer = consumer

        # Create consumer group if it doesn't exist
        try:
            self.client.xgroup_create(
                self.stream,
                self.group,
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError:
            pass  # group already exists

    def consume(self, handler):
        while True:
            messages = self.client.xreadgroup(
                self.group,
                self.consumer,
                {self.stream: ">"},
                count=1,
                block=1000,
            )

            if not messages:
                continue

            _, entries = messages[0]
            for msg_id, fields in entries:
                packet = {k: json.loads(v) for k, v in fields.items()}
                handler(packet)
                self.client.xack(self.stream, self.group, msg_id)
