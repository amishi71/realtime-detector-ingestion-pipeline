from datetime import datetime, timedelta
from prometheus_client import Counter
import math


preprocessor_output_frames = Counter(
    "preprocessor_output_frames_total",
    "Total frames emitted by preprocessor",
)


class Preprocessor:
    """
    Enforces downstream data contracts:
    - regular time cadence
    - monotonic logical sequence
    - explicit values
    - explicit quality labels
    """

    def __init__(self, cadence_seconds=1, unusable_after=5):
        self.cadence = timedelta(seconds=cadence_seconds)
        self.unusable_after = unusable_after

        self.last_output_time = None
        self.last_value = {}   # per-sensor
        self.stats = {}       # per-sensor rolling stats

        self.logical_sequence = 0
        self.missing_streak = {}   # per-sensor

    def process(self, packet):
        """
        Consume a raw packet and emit one or more clean frames.
        Returns a list of output frames.
        """
        outputs = []

        sensor = packet["sensor_id"]
        value = packet["value"]
        packet_time = datetime.fromisoformat(packet["timestamp"].replace("Z", ""))
        packet_used = False

        # Initialize sensor state
        if sensor not in self.last_value:
            self.last_value[sensor] = value
            self.missing_streak[sensor] = 0
            self.stats[sensor] = {"count": 0, "mean": 0.0, "M2": 0.0}

        # First packet initializes the timeline
        if self.last_output_time is None:
            self.last_output_time = packet_time
            frame = self._emit_frame(packet_time, sensor, value, "VALID")
            outputs.append(frame)
            return outputs

        next_time = self.last_output_time + self.cadence

        while next_time <= packet_time or not packet_used:
            if not packet_used and packet_time <= next_time:
                # Use real packet as soon as it can fill a frame
                self.missing_streak[sensor] = 0
                frame = self._emit_frame(next_time, sensor, value, "VALID")
                packet_used = True
            else:
                # Imputation
                self.missing_streak[sensor] += 1
                quality = (
                    "UNUSABLE"
                    if self.missing_streak[sensor] > self.unusable_after
                    else "IMPUTED"
                )
                frame = self._emit_frame(next_time, sensor, self.last_value[sensor], quality)

            outputs.append(frame)
            self.last_output_time = next_time
            next_time += self.cadence

        return outputs

    # ---------------- Rolling normalization ----------------

    def _update_stats(self, sensor, x):
        stats = self.stats[sensor]
        stats["count"] += 1

        delta = x - stats["mean"]
        stats["mean"] += delta / stats["count"]
        delta2 = x - stats["mean"]
        stats["M2"] += delta * delta2

    def _std(self, sensor):
        stats = self.stats[sensor]
        if stats["count"] < 2:
            return 1.0
        return math.sqrt(stats["M2"] / (stats["count"] - 1))

    # ---------------- Frame emission ----------------

    def _emit_frame(self, timestamp, sensor, value, quality):
        # Update rolling stats only when value is real or imputed
        if quality != "UNUSABLE":
            self._update_stats(sensor, value)

        mean = self.stats[sensor]["mean"]
        std = self._std(sensor)
        z_score = 0.0 if std == 0 else (value - mean) / std

        frame = {
            "timestamp": timestamp.isoformat() + "Z",
            "sequence": self.logical_sequence,
            "sensor_id": sensor,
            "value": value,
            "normalized": z_score,
            "mean": mean,
            "std": std if std > 1e-6 else 1.0,             # avoid divide-by-zero 
            "quality": quality,
        }

        self.last_value[sensor] = value
        self.logical_sequence += 1

        preprocessor_output_frames.inc()  

        return frame
