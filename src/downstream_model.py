from collections import deque
from prometheus_client import Gauge

raw_error = Gauge(
    "downstream_raw_prediction_error",
    "Absolute prediction error on raw telemetry",
    ["sensor"],
)

clean_error = Gauge(
    "downstream_clean_prediction_error",
    "Absolute prediction error on cleaned telemetry (in raw units)",
    ["sensor"],
)


class MovingAverageModel:
    def __init__(self, window_size=5):
        self.window_size = window_size
        self.raw_windows = {}     # sensor_id -> deque of raw values
        self.clean_windows = {}   # sensor_id -> deque of z-scores

    # RAW STREAM

    def handle_raw(self, packet):
        try:
            sensor = packet["sensor_id"]
            value = float(packet["value"])

            window = self.raw_windows.setdefault(sensor, deque(maxlen=self.window_size))

            if len(window) == self.window_size:
                prediction = sum(window) / len(window)
                error = abs(prediction - value)
                raw_error.labels(sensor=sensor).set(error)

            window.append(value)
        except Exception as e:
            print("RAW MODEL ERROR:", e)

    # CLEAN STREAM (z-score → real units → compare)

    def handle_clean(self, frame):
        try:
            sensor = frame["sensor_id"]

            z_value = float(frame["normalized"])
            mean = float(frame["mean"])
            std = float(frame["std"])
            actual_raw = float(frame["value"])

            window = self.clean_windows.setdefault(sensor, deque(maxlen=self.window_size))

            if len(window) == self.window_size:
                # predict in z-score space
                z_pred = sum(window) / len(window)

                # convert back to physical units
                raw_pred = mean + z_pred * std

                # compare in real units
                error = abs(raw_pred - actual_raw)
                clean_error.labels(sensor=sensor).set(error)

            window.append(z_value)
        except Exception as e:
            print("CLEAN MODEL ERROR:", e)
