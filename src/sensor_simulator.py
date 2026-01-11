# Pretend to be a simulator and emit data over time.
''' 
METADATA:

timestamp
When this measurement occurred (not when it arrived)

sensor_id
Which physical instrument produced it

sequence_number
Order guarantee within a sensor (critical for real-time systems)

status / quality flag
Is this reading nominal, degraded, or invalid?

'''
# src/sensor_simulator.py


import time
import random
from datetime import datetime


def run_simulator(emit):
    #changed a single sensor to an array of sensors
    sensors = {
        "sensor_001": {"baseline": 100.0, "drift": 0.01},
        "sensor_002": {"baseline": 200.0, "drift": -0.005},
        "sensor_003": {"baseline": 300.0, "drift": 0.02},
    }

    # per-sensor sequence numbers
    sequences = {sid: 0 for sid in sensors}

    # per-sensor loss and recovery state
    burst_remaining = {sid: 0 for sid in sensors}
    recovering_remaining = {sid: 0 for sid in sensors}
    last_sequence_emitted = {sid: None for sid in sensors}

    initial_baselines = {sid: cfg["baseline"] for sid, cfg in sensors.items()}

    while True:
        # pick one sensor to emit this second
        sensor_id = random.choice(list(sensors.keys()))
        sensor = sensors[sensor_id]

        # --- noise ---
        noise = random.uniform(-0.5, 0.5)
        observed_value = sensor["baseline"] + noise

        # --- drift ---
        sensor["baseline"] += sensor["drift"]

        seq = sequences[sensor_id]

        # --- missing packets ---
        if burst_remaining[sensor_id] > 0:
            burst_remaining[sensor_id] -= 1
            sequences[sensor_id] += 1
            time.sleep(1)
            continue

        if random.random() < 0.02:
            burst_remaining[sensor_id] = random.randint(2, 5)
            recovering_remaining[sensor_id] = 3
            sequences[sensor_id] += 1
            time.sleep(1)
            continue

        if random.random() < 0.05:
            sequences[sensor_id] += 1
            time.sleep(1)
            continue

        # --- sequence corruption ---
        emit_sequence = seq

        if random.random() < 0.01 and last_sequence_emitted[sensor_id] is not None:
            emit_sequence = last_sequence_emitted[sensor_id]
        elif random.random() < 0.01:
            emit_sequence = seq + 1
            sequences[sensor_id] += 1

        # --- status ---
        if recovering_remaining[sensor_id] > 0:
            status = "RECOVERING"
            recovering_remaining[sensor_id] -= 1
        elif abs(noise) > 0.4 or abs(sensor["baseline"] - initial_baselines[sensor_id]) > 1.0:
            status = "DEGRADED"
        else:
            status = "NOMINAL"

        packet = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "sensor_id": sensor_id,
            "sequence_number": emit_sequence,
            "value": round(observed_value, 3),
            "status": status,
        }

        # --- deliberate physics violations (calibration beam) ---
        # Occasionally inject known-bad data so the validator has something to detect.
        # This is how real detector systems are tested.

        if random.random() < 0.05:
            # Force a backwards sequence (violates monotonicity)
            packet["sequence_number"] = packet["sequence_number"] - random.randint(1, 5)

        if random.random() < 0.05:
            # Corrupt timestamp format
            packet["timestamp"] = "BAD_TIMESTAMP"

        print(packet)
        emit(packet)

        print(packet)
        emit(packet)

        last_sequence_emitted[sensor_id] = emit_sequence
        sequences[sensor_id] += 1
        time.sleep(1)


if __name__ == "__main__":
    from src.redis_producer import RedisProducer

    producer = RedisProducer(stream="sensor_packets")
    run_simulator(producer.emit)
