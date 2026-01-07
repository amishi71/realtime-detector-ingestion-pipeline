# src/sensor_simulator.py

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
    sensor_id = "sensor_001"
    sequence_number = 0

    baseline_value = 100.0
    initial_baseline = baseline_value
    drift_rate = 0.01

    burst_remaining = 0
    recovering_remaining = 0
    last_sequence_emitted = None

    while True:
        # --- noise ---
        noise = random.uniform(-0.5, 0.5)
        observed_value = baseline_value + noise

        # --- drift ---
        baseline_value += drift_rate

        # --- missing packets ---
        if burst_remaining > 0:
            burst_remaining -= 1
            sequence_number += 1
            time.sleep(1)
            continue

        if random.random() < 0.02:
            burst_remaining = random.randint(2, 5)
            recovering_remaining = 3
            sequence_number += 1
            time.sleep(1)
            continue

        if random.random() < 0.05:
            sequence_number += 1
            time.sleep(1)
            continue

        # --- sequence corruption ---
        emit_sequence = sequence_number

        if random.random() < 0.01 and last_sequence_emitted is not None:
            emit_sequence = last_sequence_emitted
        elif random.random() < 0.01:
            emit_sequence = sequence_number + 1
            sequence_number += 1

        # --- status ---
        if recovering_remaining > 0:
            status = "RECOVERING"
            recovering_remaining -= 1
        elif abs(noise) > 0.4 or abs(baseline_value - initial_baseline) > 1.0:
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

        print(packet)
        emit(packet)

        last_sequence_emitted = emit_sequence
        sequence_number += 1
        time.sleep(1)
