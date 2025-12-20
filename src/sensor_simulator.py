#Pretend to be a simulator and emit data over time.
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
import time
import random
from datetime import datetime

sensor_id= "sensor_001"
sequence_number= 0

baseline_value= 100.0 #fixed baseline signal
initial_baseline= baseline_value
drift_rate= 0.01 #value drifts per second

burst_remaining= 0 #Bursty loss state
recovering_remaining= 0
last_sequence_emitted = None

while True:
    #---noise---
    noise= random.uniform(-0.5, 0.5) #noise is randomness without memory-does not accumulate.
    observed_value= baseline_value+ noise

    #---drift---
    baseline_value += drift_rate ## Drift changes the baseline- has memory

    #---missing packets---
    #Whether we are in a burst of silence
    if burst_remaining> 0: #Silence-no emission
        burst_remaining -= 1
        sequence_number += 1
        time.sleep(1)
        continue

    #occassionally start a bursty loss
    if random.random() < 0.02: #2% chance
        burst_remaining= random.randint(2, 5) #for seconds of silence
        recovering_remaining= 3
        sequence_number += 1
        time.sleep(1)
        continue

    if random.random() < 0.05: #5% chance 
        sequence_number += 1
        time.sleep(1)
        continue

    #---Sequence Corruption---
    emit_sequence = sequence_number

    if random.random()< 0.01 and last_sequence_emitted is not None: #duplicate packet
        emit_sequence = last_sequence_emitted

    elif random.random()< 0.01: #skipped sequence
        emit_sequence = sequence_number + 1
        sequence_number += 1

     # --- Status determination ---
    if recovering_remaining > 0:
        status = "RECOVERING"
        recovering_remaining -= 1
    elif abs(noise) > 0.4 or abs(baseline_value - initial_baseline) > 1.0:
        status = "DEGRADED"
    else:
        status = "NOMINAL"


    reading = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensor_id": sensor_id,
        "sequence_number": emit_sequence,
        "value": round(observed_value, 3),
        "status": status
    }

    print(reading)
    last_sequence_emitted= emit_sequence
    sequence_number += 1
    time.sleep(1)  # Emit data every second
