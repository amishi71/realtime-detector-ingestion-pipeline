

#validation.py

"""
Validation scenarios for Preprocessor (wind-tunnel tests)

Scenario 1: Perfect data
- One packet per second
- Expect: all output frames marked VALID

Scenario 2: Short gap
- Skip 2 seconds
- Expect: IMPUTED frames, then return to VALID

Scenario 3: Long gap
- Skip more than unusable_after seconds
- Expect: transition from IMPUTED to UNUSABLE

Scenario 4: Late arrival
- Packet arrives after cadence has advanced
- Observe: does missing streak reset correctly?
"""

from datetime import datetime, timedelta
from src.preprocessor import Preprocessor

def log_frame(frame):
    print(
        f"SEQ={frame['sequence']} "
        f"TIME={frame['timestamp']} "
        f"VALUE={frame['value']} "
        f"QUALITY={frame['quality']}"
    )


def scenario_perfect_data():
    print("\n--- Scenario 1: Perfect data ---")
    pre = Preprocessor()
    t0 = datetime.utcnow()

    for i in range(5):
        packet = {
            "timestamp": (t0 + timedelta(seconds=i)).isoformat() + "Z",
            "value": 100.0
        }
        frames = pre.process(packet)
        for f in frames:
            log_frame(f)


def scenario_short_gap():
    print("\n--- Scenario 2: Short gap ---")
    pre = Preprocessor()
    t0 = datetime.utcnow()

    packet_times = [0, 1, 4]  # missing t=2,3

    for offset in packet_times:
        packet = {
            "timestamp": (t0 + timedelta(seconds=offset)).isoformat() + "Z",
            "value": 100.0
        }
        frames = pre.process(packet)
        for f in frames:
            log_frame(f)


def scenario_long_gap():
    print("\n--- Scenario 3: Long gap ---")
    pre = Preprocessor(unusable_after=3)
    t0 = datetime.utcnow()

    packet_times = [0, 1, 7]  # long silence

    for offset in packet_times:
        packet = {
            "timestamp": (t0 + timedelta(seconds=offset)).isoformat() + "Z",
            "value": 100.0
        }
        frames = pre.process(packet)
        for f in frames:
            log_frame(f)


def scenario_late_arrival():
    print("\n--- Scenario 4: Late arrival ---")
    pre = Preprocessor()
    t0 = datetime.utcnow()

    packets = [
        (0, 100.0),
        (1, 100.0),
        (1.3, 105.0),  # late packet
        (4, 110.0)
    ]

    for offset, value in packets:
        packet = {
            "timestamp": (t0 + timedelta(seconds=offset)).isoformat() + "Z",
            "value": value
        }
        frames = pre.process(packet)
        for f in frames:
            log_frame(f)


if __name__ == "__main__":
    scenario_perfect_data()
    scenario_short_gap()
    scenario_long_gap()
    scenario_late_arrival()
