from datetime import datetime

class Observer:
    def __init__(self):
        self.last_sequence = None
        self.last_timestamp = None

        self.missing_packets = 0 #counters, not detectors.
        self.degraded_count = 0

        self.in_recovery = False  #state, not a counter.
        self.recovery_length = 0

    def observe(self, packet: dict): #once per emitted packet.

        """
        Observe a single sensor packet and log operational insights.
        """

        seq = packet["sequence_number"]
        ts = datetime.fromisoformat(packet["timestamp"].replace("Z", ""))
        status = packet["status"]

        # --- Sequence integrity ---
        if self.last_sequence is not None:
            expected = self.last_sequence + 1 #encodes the invariant.

            if seq != expected: #flags discontinuity.
                missed = max(0, seq - expected)
                if missed > 0:
                    self.missing_packets += missed
                    print(f"⚠️ Missing {missed} packets (expected {expected}, got {seq})")

        # --- Timestamp spacing ---
        if self.last_timestamp is not None:
            delta = (ts - self.last_timestamp).total_seconds()
            if delta > 1.5:
                print(f"⚠️ Time gap of {delta:.2f}s detected")

        # --- Status transitions ---
        if status == "DEGRADED":
            self.degraded_count += 1
            print("⚠️ Sensor entered DEGRADED state")

        if status == "RECOVERING":
            if not self.in_recovery:
                self.in_recovery = True
                self.recovery_length = 0
                print("ℹ️ Entered RECOVERING state")
            self.recovery_length += 1
        else:
            if self.in_recovery:
                print(f"ℹ️ Recovered after {self.recovery_length} packets")
                self.in_recovery = False

        # --- Update memory ---
        self.last_sequence = seq
        self.last_timestamp = ts
