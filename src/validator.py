from datetime import datetime, timedelta
from prometheus_client import Counter

validator_violations = Counter(
    "validator_violations_total",
    "Total validator invariant violations",
    ["type"],
)


class Validator:
    """
    Declares physical invariants of the telemetry universe.
    Detects violations but never mutates or drops packets.
    """

    def __init__(
        self,
        max_future_skew_seconds=5,
        max_backward_skew_seconds=2,
    ):
        self.last_sequence = {}   # per sensor
        self.last_timestamp = {}  # per sensor

        self.max_future_skew = timedelta(seconds=max_future_skew_seconds)
        self.max_backward_skew = timedelta(seconds=max_backward_skew_seconds)

        self.violation_count = 0

    def validate(self, packet: dict):
        """
        Validate a packet and return it unchanged.
        All violations are logged, not raised.
        """
        self._check_schema(packet)
        self._check_sequence(packet)
        self._check_timestamp(packet)

        return packet

    # ------------------ Checks ------------------

    def _check_schema(self, packet):
        REQUIRED_FIELDS = {
            "sensor_id": str,
            "sequence_number": int,
            "timestamp": str,
            "value": (int, float),
            "status": str,
        }

        for field, expected_type in REQUIRED_FIELDS.items():
            if field not in packet:
                self._violation(
                    f"Missing field: {field}",
                    vtype="schema_missing_field",
                )
                return

            if not isinstance(packet[field], expected_type):
                self._violation(
                    f"Field '{field}' has invalid type "
                    f"(expected {expected_type}, got {type(packet[field])})",
                    vtype="schema_invalid_type",
                )

        if packet["status"] not in {
            "OK", "NOMINAL", "DEGRADED", "MISSING", "RECOVERING"
        }:
            self._violation(
                f"Unknown status value: {packet['status']}",
                vtype="schema_unknown_status",
            )

    def _check_sequence(self, packet):
        sensor = packet["sensor_id"]
        seq = packet["sequence_number"]

        last = self.last_sequence.get(sensor)
        if last is not None and seq < last:
            self._violation(
                f"Sequence went backwards for {sensor} "
                f"(last={last}, got={seq})",
                vtype="sequence_backwards",
            )

        self.last_sequence[sensor] = max(seq, last) if last is not None else seq

    def _check_timestamp(self, packet):
        sensor = packet["sensor_id"]

        try:
            ts = datetime.fromisoformat(packet["timestamp"].replace("Z", ""))
        except Exception:
            self._violation(
                f"Invalid timestamp format: {packet['timestamp']}",
                vtype="timestamp_parse_error",
            )
            return

        now = datetime.utcnow()

        if ts - now > self.max_future_skew:
            self._violation(
                f"Timestamp too far in future for {sensor}: {ts.isoformat()}",
                vtype="future_timestamp",
            )

        last_ts = self.last_timestamp.get(sensor)
        if last_ts and last_ts - ts > self.max_backward_skew:
            self._violation(
                f"Timestamp went backwards for {sensor} "
                f"(last={last_ts.isoformat()}, got={ts.isoformat()})",
                vtype="timestamp_backwards",
            )

        self.last_timestamp[sensor] = max(ts, last_ts) if last_ts else ts

    # Reporting 

    def _violation(self, message, vtype="unknown"):
        self.violation_count += 1
        validator_violations.labels(type=vtype).inc()
        print(f"🚨 VALIDATION VIOLATION: {message}")
