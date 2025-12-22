from datetime import datetime, timedelta


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
        self.last_value = None

        self.logical_sequence = 0
        self.missing_streak = 0

    def process(self, packet):
        """
        Consume a raw packet and emit one or more clean frames.
        Returns a list of output frames.
        """
        outputs = []

        packet_time = datetime.fromisoformat(packet["timestamp"].replace("Z", ""))
        packet_used = False

        # First packet initializes the timeline
        if self.last_output_time is None:
            self.last_output_time = packet_time
            frame = self._emit_frame(packet_time, packet["value"], "VALID")
            outputs.append(frame)
            return outputs

        next_time = self.last_output_time + self.cadence

        while next_time <= packet_time or not packet_used:
            if not packet_used and packet_time <= next_time:
                # Use real packet as soon as it can fill a frame
                self.missing_streak = 0
                frame = self._emit_frame(next_time, packet["value"], "VALID")
                packet_used = True
            else:
                # Imputation
                self.missing_streak += 1
                quality = (
                    "UNUSABLE"
                    if self.missing_streak > self.unusable_after
                    else "IMPUTED"
                )
                frame = self._emit_frame(next_time, self.last_value, quality)

            outputs.append(frame)
            self.last_output_time = next_time
            next_time += self.cadence

        return outputs

    def _emit_frame(self, timestamp, value, quality):
        frame = {
            "timestamp": timestamp.isoformat() + "Z",
            "sequence": self.logical_sequence,
            "value": value,
            "quality": quality,
        }

        self.last_value = value
        self.logical_sequence += 1
        return frame
