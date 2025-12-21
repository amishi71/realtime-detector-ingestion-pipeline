# 20/12/25

## Component 1 — Simulated Sensor Generator

### Task 1: Create a uniform simulated sensor generator
* Built first long-running sensor simulator
* Learned process lifecycle and operator interrupts
* Confirmed `python3` usage on macOS
* Verified stable cadence and sequence behavior

### Task 2: Corrupt the data — add signals
*[A physical quantity + imperfections]*
* Added noise and drift
* Added missing packets and sequence corruption
* Added flags for status determination

---

## Checkpoint 1: Observed failure modes (simulator)
This checkpoint documents what kinds of data corruption the simulator can already produce, and what we can observe with the current --Observer--.

---

### Types of corruption

#### Noise
* Small random fluctuations around a baseline value
* Doesn’t accumulate over time
* Represents normal sensor jitter

#### Drift
* Slow, continuous change in baseline value over time
* Accumulates
* Represents calibration drift or thermal effects

#### Missing packets
* Single dropped packets (random)
* Bursty packet loss (2–5 seconds of silence)
* Causes gaps in sequence numbers and timestamps

#### Sequence corruption
* Skipped sequence numbers
* Rare duplicate sequence numbers
* Breaks assumptions about monotonic ordering

#### Degraded quality states
* Packets flagged as **DEGRADED** when noise is high or drift exceeds threshold
* Temporary **RECOVERING** periods after burst losses

---

These represent realistic failure modes seen in real-world telemetry systems.

---

### Observer capabilities

The observer can:
* Detect non-monotonic sequence numbers
* Count and report missing packets
* Detect abnormal time gaps
* Track transitions into and out of recovery
* Count degraded packets
* Maintain internal state across packets

---

### Current limitations
* Observer prints logs but does not store metrics
* No aggregation windows yet
* No alert thresholds defined
* No persistence

---

