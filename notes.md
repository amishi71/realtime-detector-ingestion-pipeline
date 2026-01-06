# Notes — Real-Time Detector Style Ingestion and Cleaning Pipeline

## Project goal

Build a small real-time telemetry pipeline that can handle bad sensor data.

The system should:

* accept unreliable input
* detect problems without changing the data
* produce clean, regular output for downstream use

This is meant to mimic how real detector / telemetry systems behave.

---

## Component 1 — Simulated Sensor Generator

### What it does

A long-running Python process that emits sensor packets at ~1 Hz.

Each packet has:

* `timestamp`
* `sensor_id`
* `sequence_number`
* `value`
* `status`

The simulator intentionally produces bad data to test the pipeline.

### Failure modes implemented

* **Noise**
  Small random changes around a base value.

* **Drift**
  Slow change in the baseline over time.

* **Missing packets**

  * random single drops
  * bursty drops (2–5 seconds)

* **Sequence issues**

  * skipped sequence numbers
  * rare duplicates

* **Status flags**

  * `NOMINAL`
  * `DEGRADED` (high noise or drift)
  * `RECOVERING` (after burst loss)

The simulator violates assumptions on purpose.

---

## Component 2 — Message Bus

A simple in-process message bus.

Responsibilities:

* hold a list of subscribers
* publish each packet to all subscribers

It does **not**:

* validate data
* buffer data
* modify packets

This keeps components loosely coupled.

---

## Component 3 — Observer

### Purpose

Watch the data stream and report problems without fixing anything.

### What it detects

* non-monotonic sequence numbers
* missing packets
* abnormal time gaps
* recovery periods
* degraded packets

### Design choice

The observer is **read-only**.
It never corrects data.

This keeps detection separate from correction.

---

## Component 4 — Preprocessor

### Purpose

Turn irregular, unreliable input into a clean downstream stream.

### Output rules

* Fixed cadence: **1 Hz**
* One output frame every second, no matter what
* Output sequence is strictly increasing
* Output sequence is independent of raw sensor sequence numbers

### Missing data handling

* **Single missing packet**
  Forward-fill last value
  Mark frame as `IMPUTED`

* **Multiple missing seconds**
  Continue forward-fill
  Mark all frames as `IMPUTED`

* **Long silence**
  Emit placeholder value
  Mark frames as `UNUSABLE`

The preprocessor never edits past frames.

### Quality labels

* `VALID` — direct from raw packet
* `IMPUTED` — filled due to missing data
* `UNUSABLE` — placeholder only, not reliable

---

## Invariants after preprocessing

Downstream systems can assume:

* time is regular
* every second has a frame
* every frame has a value
* quality is always explicit
* sequence numbers are monotonic

No guessing required downstream.

---

## Component 5 — Validator

### Purpose

Check whether incoming packets violate basic physical rules.

Validator checks:

* schema correctness
* sequence numbers going backwards
* timestamps jumping backwards or too far into the future

### Design choice

Invalid packets are **observed, not dropped**.

This separates:

* impossible data (validator)
* degraded data (observer)
* corrected data (preprocessor)

---

## Canonical packet schema

```
sensor_id        : string
sequence_number  : int
timestamp        : ISO-8601 (event time, UTC)
value            : float
status           : enum
```

Timestamps are **event-time**, not ingestion-time.

---

## Entry point refactor

### Problem

The simulator was doing too many things:

* generating data
* wiring components
* acting as entry point

### Fix

* Added `src/main.py`
* Simulator is now a callable function
* `main.py` wires everything together

Run using:

```bash
python3 -m src
```

This fixes imports and keeps responsibilities clean.

---

## Current state

The system now has:

* a realistic sensor simulator
* a validator for impossible data
* an observer for degradation
* a preprocessor that enforces cadence and quality
* a clean entry point

The pipeline runs continuously and produces regular output even under failure.

---

## Next step

Add metrics:

* validator violation counts
* observer counters
* preprocessor output rate

Metrics will observe behavior only.
They will not change system logic.

