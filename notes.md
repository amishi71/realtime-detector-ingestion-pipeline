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

-------

## Checkpoint — Project Status

### What is done so far

**Core data behavior**

* Sensor packets use event-time timestamps
* Noise, drift, missing packets, and sequence errors are simulated
* Status flags implemented: `NOMINAL`, `DEGRADED`, `RECOVERING`

**Pipeline structure**

* Data flow:
  `Simulator → MessageBus → Validator → Observer → Preprocessor`
* Each component has a single responsibility
* Detection layers do not modify data

**Packet contracts**

* Packet schema is fixed
* Downstream assumptions are explicit
* Output sequence is monotonic and independent of raw sensor order

**Preprocessing guarantees**

* Fixed output cadence (1 Hz)
* Forward-fill for short gaps
* `UNUSABLE` frames for long silence
* Past frames are never changed

**Observability**

* Prometheus metrics added
* Counters for:

  * observer health
  * validator violations
  * preprocessor timing
* `/metrics` endpoint is running

---

### What is still pending

**Containerization**

* Dockerfile not added yet
* docker-compose setup pending

**Streaming backend**

* Currently using in-process MessageBus
* Need to switch to Redis Streams or Kafka

**Metrics visualization**

* Prometheus is working
* Grafana dashboards not set up

**Downstream consumer**

* No downstream model yet
* Need a simple consumer to compare:

  * raw data vs cleaned data

**Evaluation**

* Latency, throughput, and missing-data stats not measured yet

**Repository cleanup**

* README needs:

  * architecture diagram
  * run instructions
  * component descriptions

---
