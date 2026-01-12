# Notes — Real-Time Detector-Style Ingestion & Cleaning Pipeline
---

## Project goal

Build a real-time telemetry pipeline that behaves like a detector.

The system should:

* accept corrupted sensor data
* detect problems without hiding them
* output a clean, regular stream
* prove that cleaning improves downstream prediction

This is not about “pretty data”.
It’s about making data **usable for models**.

---

# Phase 1 — Getting data into the system

## Sensor simulator (first version)

I started by writing a Python loop that emits one packet per second.

Each packet contains:

* `timestamp` (event time, not arrival time)
* `sensor_id`
* `sequence_number`
* `value`
* `status`

At first it was only one sensor.

### Failure modes added

The simulator was made intentionally unreliable:

* **Noise** – random jitter around a baseline
* **Drift** – baseline slowly moves over time
* **Missing packets**

  * random drops
  * burst losses (2–5 seconds)
* **Sequence corruption**

  * skipped sequence numbers
  * rare duplicates
* **Status flags**

  * `NOMINAL`
  * `DEGRADED`
  * `RECOVERING`

This gave me a controllable but realistic source of bad telemetry.

---

# Phase 2 — Wiring the pipeline

## MessageBus (initial transport)

I first used a simple in-memory `MessageBus`:

* it keeps a list of subscribers
* it publishes packets to all of them
* it does not modify or validate anything

This made it easy to test the flow before adding real infrastructure.

---

## Validator

The validator checks if packets violate physical invariants.

It checks:

* required fields and types
* sequence numbers going backwards
* timestamps jumping backwards
* timestamps too far in the future

Important design decision:

> Violations are logged but packets are never dropped.

This keeps detection separate from correction.

Later, the validator exported Prometheus metrics so violations could be graphed.

---

## Observer

The observer watches the raw stream without changing it.

It tracks:

* missing packets
* large time gaps
* degraded packets
* recovery periods

This gives me a live picture of data quality.

Observer also exports Prometheus counters.

---

# Phase 3 — Making the data usable

## Preprocessor

This is where raw telemetry becomes clean data.

The preprocessor enforces:

* fixed output cadence (1 Hz)
* exactly one frame per second
* strictly increasing logical sequence
* explicit quality labels

The output sequence is **not** the sensor sequence.
It is a logical clock for downstream systems.

### Missing data handling

* one missing → forward fill → `IMPUTED`
* multiple missing → keep filling → `IMPUTED`
* long silence → placeholder → `UNUSABLE`

Frames are never rewritten.
Time always moves forward.

---

# Phase 4 — Making it real (Redis)

## Replacing MessageBus with Redis Streams

The in-memory bus was replaced with Redis without changing logic.

Redis is used only as a **stream**:

* simulator writes packets with `XADD`
* consumers read using `XREADGROUP`

Two streams exist:

* `sensor_packets` (raw)
* `sensor_clean` (preprocessed frames)

The simulator does not know about Redis — it only calls `emit(packet)`.

---

# Phase 5 — Observability

## Prometheus

Metrics added for:

* validator violations
* observer counts
* preprocessor output rate
* downstream model error

All metrics are exposed on `/metrics`.

## Grafana

Grafana shows:

* raw vs clean prediction error
* packet loss
* sensor degradation
* cleaning benefit

This makes the system falsifiable instead of theoretical.

---

# Phase 6 — Multi-sensor support

The simulator was upgraded to produce multiple sensors:

* `sensor_001`
* `sensor_002`
* `sensor_003`

Each has its own:

* baseline
* drift
* noise
* loss

This turned the pipeline from a toy into a detector array.

---

# Phase 7 — Normalization

Raw sensor values are not comparable across sensors.

So I added per-sensor rolling statistics:

* mean
* standard deviation

Each output frame now includes:

```
normalized = (value − mean) / std
```

This lets models see **signal instead of scale**.

---

# Phase 8 — Downstream model

A simple moving-average predictor.

For each sensor:

* predict next value as mean of last N values
* compute absolute error

The same model runs on:

* raw stream
* cleaned, normalized stream

Both errors are exported to Prometheus.

If cleaning works, cleaned error should be lower.

---

# Phase 9 — Containerization

Everything runs in Docker:

* Redis
* pipeline
* Prometheus
* Grafana

Grafana uses a persistent volume.
Dashboards are exported as JSON and committed.

---

# Current state

The system is now:

* multi-sensor
* event-time correct
* loss-tolerant
* normalized
* observable
* reproducible
* quantitatively validated

It runs end-to-end and proves that cleaning improves inference.

---

# Final check against the original goal

Original spec:

> “Build a streaming ingestion pipeline that cleans, aligns, imputes, and emits normalized data to a downstream model.
> Show that cleaning improves downstream model accuracy.”

>> Now?
- Under drift, loss, and jitter, preprocessing reduces predictive error across all sensors — and Grafana shows it live.

---

