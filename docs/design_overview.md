`docs/design_overview.md` 

# System Design

## Architecture

```
Multi-Sensor Simulator
        ↓
   Redis Streams (raw)
        ↓
   Redis Consumer
        ↓
   Validator  →  Observer
        ↓
   Preprocessor (cadence, imputation, normalization)
        ↓
   Redis Streams (clean)
        ↓
   Downstream Model (raw + clean)
        ↓
   Prometheus
        ↓
   Grafana
```

The simulator produces corrupted sensor data.
Redis is used to pass data between components.
The pipeline turns raw packets into clean, regular data frames.
The downstream model is used to check whether cleaning the data actually improves predictions.

The system separates three kinds of time:

* **Event time** (timestamps from sensors)
* **Processing time** (when Redis consumers handle packets)
* **Model time** (the fixed 1 Hz timeline created by the preprocessor)

The preprocessor aligns these into a single, regular time grid for the model.

---

## Components

### Sensor Simulator

Generates data for multiple sensors.
Each sensor has its own noise, drift, packet loss, and recovery behavior.

This simulates how real hardware sensors behave under imperfect conditions.

---

### Redis Streams

Used as the messaging layer between components.

Two streams are used:

* `sensor_packets` for raw data
* `sensor_clean` for processed data

This allows raw and cleaned data to be handled separately.

---

### Validator

Checks basic physical and structural rules:

* message format is correct
* packet order increases per sensor
* timestamps move forward

If something is wrong, it is recorded but the packet is still passed through.

---

### Observer

Tracks how reliable each sensor is over time:

* missing packets
* time gaps
* degraded periods
* recovery events

These measurements are sent to Prometheus.

---

### Preprocessor

Converts irregular sensor data into regular model-ready frames.

It guarantees:

* fixed 1 Hz output
* ordered sequence numbers
* per-sensor normalization (z-scores)
* explicit quality flags

Missing data is filled using imputation or marked unusable.
The preprocessor only uses past data, never future data, so results are causal.

---

### Downstream Model

A simple moving-average predictor.

It runs twice:

* once on raw data
* once on cleaned data

This makes it possible to compare prediction error before and after preprocessing.

A simple model is used so that any improvement comes from better data, not a more complex model.

---

### Prometheus

Collects system metrics:

* validator errors
* observer health
* output rate
* prediction errors

---

### Grafana

Shows dashboards for:

* sensor corruption
* system health
* raw vs clean model error

This makes it easy to see whether preprocessing helps.

---

## Failure Modes Modeled

The system is built to handle:

* random and burst packet loss
* timestamp errors
* duplicate and missing packets
* sensor drift and recovery

These are treated as normal operating conditions, not special cases.

In a real system, these measurements could be used by control software to recalibrate sensors, ignore bad data, or trigger maintenance.

---
