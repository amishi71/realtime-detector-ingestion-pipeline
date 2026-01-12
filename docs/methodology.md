`docs/methodology.md`.

# Methodology

## Hypothesis

Cleaning the data before using it in a model will reduce prediction error when the data contains noise, drift, and packet loss.

In simple terms:
if the data is messy, fixing it first should help the model make better predictions.

---

## Experimental Setup

The experiment runs inside a live streaming pipeline.

A simulator generates data for multiple sensors. The data includes:

* random noise
* slow drift
* missing packets
* burst losses
* out-of-order or skipped sequence numbers

The data is then sent through two different paths:

**1. Raw path**

```
Simulator → Redis → Downstream Model
```

**2. Clean path**

```
Simulator → Redis → Validator → Observer → Preprocessor → Clean Redis → Downstream Model
```

Both paths use the **same moving-average model**.
The only difference is whether the data was cleaned before it reached the model.

This makes it possible to see how much the preprocessing alone changes the results.

---

## What is Changed in the Experiment

The model itself does not change.
Only the quality of the data changes.

The simulator controls:

* how much noise is added
* how fast the sensor drifts
* how often packets are lost
* how long packet loss lasts
* the size of the moving-average window

These values stay fixed during a run but can be different for different sensors.

---

## What is Measured

For each sensor and each time step, the system measures:

```
error = | predicted value − true value |
```

This is done for:

* the raw data stream
* the cleaned data stream

This gives two error values that can be compared directly.

---

## Metrics

The system exports two metrics to Prometheus:

* `downstream_raw_prediction_error`
* `downstream_clean_prediction_error`

Grafana plots these values over time so the two data paths can be compared.

---

## How the Results Are Judged

The hypothesis is considered correct if, most of the time,

```
cleaned prediction error < raw prediction error
```

especially during periods when the data has loss, drift, or recovery.

---

## Why this experiment is fair

The two data paths:

* use the same model
* use the same underlying signal
* run at the same time

The only difference is whether the data was cleaned first.

This means any change in prediction error is due to the preprocessing, not to the model or the input signal.

---

