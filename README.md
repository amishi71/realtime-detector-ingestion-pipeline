
# Real-time Detector-style Ingestion & Cleaning Pipeline

>> Prove that cleaning noisy, drifting, lossy telemetry improves downstream prediction accuracy.

---

## What problem does this solve?

Real-world sensors produce corrupted data: noise, drift, packet loss, timestamp jitter, and out-of-order delivery.
Models trained on raw telemetry learn transport and hardware artifacts instead of the underlying physical signal.

This system simulates that corruption, cleans it in real time, and **quantitatively proves** that preprocessing improves inference.

---

## Architecture

```
Simulator
   ↓
Redis Streams
   ↓
Validator → Observer
   ↓
Preprocessor (cadence, imputation, normalization)
   ↓
Clean Stream
   ↓
Downstream Model
   ↓
Prometheus → Grafana
```

---

## How to run

```bash
docker compose up --build
```

Then open:

* **Grafana**: [http://localhost:3000]
* **Prometheus**: [http://localhost:9090]
* Login: `admin / admin`

---

## What the dashboards show

The main dashboard compares prediction error of the same downstream model on:

* raw telemetry
* cleaned, normalized telemetry

If the cleaned error stays below the raw error, preprocessing is improving signal quality.

---

## What this demonstrates

Under realistic noise, drift, and packet loss, real-time cleaning and normalization significantly improve downstream model accuracy.

---

# Downstream Model

The downstream model is a simple moving-average predictor.
For each sensor, it keeps a rolling window of the last *N* values and predicts the next value as their mean.

Two parallel models are run:

* one on the **raw telemetry stream**
* one on the **cleaned, normalized stream**

For the cleaned path, the model operates in **z-score space**.
Let the model output a normalized prediction ( z_{\text{pred}} ).
This is converted back to physical units using the current rolling statistics:

[
\hat{x} = \mu + z_{\text{pred}} \cdot \sigma
]

where
( \mu ) is the rolling mean and
( \sigma ) is the rolling standard deviation for that sensor.

The cleaned prediction error is computed as:

[
\text{error}*{\text{clean}} = \lvert \hat{x} - x*{\text{actual}} \rvert
]

The raw model computes:

[
\text{error}*{\text{raw}} = \lvert \bar{x}*{\text{raw}} - x_{\text{actual}} \rvert
]

where ( \bar{x}_{\text{raw}} ) is the moving-average prediction on raw values.

Both errors are measured in the **same physical units**, making the comparison fair.
The same model and window size are used for both streams, so any reduction in error is caused by preprocessing, not by model choice.

---

# Experimental Protocol

The system evaluates two parallel data paths in real time:

**Raw path**
Simulator → Redis → Downstream model

**Clean path**
Simulator → Redis → Validator → Observer → Preprocessor → Clean Redis stream → Downstream model

Both paths feed the **same moving-average predictor**.
The only difference is whether the model receives raw telemetry or cleaned, normalized telemetry.

Prediction errors from both paths are continuously recorded and compared.
This isolates the causal effect of preprocessing.

---

# Dashboards

The Grafana dashboards that visualize all metrics are included in the repository.

To load them:

1. Open Grafana at
   `http://localhost:3000`
2. Go to **Dashboards → Import**
3. Upload
   `dashboards/realtime_detector.json`

The main panels compare:

* `downstream_raw_prediction_error`
* `downstream_clean_prediction_error`

When the cleaned line stays below the raw line, the pipeline is improving inference.

---

# Reproducibility

The entire system is containerized using Docker Compose, including:

* Redis
* the telemetry pipeline
* Prometheus
* Grafana

Anyone can reproduce the experiment by running:

```bash
docker compose up --build
```

and opening the dashboards.
The same simulator, corruption patterns, preprocessing logic, and evaluation model will run, allowing the results to be independently verified.

----