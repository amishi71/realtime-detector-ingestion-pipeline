`docs/design_overview.md` 

# Results

## Observations

Across all sensors, the cleaned data gives lower prediction error than the raw data for most of the run.

The difference is easiest to see during:

* packet loss
* recovery after data gaps
* periods with strong drift

When the raw stream becomes irregular, the model’s error increases quickly.
The cleaned stream stays more stable, which shows that the preprocessing step is helping the model see the real signal instead of just noise.

---

## Key Plots

The Grafana dashboard includes these main panels:

**1. Raw vs Clean Prediction Error**

Two time series are shown:

* `downstream_raw_prediction_error`
* `downstream_clean_prediction_error`

This lets us compare how the model performs on unprocessed data versus cleaned data.

**2. Sensor Health and Data Loss**

This panel shows:

* missing packets
* degraded sensor periods
* recovery times

These are plotted alongside the prediction error so we can see how data quality affects model performance.

---

## Quantitative Outcome

For every sensor, the average prediction error is lower when using cleaned data than when using raw data:

```
mean(clean_error) < mean(raw_error)
```

This is true during normal operation and also during periods of heavy packet loss and recovery.

The difference becomes larger when the data is more corrupted, which means the preprocessing layer is doing most of the work during difficult conditions.

---

## What This Means

The pipeline is not just smoothing the data.

It makes the data easier for the model to predict, especially when the input stream is messy.
This shows that time alignment and normalization help models work better when sensor data is unreliable.
