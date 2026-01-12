`docs/discussion.md` 

# Discussion

## Interpretation

The results show that cleaning and aligning the data before running a model makes a real difference.

When the moving-average model is run on the raw stream, it has to deal with:

* missing packets
* gaps when data is lost
* uneven timestamps
* different value scales across sensors
* slow drift in readings

Because of this, the model’s **mean absolute error (MAE)** increases even when the true signal itself is not changing much.

After preprocessing, the model sees:

* a fixed 1 Hz time grid
* no missing frames
* quality labels for unreliable data
* normalized values for each sensor

With these conditions, the model is mainly learning changes in the signal instead of errors caused by the data pipeline.

For example, when packet loss is around 20–30%, the MAE on the raw stream increases significantly, while the MAE on the cleaned stream stays much closer to its baseline value.

The Grafana dashboards consistently show that:

> prediction error on the cleaned stream is lower than on the raw stream

even during periods of packet loss and recovery.

This shows that the preprocessing is not just making the data look nicer — it is improving how well the model can make predictions.

---

## Why this matters

In real systems, many failures are caused by bad data rather than bad physics.

For example, a temperature sensor that slowly drifts can make a stable system look unstable, which could trigger the wrong control actions or unnecessary shutdowns.

This project shows that:

* data cleaning is not optional
* it is part of the inference system itself

By separating detection (finding bad data), correction (fixing or marking it), and modeling, the overall system becomes more stable and easier to trust.

---

## Limitations

This project keeps some parts simple on purpose.

Main limitations:

* The downstream model is just a moving-average predictor. It is good for comparison, but it does not capture complex behavior.

* The data is simulated. While the noise and failures are realistic, they do not come from a real physical device.

* The system has no feedback loop. It detects and measures problems, but it does not act on them.

* The preprocessing parameters (such as window sizes and normalization ranges) are fixed and not learned automatically.

These choices were made so the effect of preprocessing could be clearly measured.

---

## Future Work

There are several ways this system could be extended.

Possible next steps include:

* Replacing the moving-average model with models such as Kalman filters, ARIMA, or neural time-series models.

* Using real telemetry data from IoT devices or public datasets.

* Adding a control layer that reacts to data quality problems by changing sampling rates or flagging sensors.

* Running ablation tests to measure how much each preprocessing step (imputation, normalization, cadence) contributes to the final error reduction.

---

