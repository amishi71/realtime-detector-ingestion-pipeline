`docs/results.md` 

# Results

## Test Run

Date: 2026-02-15  
Duration: 6 hours  
Configuration: 3 sensors, 1 Hz, ~5% single packet loss (2% burst loss); see `config/simulator.yaml` for exact values

---

## Summary

| Metric              | Raw    | Clean              | Change |
| ------------------- | ------ | ------------------ | ------ |
| Mean Absolute Error | 0.399  | 0.414              | +3.8%  |
| Missing Packets     | ~3,200 | —                  | —      |
| Frames Processed    | —      | ~64,800 (expected) | —      |

Missing packets counted cumulatively across sensors.

---

## Per-Sensor

| Sensor     | Raw   | Clean | Change |
| ---------- | ----- | ----- | ------ |
| sensor_001 | 0.371 | 0.389 | +4.9%  |
| sensor_002 | 0.597 | 0.588 | -1.5%  |
| sensor_003 | 0.230 | 0.266 | +15.7% |

---

## Observations

- One sensor improved.
- Two sensors degraded.
- Pipeline handled corruption correctly.
- Naive global preprocessing does not generalize across sensors.

---

## Interpretation

The pipeline:

- Simulates realistic corruption  
- Maintains streaming stability  
- Enables A/B comparison  

However, preprocessing must be sensor-specific to consistently improve performance.
