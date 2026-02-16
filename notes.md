# Notes — Real-Time Detector-Style Ingestion & Cleaning Pipeline
---

## What This Is
- A streaming system that generates corrupted sensor data, cleans it in real time, and checks whether preprocessing actually improves prediction accuracy.

- Question: does cleaning help, or can models just learn through the noise?

---

## Running the Stack

```bash
# Start everything
docker compose up --build

# Run in background
docker compose up -d --build

# Watch logs
docker compose logs -f

# Shut down
docker compose down

# Full clean restart
docker compose down -v
```
---

## Current State
**Working:**
- 3 simulated sensors with independent drift, noise, loss patterns
- Redis streams moving data between services
- Validator checks packet structure and sequence
- Observer tracks loss rates and state changes
- Preprocessor normalizes and imputes missing data
- Real-time Grafana dashboards
- Fully containerized

**In progress:**
- 24-hour stability runs
- Understanding why preprocessing helps one sensor but hurts two others
- Per-sensor tuning instead of global parameters

**Latest numbers** (3 sensors, 1 Hz, ~5% single packet loss, 2% burst loss; see `config/simulator.yaml`):

| Sensor      | Raw error | Clean error | Change    |
| ----------- | --------- | ----------- | --------- |
| 001         | 0.371     | 0.389       | +4.9%     |
| 002         | 0.597     | 0.588       | -1.5%     |
| 003         | 0.230     | 0.266       | +15.7%    |
| **Average** | **0.399** | **0.414**   | **+3.8%** |

- Preprocessing made average error worse. 
- That wasn't expected: deeper research needed.

---

## Notes:

### Preprocessing isn't automatically helpful
- Assumed cleaning would improve accuracy. 
- For two of three sensors, it degraded. 
- Global normalization might be smoothing out signal along with noise. 
- Next step: per-sensor parameters.

### Redis streams work well for this
- Consumer groups let each service run independently. 
- If preprocessor crashes, validator keeps going. 
- At-least-once delivery means no silent data loss.

### Dashboards catch what you miss
- Wouldn't have noticed sensor 003's error spike without real-time visualization. 
- For future projects ensure: metrics first, then optimization.

### Realistic simulation matters
- Gaussian noise is too clean. 
- Burst loss + drift + jitter actually stresses the system. 
- If it works here, it might work on real hardware.

---

## What I'd Do Differently Starting Over
- Assume multiple sensors from day one (refactoring is tedious)
- Add metrics before adding features
- Write tests earlier
- Get real sensor data sooner

---

## Building notes

### Phase 1 — Simulator
Python script emitting packets with:
- timestamp (event time, not arrival)
- sensor_id
- sequence_number
- value
- status (NOMINAL/DEGRADED/RECOVERING)

* Corruption is configured: noise, drift, random loss, burst loss (2-5 packets), sequence skips.

### Phase 2 — First Transport
- Started with in-memory message bus (just Python objects).  
- Tested logic without Redis complexity.

### Phase 3 — Validator
Check:
- required fields exist
- sequence numbers don't go backwards
- timestamps aren't wildly out of order

* Rule: validate but never drop. Log violations and keep packets moving.

### Phase 4 — Observer
Passive monitor- track:
- inferred packet loss
- large time gaps
- degraded/recovering states

* No modific*ations.

### Phase 5 — Preprocessor
Where cleaning happens, enforce:
- one output per second (logical clock)
- strictly increasing sequence
- quality labels (VALID/IMPUTED/UNUSABLE)

Missing data handling (implemented):
- Gaps ≤ `max_linear_gap`: linear interpolation between the last known value and the next real packet (emitted as `IMPUTED`).
- Gaps > `max_linear_gap` and ≤ `max_spline_gap`: polynomial fit using recent window values plus the next packet as endpoint (NumPy `polyfit`, degree up to 3), emitted as `IMPUTED`.
- Gaps > `max_spline_gap`: fallback strategy (default: forward-fill) and may be marked `UNUSABLE` if exceeding `unusable_after`.

* Note: imputed frames are emitted with `quality: "IMPUTED"` and are included in rolling statistics unless marked `UNUSABLE`. The implementation uses `numpy`.

### Phase 6 — Redis
Swap in Redis streams without changing core logic- decoupled services:
- `sensor_packets` (raw)
- `clean_packets` (preprocessed)

### Phase 7 — Prometheus + Grafana
Metrics everywhere:
- validator violations
- observer loss counts
- preprocessor throughput
- prediction errors

* Dashboards compare raw vs clean error in real time. 

### Phase 8 — Multi-Sensor
- Refactor simulator for multiple independent sensors (001, 002, 003). 
- Each with its own baseline, drift, noise profile.

### Phase 9 — Normalization
- Raw values are scale-dependent. 
- Added per-sensor rolling mean/std so frames include:
```
normalized = (value - mean) / std
```

### Phase 10 — Evaluation Model
Simple moving average predictor (window size 5). Two identical models run in parallel:
- one on raw stream
- one on clean stream

* Prediction errors go to Prometheus. This isolates preprocessing as the only variable.

### Phase 11 — Containerization
Everything in Docker Compose:
- Redis
- all pipeline services
- Prometheus
- Grafana

---

## Current Properties
- Multi-sensor
- Event-time consistent
- Loss-tolerant
- Normalization-aware
- Observable
- Reproducible
- Experimentally structured

The pipeline runs end-to-end and isolates preprocessing impact on prediction error under drift, noise, and packet loss.

Does preprocessing help? 
>> Only **Sometimes.** 