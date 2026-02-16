`docs/design.md` 

# Design Overview

## Architecture

The system is composed of independent services connected through Redis Streams:
[sensor_simulator] → Redis → [validator] → Redis → [observer] → Redis → [preprocessor] → Redis → [model]

Each service:
- Runs in its own container  
- Communicates only via Redis  
- Has a single responsibility  

---

## Key Design Decisions

### Redis Streams for Transport

Reasons:
- Consumer groups support multiple readers  
- At-least-once delivery  
- Full service decoupling  
- Built-in acknowledgment  

Alternatives considered:
- Kafka (overhead too high for this scale)  
- RabbitMQ (less aligned with time-series use case)  
- In-memory bus (not container-scalable)  

---

### Never Drop Packets

The validator records violations but never drops packets.
- No silent data loss  
- Observable corrupted data remains   
- Measurable violations    

---

### Per-Sensor State

Each sensor maintains:
- Sequence tracking  
- Timestamp history  
- Rolling statistics  
- Recovery state  

* This prevents cross-sensor interference.

---

### Cadence Enforcement

The preprocessor emits exactly one frame per second, in order to-
- Create a predictable downstream clock  
- Simplify modeling  
- Force explicit missing-data handling  

---

### Parallel Model Paths

Two identical models run:
- Raw data path  
- Clean data path  

* Experimental variable- Preprocessing.

---

## Data Flow

Multi-Sensor Simulator  
↓  
Redis (raw stream)  
↓  
Validator → Observer  
↓  
Preprocessor  
↓  
Redis (clean stream)  
↓  
Downstream Model  
↓  
Prometheus  
↓  
Grafana  

---

## Time Model

The system separates:
- **Event time** (sensor timestamps)  
- **Processing time** (consumer handling time)  
- **Model time** (fixed 1 Hz grid)  

The preprocessor aligns all event-time data into the model-time grid.

---

## Components

### Sensor Simulator

Generates multi-sensor corrupted telemetry.

---

### Validator

Checks:
- Message structure  
- Monotonic sequence numbers  
- Forward-moving timestamps  

Does not discard packets.

Runtime notes:

- `timestamp_parse_error`: invalid timestamps (e.g., `BAD_TIMESTAMP`) are reported by the validator and increment `validator_violations_total{type="timestamp_parse_error"}`. The `preprocessor` substitutes the current UTC processing time when parsing fails to keep the pipeline moving.
- `sequence_backwards`: duplicates or backwards sequence numbers are logged as violations; the validator forwards the packet so downstream components (observer, preprocessor) remain observable.

---

### Observer

Tracks reliability metrics:
- Missing packets  
- Time gaps  
- Degradation  
- Recovery events  

* Exports metrics to Prometheus.
---

### Preprocessor

Guarantees:
- Fixed 1 Hz output  
- Ordered sequence  
- Per-sensor normalization  
- Quality flags  

---

### Downstream Model
- A moving-average predictor run on both raw and cleaned streams.

---

### Monitoring

Prometheus collects:
- Validator errors  
- Observer metrics  
- Output rate  
- Prediction errors  

Grafana visualizes:
- Corruption patterns  
- System health  
- Raw vs clean error comparison  

---

## Failure Modes Modeled

The system handles:
- Random packet loss  
- Burst loss  
- Timestamp errors  
- Duplicate packets  
- Sensor drift  
- Recovery events  

