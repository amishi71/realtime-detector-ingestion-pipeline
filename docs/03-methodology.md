`docs/methodology.md` 

# Methodology

## Hypothesis

- Cleaning corrupted streaming data before modeling reduces prediction error when the data contains noise, drift, and packet loss.

---

## Experimental Setup

* The experiment runs inside the live pipeline.
* The simulator injects:
- Random noise  
- Drift  
- Packet loss  
- Burst loss  
- Out-of-order sequences  

* Two paths are evaluated:
- Raw Path:  
Simulator → Redis → Model  

- Clean Path:  
Simulator → Validator → Observer → Preprocessor → Redis → Model  

* Both use the same moving-average model, differing only in preprocessing.
---

## Controlled Variables

The simulator controls:
- Noise magnitude  
- Drift rate  
- Packet loss frequency  
- Loss duration  
- Moving-average window size  

* These remain constant within a run.

---

## Measurement

* For each timestamp:
error = | predicted value − true value |

Measured for:
- Raw stream  
- Clean stream  

---

## Metrics Exported
- downstream_raw_prediction_error  
- downstream_clean_prediction_error  

* Visualized in Grafana.

Runtime/diagnostic metrics:

- `validator_violations_total` (labels include `timestamp_parse_error`, `sequence_backwards`) — useful for understanding how many malformed or out-of-order packets were seen during a run.
- Preprocessor counters: `preprocessor_imputed_frames_total` and `preprocessor_unusable_frames_total` surface how often imputation or unusable labels were emitted during gaps.

These diagnostic metrics help interpret when and why preprocessing changes the downstream error.

---

## Evaluation Rule

The hypothesis holds if:
cleaned_error < raw_error  
for most time periods, especially during corruption.

---

## Fairness

Both paths:
- Use the same model  
- Use the same signal  
- Run simultaneously  

* Only preprocessing differs.
