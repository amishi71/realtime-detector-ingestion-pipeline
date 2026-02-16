`docs/discussion.md`

# Discussion

## Interpretation

Cleaning and aligning data affects prediction error.

Raw models must handle:

- Missing packets  
- Irregular timestamps  
- Scale differences  
- Drift  

Preprocessed models receive:

- Fixed cadence  
- Normalized values  
- Quality labels  

Results show improvement in some sensors but degradation in others.

This suggests preprocessing must be tuned per sensor.

---

## Why It Matters

Many real-world system failures are data-quality failures.

Stable control and inference require:

- Detection  
- Correction  
- Monitoring  

Preprocessing is part of the inference system itself.

---

## Limitations

- Simple moving-average model  
- Simulated data  
- No feedback loop  
- Global preprocessing parameters  
- No statistical significance testing  

---

## Future Work

- Per-sensor preprocessing configuration  
- Alternative imputation strategies  
- Kalman filters or ARIMA models  
- Neural time-series models  
- Real telemetry datasets  
- Statistical testing  
- Ablation analysis  
- Long-duration stability tests  

---

## Conclusion

The pipeline demonstrates that preprocessing measurably affects prediction accuracy.

The system architecture works.

The open problem is adaptive, per-sensor preprocessing.
