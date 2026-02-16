`docs/problem.md` 
 
 # Problem Statement

## The Problem

Data from real sensors is rarely clean.  
It is often affected by:

- random noise  
- slow calibration drift  
- missing packets  
- long gaps in data  
- uneven timestamps  
- out-of-order messages  

When this kind of data is sent directly into a model, the model reacts to these issues instead of the real physical signal. This leads to unstable and inaccurate predictions.

In systems such as particle detectors, trading feeds, or spacecraft sensors, these problems are normal operating conditions. They cannot be corrected later using offline processing.

---

## Goal

The goal of this project is to build a real-time streaming pipeline for multiple sensors that can:
- detect corrupted data  
- maintain a regular time step  
- fill in missing values when possible  
- normalize sensors onto a common scale  
- produce a cleaned stream suitable for modeling  

* The system must also demonstrate, using quantitative metrics and plots, whether preprocessing improves downstream model accuracy.

---

## Constraints

The system must operate under real-world conditions:
- It must run continuously  
- Sensors may fail or degrade  
- Ordering must use sensor timestamps, not arrival time  
- Data cannot be repaired offline  
- Detection must be separate from correction  
- The system must remain observable during failure  

---

## Success Criteria

The system is considered successful if:
- Model prediction error on cleaned data is lower than on raw data for most time periods and across sensors.
- The pipeline continues producing output during: packet loss, burst gaps, sensor recovery  
- All failures and degradations are visible in monitoring dashboards.

* The objective is not data improvement, but improved model performance under realistic conditions.
