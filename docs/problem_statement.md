`docs/problem_statement.md` 
 
# Problem Statement

## The Problem

Data from real sensors is rarely clean.
It is often affected by things like:

* random noise
* slow calibration drift
* missing packets
* long gaps in data
* uneven timestamps
* out-of-order messages

When this kind of data is sent directly into a model, the model starts reacting to these problems instead of the real physical signal. This leads to unstable and inaccurate predictions.

In systems like particle detectors, trading feeds, or spacecraft sensors, these issues happen all the time and cannot be fixed later using offline processing.

---

## Goal

The goal of this project is to build a real-time streaming pipeline for multiple sensors that can:

* detect when data is corrupted
* keep a regular time step
* fill in missing values when possible
* put all sensors on a common scale
* and produce a cleaned data stream for modeling

The system should also show, using numbers and plots, that this cleaning actually improves the accuracy of a downstream model.

---

## Constraints

The system must work under real-world conditions:

* It must run continuously
* Sensors may fail or behave differently
* Data must be ordered using sensor timestamps, not arrival time
* Data cannot be fixed later using offline scripts
* Detection of bad data must be separate from correction
* The system must stay observable even when things go wrong

---

## How Success Is Measured

The system is considered successful if:

1. The prediction error of a model using cleaned data is lower than when using raw data, for most of the time and across sensors.

2. The pipeline keeps producing output even when there is:

   * packet loss
   * burst gaps
   * or sensor recovery

3. All problems (loss, violations, degraded data) can be seen in the monitoring dashboards.

The aim is not just to make the data look nicer, but to make the model work better under messy, real-world conditions.
