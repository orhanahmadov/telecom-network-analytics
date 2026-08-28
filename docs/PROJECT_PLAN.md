# Project Plan: Telecom CDR & Network Data Analytics Platform

## 1. Problem Statement
- **Domain:** Telecommunications & Network Analytics.
- **Business Question:** Which customer segments are experiencing network quality degradation and usage anomalies, driving potential churn?
- **Consumer:** Telecom Operations & Customer Retention Team.

## 2. Data Sources
- **Source:** Synthetic Custom CDR & Data Usage Generator.
- **Volume:** ~5,000,000 events/day.
- **Quality Issues:** Simulated duplicate events, late-arriving CDRs, missing cell-tower IDs.

## 3. Target Architecture
```mermaid
graph TD
    A[Synthetic CDR Generator] -->|Batch/Stream JSON| B[Python Ingestion Layer]
    B -->|Metadata & Customer Dim| C[(PostgreSQL OLTP)]
    B -->|Bulk CDR Ingestion| D[(ClickHouse OLAP)]
    E[Apache Airflow] -->|Orchestrates| B
    E -->|Triggers Data Quality| F[Great Expectations / Custom Validation]
