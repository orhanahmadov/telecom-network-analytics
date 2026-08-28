# Telecom CDR & Network Data Analytics Platform

An end-to-end data engineering platform designed to ingest, process, and analyze Call Detail Records (CDRs) and data usage metrics to identify network quality issues and churn signals.

## Target Architecture

```mermaid
graph TD
    A[Synthetic CDR Generator] -->|Batch Ingestion| B[Python Ingestion Layer]
    B -->|Customer Metadata & Dim| C[(PostgreSQL)]
    B -->|Bulk CDR Ingestion| D[(ClickHouse OLAP)]
    E[Apache Airflow] -->|Orchestration| B
