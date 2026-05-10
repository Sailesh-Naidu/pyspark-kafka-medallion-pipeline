# PySpark Kafka End-to-End Streaming Pipeline

## Project Overview

This project is a production-style real-time streaming data pipeline built using:

- Apache Kafka
- PySpark Structured Streaming
- Delta Lake
- Medallion Architecture (Bronze → Silver → Gold)

The goal of this project is to simulate a real-world trading platform pipeline while learning advanced Data Engineering concepts such as:

- Event-time streaming
- Watermarking
- Stateful aggregations
- Checkpointing
- Delta Lake
- Windowed streaming analytics
- Schema evolution
- Data quality validation
- DLQ (Dead Letter Queue)

---

# Architecture

Kafka → Bronze → Silver → Gold

---

# Current Features Implemented

## Bronze Layer

Purpose:
- Raw ingestion layer
- Minimal transformations
- Stores raw validated streaming data from Kafka

Implemented:
- Kafka ingestion
- Delta streaming writes
- Checkpointing
- Raw trade ingestion
- Ingestion timestamp tracking

---

## Silver Layer

Purpose:
- Data cleaning and validation layer

Implemented:
- Schema validation
- Invalid record detection
- DLQ handling
- Watermarking
- Deduplication
- Event-time handling
- Stateful streaming concepts

Important concepts learned:
- Watermark vs checkpointing
- Stateful vs stateless transformations
- Deduplication with watermark
- Late-arriving data

---

## Gold Layer

Purpose:
- Business aggregations and analytics

Implemented:
- Window aggregations
- Trader-level metrics
- Stateful aggregations
- Delta streaming writes
- Window + watermark interaction

Metrics generated:
- Total traded quantity
- Trade count
- Average trade price
- Total trade value

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Kafka | Streaming ingestion |
| PySpark Structured Streaming | Stream processing |
| Delta Lake | ACID data lake |
| Docker | Local Kafka setup |
| Python | Pipeline development |

---

# Key Concepts Covered

## Streaming Concepts

- Micro-batches
- Event-time processing
- Watermarking
- Checkpointing
- Stateful processing
- Window aggregations

---

## Delta Lake Concepts

- ACID transactions
- Delta transaction logs
- Streaming reads/writes
- Checkpoint recovery

---

# Folder Structure

```text
project/
│
├── BronzeLayer/
├── silverLayer/
├── GoldLayer/
│
├── data/
├── checkpoints/
│
├── requirements.txt
└── README.md
```

---

# How to Run

## 1. Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

---

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 3. Run Bronze Layer

```bash
python BronzeLayer/BronzeLayer.py
```

---

## 4. Run Silver Layer

```bash
python silverLayer/SilverLayer.py
```

---

## 5. Run Gold Layer

```bash
python GoldLayer/GoldLayer.py
```

---

# Upcoming Features

The project will continue expanding with:

- Schema Registry
- Avro serialization
- Schema evolution
- SCD Type 1 & Type 2
- Delta MERGE INTO
- Spark optimizations
- Data skew handling
- Airflow orchestration
- Snowflake integration
- DBT transformations

---

# Notes

This project is intentionally designed as a progressive learning project to simulate real-world Data Engineering workflows and production streaming systems.
