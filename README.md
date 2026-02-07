# Distributed Spatio-Temporal Stream Processing Platform

## Overview
This repository contains an experimental research platform for studying
models, architectures and methods of distributed stream processing of
spatio-temporal data.

## Research Goals
- Event-time stream processing
- Stateful computations
- Spatial operators in real-time
- Latency and throughput analysis

## Technologies
- Stream transport layer
- Stateful stream processing engine
- Spatial-temporal storage
- Analytical and visualization components

## Repository Structure
```text
spatio_temporal_stream_processing/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── docs/                     # Научная документация
│   ├── concept/
│   ├── architecture/
│   ├── models/
│   ├── experiments/
│   └── figures/
│
├── docker/                   # Воспроизводимая инфраструктура
│   ├── docker-compose.yml
│   ├── kafka/
│   ├── flink/
│   ├── postgis/
│   ├── monitoring/
│   └── visualization/
│
├── stream-processing/        # Потоковые вычисления (Scala)
│   ├── flink-jobs/
│   │   ├── core/
│   │   ├── spatial/
│   │   ├── trajectories/
│   │   └── experiments/
│   └── build.sbt
│
├── analytics/                # Аналитика и алгоритмы (Python)
│   ├── notebooks/
│   ├── spatial/
│   ├── graphs/
│   └── ml/
│
├── sql/                      # SQL = формализация логики
│   ├── schema/
│   ├── materialized_views/
│   └── experiments/
│
├── data/                     # Данные (НЕ всё коммитить!)
│   ├── schemas/
│   ├── generators/
│   └── samples/
│
├── experiments/              # Экспериментальные сценарии
│   ├── latency/
│   ├── throughput/
│   ├── fault-tolerance/
│   └── spatial-skew/
│
└── papers/                   # Статьи и диссертация
    ├── article-architecture/
    ├── article-stream-models/
    ├── article-spatiotemporal/
    ├── article-graphs/
    ├── article-experiments/
    └── phd-dissertation/
```

## Reproducibility
All experiments can be reproduced using Docker Compose.
