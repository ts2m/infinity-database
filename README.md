# Infinity Database

A data processing pipeline framework for managing complex data workflows using an operator-style paradigm.

## Overview

Infinity Database provides a structured approach to data processing with modular operators for ingestion, transformation, clustering, schema consolidation, SQLite building, LLM augmentation, and quality checking.

## Features

- **Multiple Input Sources**: Supports ingestion from files (CSV, Excel, JSON, Parquet) and databases (SQLite, MySQL, etc.) into a unified Intermediate Representation (IR).
- **Operator Pipeline**: Modular and reorderable operators for data processing steps.
- **Incremental Clustering**: Supports incremental clustering with saved centroids for efficient processing.
- **LLM Augmentation**: Integrates with LLMs for data augmentation with iterative refinement based on quality checks.
- **Extensible Quality Checks**: Dynamic loading of rules for quality validation.
- **Artifact Tracking**: Each processing step produces trackable artifacts with caching potential.

## Installation

```bash
pip install -e .
```

## Usage

Define your pipeline in `configs/pipeline.yaml` and run:

```bash
python -m dataflow.cli run -c configs/pipeline.yaml
```

## Quick Start

1. **Setup**: Copy the provided files into the directory structure, then execute:
   ```bash
   pip install -e .
   ```
2. **Prepare Input Data**: Place your data in `./input/tables/**/*.csv|xlsx` (for scattered tables) or `./input/raw.db` (for databases).
3. **Configure**: Edit `configs/pipeline.yaml` to replace tokens/URLs for LLM/Embedding providers.
4. **Run**: Execute the pipeline with:
   ```bash
   python -m dataflow.cli run -c configs/pipeline.yaml
   ```
5. **Outputs**: Check the results in `workdir/` directory, including embeddings, cluster maps, consolidated databases, SQLite DBs, augmentation results, and quality reports.

## Directory Structure

```
infinity_database/
├─ pyproject.toml
├─ README.md
├─ configs/
│  └─ pipeline.yaml
└─ src/
   └─ dataflow/
      ├─ __init__.py
      ├─ cli.py
      ├─ core/
      │  ├─ artifact.py
      │  ├─ operator.py
      │  ├─ pipeline.py
      │  ├─ registry.py
      │  └─ config.py
      ├─ ir/
      │  └─ schema.py
      ├─ operators/
      │  ├─ ingest_files.py
      │  ├─ ingest_db.py
      │  ├─ deduplicate.py
      │  ├─ embed.py
      │  ├─ cluster.py
      │  ├─ consolidate_schema.py
      │  ├─ compile_ddl.py
      │  ├─ build_sqlite.py
      │  ├─ augment_llm.py
      │  └─ quality_check.py
      ├─ qc_rules/
      │  ├─ basic.py
      │  └─ semantic.py
      ├─ providers/
      │  ├─ embedding.py
      │  └─ llm.py
      └─ utils/
         ├─ sqlite_exec.py
         └─ logging.py
```

## Customization

To add custom operators (e.g., for normalization or semantic recognition), create a new operator class inheriting from `Operator`, define input and output kinds, and insert it into the pipeline configuration at the desired step.
