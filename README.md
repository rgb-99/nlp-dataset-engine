# 🚀 NLP Dataset Engine

![CI Status](https://github.com/rgb-99/nlp-dataset-engine/actions/workflows/ci.yml/badge.svg)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A high-performance, memory-efficient streaming engine for processing massive NLP datasets. Designed for GSoC 2026 preparation.

## ⚡ Features
- **Lazy Loading:** Processes datasets larger than RAM using Python generators.
- **Auto-Validation:** Automatically drops empty or short text records.
- **Observability:** Provides detailed statistical reports on data quality.
- **CLI Interface:** simple command-line usage.

## 🛠️ Installation

```bash
# Clone the repo
git clone [https://github.com/rgb-99/nlp-dataset-engine.git](https://github.com/rgb-99/nlp-dataset-engine.git)

# Install in editable mode
cd nlp-dataset-engine
pip install -e .[dev]
```

## 🚀 Usage

### 1. Ingest Data (CSV → JSONL)
Convert massive raw CSV files into clean, validated JSONL datasets for AI training. This runs in O(1) memory.

```bash
# Basic usage
nlp-engine ingest --input raw_data.csv --output clean_dataset.jsonl --col text
```
### 2. Automatic Observability & Filtering
The engine automatically enforces data quality:
* **Language Detection:** Removes non-English text by default.
* **Health Report:** Prints a statistical summary (drop rate, speed, count) after every run.

**Disable English Filtering:**
If you want to keep all languages (e.g., for a multilingual dataset), use the `--no-english` flag:

```bash
nlp-engine ingest --input global_data.csv --output clean.jsonl --no-english
