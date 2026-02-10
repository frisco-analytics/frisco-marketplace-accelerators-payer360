# Payer360 Marketplace Accelerator - Duplicate Detection

This repository contains tools and notebooks for detecting and deduplicating duplicate records in the Payer360 marketplace dataset.

## Overview

The Payer360 Marketplace Accelerator project provides a comprehensive workflow for identifying potential duplicate records and intelligently deduplicating them based on configurable thresholds. This is particularly useful for data quality improvement and consolidation of payer information.

## Project Structure

The project is organized as a sequential workflow with the following components:

### Setup
- **0_setup.ipynb** - Initial setup notebook that prepares the environment and loads necessary dependencies

### Detection & Analysis
- **1_detect_duplicates.ipynb** - Identifies potential duplicate records using similarity matching algorithms
- **2_analyze_duplicate_candidates.py** - Analyzes the detected duplicate candidates with detailed metrics and scoring

### Deduplication
- **3_deduplicate_by_threshold.py** - Applies deduplication rules based on configurable similarity thresholds

### Configuration
- **config/** - Directory containing configuration files for the deduplication pipeline

## Getting Started

### Prerequisites
- Python 3.7+
- Jupyter Notebook or JupyterLab
- Required Python packages (see setup notebook)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/frisco-analytics/frisco-marketplace-accelerators-payer360.git
cd frisco-marketplace-accelerators-payer360
```

2. Install dependencies by running the setup notebook:
```bash
jupyter notebook 0_setup.ipynb
```

## Usage

Follow the workflow in order:

1. **Setup**: Run `0_setup.ipynb` to configure your environment
2. **Detection**: Run `1_detect_duplicates.ipynb` to identify potential duplicates
3. **Analysis**: Execute `2_analyze_duplicate_candidates.py` to analyze the results
4. **Deduplication**: Run `3_deduplicate_by_threshold.py` to apply deduplication

## Configuration

Configuration settings can be found and modified in the `config/` directory. Adjust thresholds, matching criteria, and other parameters as needed for your specific use case.

## Features

- Intelligent duplicate detection using multiple similarity metrics
- Detailed analysis and scoring of duplicate candidates
- Configurable similarity thresholds for deduplication
- Scalable processing for large datasets

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions or issues, please contact the Frisco Analytics team.