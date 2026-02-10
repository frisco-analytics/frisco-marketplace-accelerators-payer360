# Frisco Marketplace Accelerators - Payer360

A comprehensive marketplace accelerator solution for Payer360 data management and analytics, providing tools for duplicate detection, data deduplication, and intelligent record consolidation.

## Overview

The **frisco-marketplace-accelerators-payer360** project is part of the Frisco Analytics marketplace accelerators suite. It delivers an enterprise-grade workflow for identifying, analyzing, and intelligently deduplicating duplicate records in Payer360 datasets. This solution is designed to improve data quality, enhance operational efficiency, and enable better decision-making through consolidated payer information.

### Key Capabilities

- **Intelligent Duplicate Detection** - Advanced similarity matching algorithms identify potential duplicate records across large datasets
- **Detailed Analysis & Scoring** - Comprehensive metrics and scoring systems for evaluating duplicate candidates
- **Configurable Deduplication** - Flexible similarity thresholds and matching criteria tailored to your business needs
- **Scalable Processing** - Optimized for handling large-scale payer datasets efficiently
- **Production-Ready** - Enterprise-grade implementation with MIT License

## Project Structure

The project follows a sequential, modular workflow architecture:

```
frisco-marketplace-accelerators-payer360/
├── 0_setup.ipynb                          # Environment setup and dependencies
├── 1_detect_duplicates.ipynb              # Duplicate detection engine
├── 2_analyze_duplicate_candidates.py      # Candidate analysis and scoring
├── 3_deduplicate_by_threshold.py          # Deduplication execution
├── config/                                # Configuration files
├── LICENSE                                # MIT License
└── README.md                              # This file
```

### Workflow Components

#### 1. Setup Phase
- **0_setup.ipynb** - Initializes the environment, loads dependencies, and prepares the data pipeline

#### 2. Detection & Analysis Phase
- **1_detect_duplicates.ipynb** - Employs similarity matching algorithms to identify potential duplicate records
- **2_analyze_duplicate_candidates.py** - Evaluates candidates with detailed metrics, scoring, and statistical analysis

#### 3. Deduplication Phase
- **3_deduplicate_by_threshold.py** - Applies intelligent deduplication rules based on configurable similarity thresholds

#### 4. Configuration
- **config/** - Houses configuration files for customizing the deduplication pipeline behavior

## Getting Started

### Prerequisites

- **Python** 3.7 or higher
- **Jupyter Notebook** or **JupyterLab** environment
- Standard Python data science stack (NumPy, Pandas, etc.)
- Additional dependencies specified in setup notebook

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/frisco-analytics/frisco-marketplace-accelerators-payer360.git
   cd frisco-marketplace-accelerators-payer360
   ```

2. **Install dependencies:**
   ```bash
   jupyter notebook 0_setup.ipynb
   ```

3. **Configure settings** (optional):
   - Review and modify `config/` files as needed for your specific requirements

## Usage Guide

### Step-by-Step Workflow

Execute the pipeline in the following order:

1. **Initialize Environment**
   ```bash
   jupyter notebook 0_setup.ipynb
   ```
   - Sets up Python environment
   - Loads required libraries and modules
   - Prepares data sources

2. **Detect Duplicates**
   ```bash
   jupyter notebook 1_detect_duplicates.ipynb
   ```
   - Analyzes dataset for potential duplicates
   - Applies similarity matching algorithms
   - Generates duplicate candidate pairs

3. **Analyze Candidates**
   ```bash
   python 2_analyze_duplicate_candidates.py
   ```
   - Evaluates duplicate candidates
   - Calculates detailed matching scores
   - Provides statistical insights

4. **Execute Deduplication**
   ```bash
   python 3_deduplicate_by_threshold.py
   ```
   - Applies deduplication rules
   - Consolidates identified duplicates
   - Produces deduplicated output dataset

## Configuration

Configuration files in the `config/` directory enable fine-tuning of the deduplication pipeline:

- **Similarity Thresholds** - Adjust matching confidence levels
- **Matching Criteria** - Define which fields are compared
- **Processing Parameters** - Control batch sizes, algorithms, and output options
- **Business Rules** - Implement domain-specific deduplication logic

Refer to configuration documentation for detailed parameter descriptions and recommended values.

## Features

✅ Intelligent duplicate detection with multiple similarity metrics  
✅ Comprehensive analysis and candidate scoring  
✅ Configurable similarity thresholds for flexible deduplication  
✅ Production-ready scalable processing  
✅ Detailed reporting and insights  
✅ Enterprise-grade data quality management  

## Support & Contribution

For questions, issues, or contributions, please engage with the **@frisco-analytics** team.

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for complete details.

---

**Built by:** [@frisco-analytics](https://github.com/frisco-analytics)  
**Last Updated:** 2026-02-10 14:19:50