# Longitudinal Prescription Data Pipeline for Drug Overdose Prevention Research

This repository contains data engineering scripts developed to process large-scale longitudinal prescription data for research focused on drug overdose prevention in the United States. The project follows a **Medallion architecture** (Bronze → Silver → Gold layers) to ensure clean, reliable, and analysis-ready data across multiple drug markets and years.

## 🎯 Project Objectives

- Ingest and clean raw longitudinal prescription and lookup files.
- Standardize data types, ensure data quality, and integrate relevant reference files.
- Segment and store processed datasets by **drug market** and **calendar year**.
- Prepare and transform opioid and non-opioid prescription data for downstream research on medication trends, patient behavior, and overdose risk.

---

## 🧱 Architecture: Medallion Pattern

| **Layer** | **Description** |
|-----------|-----------------|
| **Bronze** | Raw ingested data, minimally processed |
| **Silver** | Cleaned and validated data with applied business logic |
| **Gold** | Aggregated and research-ready data by drug market and year |

---

## 📁 Repository Structure

### `1_patient_lookup_processing.py`
- Processes the **patient demographic file**.
- Ensures consistent data types (e.g., age, sex, region).
- Handles missing values and standardizes formats for integration.

### `2_product_lookup_processing.py`
- Cleans and standardizes the **product lookup file**.
- Ensures correct drug classifications, NDC mappings, and dosage form normalization.
- Supports merging with prescription records.

### `3_prescription_general_processing.py`
- Ingests raw **prescription claims** data.
- Applies format standardization, corrects column types, and checks for duplicates.
- Integrates with patient and product lookup files.
- Implements core **data quality checks** (e.g., null checks, date validations).

### `4_prescription_non_opioid_processing.py`
- Filters and processes **non-opioid prescriptions**.
- Applies standard transformations and stores segmented data by year and market.

### `5_prescription_opioid_transformations.py`
- Applies advanced transformations for **opioid prescriptions**.
- Implements logic for flagging high-risk prescriptions, dosage normalization, and temporal sorting.
- Finalizes opioid datasets for downstream analysis and reporting.




