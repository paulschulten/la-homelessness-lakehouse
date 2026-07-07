# LA Homelessness Lakehouse

**Status:** Early development. Bronze/silver/gold pipeline validated end-to-end on one data source (LA City homelessness expense data); additional sources in progress.

I'm building a lakehouse integrating LA homelessness data across a multitude of domains that bear on the crisis — including timely LAHSA, LA City, 311 encampment reports, LAPD crime data, Zillow Apartment List Rent Index, LA County rent burden indicators, eviction filings, NOAA daily weather observations, LA County GIS tract boundaries, US Census data, and LA Fire Department incident data.

The platform follows a medallion (bronze/silver/gold) data architecture built on DuckDB, Parquet, Python, and Dagster, with the data model architected for Apache Iceberg, Apache Spark, and Tabular ML as the platform scales — enabling predictive modeling across all integrated domains at the Census tract level. For example, the model should be able to predict the likelihood of a 4–7 tent encampment forming within a given tract over the next 3–6 months. Encampment formation is just one of many features of the crisis the platform is designed to model.

## Tech Stack

**Currently implemented:**
- **DuckDB** — SQL-based bronze/silver transforms
- **Python** — ingestion and orchestration logic
- **Dagster** — pipeline orchestration
- **Parquet** — current data file format

**Architected for, not yet implemented:**
- **Apache Iceberg** — open table format for the lakehouse layer (transactions, schema evolution, time travel)
- **Apache Spark** — heavier distributed transforms as data volume grows
- **Tabular ML** — predictive modeling layer (e.g. encampment formation risk)

## Roadmap

This project follows a milestone-driven, versioned release approach, with transparent disclaimers about what each version does and doesn't cover.

- **v0.1 (current)** — Bronze/silver/gold pipeline validated end-to-end on one data source
- **v0.x** — Onboard additional data sources across domains (LAHSA, 311, LAPD, eviction filings, Census/ACS, etc.)
- **v1.0** — Multi-source Gold layer with reliable cross-domain joins at the Census tract level; migration to Apache Iceberg
- **Post-v1.0** — Apache Spark integration for heavier transforms; Tabular ML layer for predictive modeling (starting with encampment formation risk)

## Data Sources

LAHSA · LA City homelessness expenditures · 311 encampment reports · LAPD crime data · Zillow Apartment List Rent Index · LA County rent burden indicators · eviction filings · NOAA daily weather observations · LA County GIS tract boundaries · US Census data · LA Fire Department incident data