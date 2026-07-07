# LA Homelessness Lakehouse

**Status:** Early development. Core medallion pipeline implemented and fully orchestrated in Dagster. Additional data domains are being integrated.

I'm building an LA homelessness lakehouse integrating data across every relevant domain - including LAHSA, LA City homelessness expenditures, 311 encampment reports, LAPD crime data, Zillow Apartment List Rent Index, LA County rent burden indicators, eviction filings, NOAA daily weather observations, LA County GIS tract boundaries, US Census data, and LA Fire Department incident data.

The platform utilizes a medallion data architecture built on DuckDB, Parquet, Python, and Dagster, with a data model designed to scale into Apache Iceberg, Apache Spark, and Tabular ML. The goal is to enable predictive modeling across integrated domains at the Census tract level. For example, predicting the likelihood of a 4–7 tent encampment forming within a given tract over the next 3–6 months. Encampment formation is just one of many features of the crisis the platform is designed to model.

## Tech Stack

**Currently implemented:**
- **DuckDB** — SQL-based bronze/silver/gold transforms
- **Python** — ingestion and orchestration logic
- **Dagster** — pipeline orchestration
- **Parquet** — current data file format

**Architected for, not yet implemented:**
- **Apache Iceberg** — lakehouse table format as the platform scales across multiple domains
- **Apache Spark** — distributed processing once the data becomes too large for DuckDB + Python to handle
- **Tabular ML** — machine‑learned pattern discovery as the foundation for robust tract‑level analytics and predictive modeling

## Roadmap

This project follows a milestone-driven, versioned release approach, with transparent disclaimers about what each version does and does not cover.

- **v0.x (current)** — bronze/silver/gold pipeline validated end-to-end across eight initial domains and fully orchestrated in Dagster. 
- **v1.0** — initial release of multi-domain gold layer with reliable cross-domain joins at the census tract level enabling tabular ml learning.   
- **Post-v1.0** — Additional data domains onboarded. Integration of Apache Iceberg and Apache Spark for more robust processing. 

