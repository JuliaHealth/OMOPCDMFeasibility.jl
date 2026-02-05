# OMOPCDMFeasibility.jl

> A Julia package for feasibility and cohort analysis on OMOP Common Data Model (CDM) data.

## Overview

OMOPCDMFeasibility.jl helps researchers and data scientists quickly explore, summarize, and compare patient cohorts using OMOP CDM databases. It is designed for use in observational health studies, cohort discovery, and data quality assessment.

## Features

- **Pre-cohort analysis:** Explore concept distributions, domain breakdowns, and data quality before defining a cohort.
- **Post-cohort analysis:** Summarize, profile, and compare cohorts after extraction.
- **Flexible database support:** Works with DuckDB, SQLite, PostgreSQL, and more.
- **Composable with JuliaHealth:** Integrates with DataFrames.jl, OMOPCommonDataModel.jl, and other JuliaHealth tools.
- **Reproducible workflows:** Designed for robust, testable, and transparent research.
- **Clear error handling:** Provides informative messages and input validation.

## Limitations

- OMOPCDMFeasibility.jl is focused on feasibility and cohort analysis only; it does not perform cohort extraction or patient-level prediction itself.
- Some advanced features (e.g., custom covariates, non-standard dialects) may require additional JuliaHealth packages or extensions.
- The package assumes your data is already in OMOP CDM format and accessible via a supported database backend.

For a step-by-step guide, see the [Quickstart](quickstart.md). For detailed workflows and function documentation, explore the other sections in this documentation.
