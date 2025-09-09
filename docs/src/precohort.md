# Pre-Cohort Analysis

**What is Feasibility Analysis?**

Feasibility analysis is the process of checking if your planned study or cohort is possible and meaningful with the data you have. It helps you answer questions like: Are there enough patients? Are the concepts I care about present? Is the data complete and reliable?

**What is Pre-Cohort Analysis?**

Pre-cohort analysis is the first step in any observational health study. Before you define your study population (the "cohort"), you use pre-cohort tools to explore your OMOP CDM database. This helps you:

- Understand what data is available
- Check the frequency and quality of key concepts
- Plan your study with confidence

Pre-cohort analysis is like scouting the terrain before starting a journey—it helps you avoid surprises and design better, more robust studies.

## 1. `analyze_concept_distribution`

```@docs
OMOPCDMFeasibility.analyze_concept_distribution
```

## 2. `generate_summary`

```@docs
OMOPCDMFeasibility.generate_summary
```

## 3. `generate_domain_breakdown`

```@docs
OMOPCDMFeasibility.generate_domain_breakdown
```

## Example: Pre-Cohort Analysis in Practice

```julia
using DataFrames, DuckDB, DBInterface
using OMOPCDMFeasibility
using OMOPCDMCohortCreator:
    GenerateDatabaseDetails,
    GenerateTables,
    GetPatientGender,
    GetPatientAgeGroup,
    GetPatientRace,
    GetPatientEthnicity,
    ConditionFilterPersonIDs

conn = DBInterface.connect(DuckDB.DB, "synthea_1M_3YR.duckdb")

GenerateDatabaseDetails(:postgresql, "dbt_synthea_dev")
GenerateTables(conn)

concept_ids = [
    31967,    # Condition: Nausea
    1127433,  # Drug: Acetaminophen
]
println("\n")
report = OMOPCDMFeasibility.generate_feasibility_report(
    conn;
    concept_set=concept_ids,
    covariate_funcs=[GetPatientGender, GetPatientRace]
)
display(report)

println("\n")
summary = OMOPCDMFeasibility.analyze_concept_distribution(
    conn;
    concept_set=concept_ids,
    covariate_funcs=[GetPatientAgeGroup, GetPatientRace]
)
display(summary)
println()

DBInterface.close!(conn)
```
