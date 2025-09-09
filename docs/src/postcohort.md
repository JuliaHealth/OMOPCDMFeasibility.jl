# Post-Cohort Analysis

**What is Post-Cohort Analysis?**

Post-cohort analysis is the process of exploring and summarizing your study population after you have defined your cohort. It helps you answer questions like: Who is in my cohort? What are their characteristics? How do they compare to the rest of the database?

This step is essential for understanding your results, checking for biases, and making your study reproducible and transparent.

Post-cohort analysis in OMOPCDMFeasibility.jl is designed to be simple and clear, even for beginners.

## 1. `create_individual_profiles`

```@docs
OMOPCDMFeasibility.create_individual_profiles
```

## 2. `create_cartesian_profiles`

```@docs
OMOPCDMFeasibility.create_cartesian_profiles
```

## Example: Post-Cohort Analysis in Practice

```julia
using DataFrames, DuckDB, DBInterface, Dates
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

diabetes_concept_ids = [201826]
cohort_result = ConditionFilterPersonIDs(diabetes_concept_ids, conn)
cohort_ids = cohort_result.person_id

sample_cohort = DataFrame(
    person_id = cohort_ids
)

println("Creating individual demographic profiles...")
individual_demographics = OMOPCDMFeasibility.create_individual_profiles(
    cohort_df=sample_cohort,
    conn=conn,
    covariate_funcs=[GetPatientGender, GetPatientRace, GetPatientAgeGroup]
)

println("Individual profiles:")
for (name, table) in pairs(individual_demographics)
    println("$name:")
    println(table)
    println()
end

println("Creating Cartesian demographic profiles...")
cartesian_demographics = OMOPCDMFeasibility.create_cartesian_profiles(
    cohort_df=sample_cohort,
    conn=conn,
    covariate_funcs=[GetPatientAgeGroup, GetPatientGender, GetPatientRace]
)

println("Cartesian profiles:")
println(cartesian_demographics)

DBInterface.close!(conn)
```
