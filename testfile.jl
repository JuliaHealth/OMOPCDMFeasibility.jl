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

individual_demographics = OMOPCDMFeasibility.create_individual_profiles(
    cohort_df=sample_cohort,
    conn=conn,
    covariate_funcs=[GetPatientGender, GetPatientRace, GetPatientAgeGroup]
)

println("Available individual tables: ", keys(individual_demographics))
for (name, table) in pairs(individual_demographics)
    println("\n$name breakdown:")
    println(table)
end

cartesian_demographics_by_df = OMOPCDMFeasibility.create_cartesian_profiles(
    cohort_df=sample_cohort,
    conn=conn,
    covariate_funcs=[GetPatientGender, GetPatientRace, GetPatientAgeGroup]
)

println("Available combination tables (by DataFrame): ", keys(cartesian_demographics_by_df))
for (name, table) in pairs(cartesian_demographics_by_df)
    println("\n$name combinations (from cohort_df):")
    println(first(table, 5))
end

DBInterface.close!(conn)