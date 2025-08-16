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