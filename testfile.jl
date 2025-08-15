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
    4044394   # Procedure: Some procedure
]
println("\nPre Cohort")
println("\nFeasibility Report:")
report = OMOPCDMFeasibility.generate_feasibility_report(
    conn;
    concept_set=concept_ids,
    covariate_funcs=[GetPatientGender, GetPatientRace]
)
display(report)

println("\nConcept Distribution Summary:")
summary = OMOPCDMFeasibility.analyze_concept_distribution(
    conn;
    concept_set=concept_ids,
    covariate_funcs=[GetPatientAgeGroup, GetPatientRace]
)
display(summary)
println()

DBInterface.close!(conn)
