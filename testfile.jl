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
