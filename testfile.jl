using DataFrames, DuckDB, DBInterface
using FunSQL: Get
using OMOPCDMFeasibility
using OMOPCDMCohortCreator:
    GenerateDatabaseDetails, 
    GenerateTables, 
    GetPatientGender, 
    GetPatientAgeGroup,
    GetPatientRace,
    GetPatientEthnicity

conn = DBInterface.connect(DuckDB.DB, "synthea_1M_3YR.duckdb")

GenerateDatabaseDetails(:postgresql, "dbt_synthea_dev")
GenerateTables(conn)

concept_ids = [31967, 4059650]

println("\nFeasibility Report:")
report = OMOPCDMFeasibility.generate_feasibility_report(
    conn;
    domain=:condition_occurrence,
    concept_set=concept_ids,
    covariate_funcs=[GetPatientGender, GetPatientRace]
)
display(report)

println("\nPatient Scan (First 10):")
scan = OMOPCDMFeasibility.scan_patients_with_concepts(
    conn;
    domain=:condition_occurrence,
    concept_set=concept_ids,
    covariate_funcs=[GetPatientGender, GetPatientRace]
)
display(first(scan, 10))

println("\nConcept Distribution Summary:")
summary = OMOPCDMFeasibility.analyze_concept_distribution(
    conn;
    domain=:condition_occurrence,
    concept_set=concept_ids,
    covariate_funcs=[GetPatientAgeGroup, GetPatientRace]
)
display(summary)
println()

DBInterface.close!(conn)
