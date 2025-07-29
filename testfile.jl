using DataFrames, DuckDB, DBInterface
using FunSQL: Get
using OMOPCDMFeasibility
using OMOPCDMCohortCreator:
    GenerateDatabaseDetails, 
    GenerateTables, 
    GetPatientGender, 
    GetPatientAgeGroup

conn = DBInterface.connect(DuckDB.DB, "synthea_1M_3YR.duckdb")

GenerateDatabaseDetails(:postgresql, "dbt_synthea_dev")
GenerateTables(conn)

concept_ids = [31967, 4059650]

df1 = OMOPCDMFeasibility.scan_patients_with_concepts(
    conn; 
    domain=:condition_occurrence, 
    concept_set=concept_ids, 
    covariate_funcs=[GetPatientGender, GetPatientAgeGroup]
)

println("First 10 patients:")
display(first(df1, 10))

df2 = OMOPCDMFeasibility.analyze_concept_distribution(
    conn;
    domain=:condition_occurrence,
    concept_set=concept_ids,
    covariate_funcs=[GetPatientGender, GetPatientAgeGroup]
)

println("Summary by concept and covariates:")
display(df2)

DBInterface.close!(conn)
