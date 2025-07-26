using DataFrames
using DuckDB
include("src/conceptsets.jl")
include("src/precohort.jl")

db_path = "synthea_1M_3YR.duckdb"
conn = DBInterface.connect(DuckDB.DB, db_path)

lookup_concept(conn; concept_id=31967)
println("\n")

summarize_domain_availability(conn; domain=:condition_occurrence, top_n=10)
println("\n")

concept_ids = [31967] # Nausea concept ID
# scan_domain_presence(conn; domain=:condition_occurrence, concept_set=concept_ids, limit=10)
println("\n")


DBInterface.close!(conn)
