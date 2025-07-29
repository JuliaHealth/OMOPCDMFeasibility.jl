__precompile__(false)

module OMOPCDMFeasibility

using DataFrames
using OMOPCDMCohortCreator

include("utils.jl")
include("precohort.jl")

export scan_patients_with_concepts,
       analyze_concept_distribution,
       generate_feasibility_report
end