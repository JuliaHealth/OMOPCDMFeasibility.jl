__precompile__(false)

module OMOPCDMFeasibility

using FunSQL
using OMOPCommonDataModel   
using DataFrames
using Tables
using Dates
using OMOPCDMCohortCreator

include("utils.jl")
include("precohort.jl")
include("reports.jl")

export scan_patients_with_concepts,
       analyze_concept_distribution
end