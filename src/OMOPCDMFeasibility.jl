__precompile__(false)

module OMOPCDMFeasibility

using DataFrames
using OMOPCDMCohortCreator

include("utils.jl")
include("postcohort.jl")

export profile_cohort_demographics
end