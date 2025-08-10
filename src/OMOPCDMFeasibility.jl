__precompile__(false)

module OMOPCDMFeasibility

using DataFrames
using OMOPCDMCohortCreator

include("utils.jl")
include("postcohort.jl")

export create_individual_profiles,
       create_cartesian_profiles

end