__precompile__(false)

module OMOPCDMFeasibility

using DataFrames
using DBInterface
using FunSQL: SQLConnection, reflect, FunSQL, From, Get, Select, Where, Fun
using OMOPCDMCohortCreator

include("utils.jl")
include("postcohort.jl")

export create_individual_profiles,
       create_cartesian_profiles

end