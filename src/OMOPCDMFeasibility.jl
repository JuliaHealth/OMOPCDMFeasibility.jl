__precompile__(false)

module OMOPCDMFeasibility

using DataFrames
using OMOPCDMCohortCreator
using DBInterface
using FunSQL:
    FunSQL, Agg, Fun, From, Get, Group, Join, LeftJoin, Select, Where, SQLConnection, reflect
using OMOPCommonDataModel
using Serialization


include("utils.jl")
include("precohort.jl")

export analyze_concept_distribution,
       generate_feasibility_report
end