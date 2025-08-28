module OMOPCDMFeasibility

using DataFrames
using DBInterface
using FunSQL:
    FunSQL,
    Agg,
    Fun,
    From,
    Get,
    Group,
    Join,
    LeftJoin,
    Select,
    Where,
    SQLConnection,
    reflect
using OMOPCommonDataModel

include("utils.jl")
include("precohort.jl")
include("postcohort.jl")

export analyze_concept_distribution, generate_summary, generate_domain_breakdown, create_individual_profiles, create_cartesian_profiles

end