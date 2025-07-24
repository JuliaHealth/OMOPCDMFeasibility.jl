module OMOPCDMFeasibility

using FunSQL
using OMOPCommonDataModel   
using DataFrames
using Tables

include("conceptsets.jl")
include("precohort.jl")

export scan_domain_presence,
       summarize_covariate_availability

end
