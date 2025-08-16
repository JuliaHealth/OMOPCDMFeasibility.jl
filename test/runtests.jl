using Test
using InlineStrings
using FeatureTransforms
using Serialization
using DataFrames
using OMOPCommonDataModel
using Dates
using DBInterface
using DuckDB
using Tables
using HealthBase
using OMOPCDMFeasibility
using OMOPCDMCohortCreator:
    GenerateDatabaseDetails, 
    GenerateTables, 
    GetPatientGender, 
    GetPatientRace,
    GetPatientAgeGroup

@testset "OMOPCDMFeasibility.jl" begin
    @testset "Post-Cohort Analysis" begin
        include("postcohort.jl")
    end
end
