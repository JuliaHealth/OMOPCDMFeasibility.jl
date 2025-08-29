using OMOPCDMFeasibility
using Test
using HealthSampleData: Eunomia
using SQLite: DB
using DataFrames
using DBInterface
import OMOPCDMCohortCreator as occ
using DataDeps

function setup_test_connection()
    # Set up automatic acceptance of data download
    ENV["DATADEPS_ALWAYS_ACCEPT"] = "true"

    eunomia = Eunomia()
    conn = DB(eunomia)

    occ.GenerateDatabaseDetails(:sqlite, "main")

    # Force compilation and avoid world age issues
    Base.invokelatest() do
        occ.GenerateTables(conn)
    end

    return conn
end

function teardown_test_connection(conn)
    DBInterface.close!(conn)
end

const TEST_CONN = setup_test_connection()

try
    @testset "Utility Functions" begin
        include("test_utils.jl")
    end

    @testset "Pre-cohort Analysis" begin
        include("test_precohort.jl")
    end

    @testset "Post-cohort Analysis" begin
        include("test_postcohort.jl")
    end
finally
    teardown_test_connection(TEST_CONN)
end
