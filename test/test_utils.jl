@testset "_get_concept_name" begin
    result = OMOPCDMFeasibility._get_concept_name(8507, TEST_CONN; schema="main", dialect=:sqlite)
    @test result isa String
    @test result != "Unknown"

    result = OMOPCDMFeasibility._get_concept_name(999999999, TEST_CONN; schema="main", dialect=:sqlite)
    @test result == "Unknown"
end

@testset "_get_concepts_by_domain" begin
    concept_ids = [8507, 8532]
    result = OMOPCDMFeasibility._get_concepts_by_domain(
        concept_ids, TEST_CONN; schema="main", dialect=:sqlite
    )
    @test result isa Dict
    @test !isempty(result)

    # Test with empty vector
    result_empty = OMOPCDMFeasibility._get_concepts_by_domain(
        Int[], TEST_CONN; schema="main", dialect=:sqlite
    )
    @test result_empty isa Dict
    @test isempty(result_empty)

    # Test with invalid concept IDs
    result_invalid = OMOPCDMFeasibility._get_concepts_by_domain(
        [999999999], TEST_CONN; schema="main", dialect=:sqlite
    )
    @test result_invalid isa Dict
    @test isempty(result_invalid)
end

@testset "_domain_id_to_table" begin
    @test OMOPCDMFeasibility._domain_id_to_table("Condition") == :condition_occurrence
    @test OMOPCDMFeasibility._domain_id_to_table("Drug") == :drug_exposure
    @test OMOPCDMFeasibility._domain_id_to_table("Procedure") == :procedure_occurrence
    @test OMOPCDMFeasibility._domain_id_to_table("Measurement") == :measurement
    @test OMOPCDMFeasibility._domain_id_to_table("Observation") == :observation

    result = OMOPCDMFeasibility._domain_id_to_table("UnknownDomain")
    @test result isa Symbol
    @test String(result) == "unknowndomain_occurrence"
end

@testset "_format_number" begin
    @test OMOPCDMFeasibility._format_number(1500000) == "1.5M"
    @test OMOPCDMFeasibility._format_number(2000000) == "2.0M"
    @test OMOPCDMFeasibility._format_number(1500) == "1.5K"
    @test OMOPCDMFeasibility._format_number(2000) == "2.0K"
    @test OMOPCDMFeasibility._format_number(999) == "999"
    @test OMOPCDMFeasibility._format_number(100) == "100"
    @test OMOPCDMFeasibility._format_number(0) == "0"
end

@testset "Internal Helper Functions" begin
    fconn = OMOPCDMFeasibility._funsql(TEST_CONN; schema="main", dialect=:sqlite)
    @test fconn isa OMOPCDMFeasibility.FunSQL.SQLConnection

    concept_table = OMOPCDMFeasibility._resolve_table(fconn, :concept)
    @test concept_table isa OMOPCDMFeasibility.FunSQL.SQLTable

    test_data = [1, 2, 3]
    identity_func = x -> x
    result = OMOPCDMFeasibility._counter_reducer(test_data, [identity_func])
    @test result == test_data
end

@testset "_concept_col tests" begin
    # Test condition domain
    @test OMOPCDMFeasibility._concept_col(:condition_occurrence) == :condition_concept_id
    
    # Test person domain - this should hit the special case
    @test OMOPCDMFeasibility._concept_col(:person) == :gender_concept_id
    
    # Test unknown domain - this should hit the generic case
    @test OMOPCDMFeasibility._concept_col(:unknown_table) == :unknown_concept_id
end

@testset "Edge Cases and Error Handling" begin
    # Test _get_concept_name edge cases
    @test OMOPCDMFeasibility._get_concept_name(0, TEST_CONN; schema="main", dialect=:sqlite) ==
        "No matching concept"
    @test OMOPCDMFeasibility._get_concept_name(-1, TEST_CONN; schema="main", dialect=:sqlite) == "Unknown"

    # Test _format_number with edge cases
    @test OMOPCDMFeasibility._format_number(0.5) == "1"
    @test OMOPCDMFeasibility._format_number(-100) == "-100"

    # Test _domain_id_to_table with edge cases
    @test OMOPCDMFeasibility._domain_id_to_table("") == :_occurrence
    @test OMOPCDMFeasibility._domain_id_to_table("UPPERCASE") == :uppercase_occurrence

    # Test internal function error handling
    fconn = OMOPCDMFeasibility._funsql(TEST_CONN; schema="main", dialect=:sqlite)
    @test_throws ErrorException OMOPCDMFeasibility._resolve_table(fconn, :nonexistent_table)
end
