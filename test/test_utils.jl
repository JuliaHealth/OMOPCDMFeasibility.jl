@testset "get_concept_name" begin
    result = OMOPCDMFeasibility.get_concept_name(8507, TEST_CONN; schema="main")
    @test result isa String
    @test result != "Unknown"
    
    result = OMOPCDMFeasibility.get_concept_name(999999999, TEST_CONN; schema="main")
    @test result == "Unknown"
end

@testset "get_concepts_by_domain" begin
    concept_ids = [8507, 8532]
    result = OMOPCDMFeasibility.get_concepts_by_domain(concept_ids, TEST_CONN; schema="main")
    @test result isa Dict
    @test !isempty(result)
    
    # Test with empty vector
    result_empty = OMOPCDMFeasibility.get_concepts_by_domain(Int[], TEST_CONN; schema="main")
    @test result_empty isa Dict
    @test isempty(result_empty)
    
    # Test with invalid concept IDs
    result_invalid = OMOPCDMFeasibility.get_concepts_by_domain([999999999], TEST_CONN; schema="main")
    @test result_invalid isa Dict
    @test isempty(result_invalid)
end

@testset "domain_id_to_table" begin
        @test OMOPCDMFeasibility.domain_id_to_table("Condition") == :condition_occurrence
        @test OMOPCDMFeasibility.domain_id_to_table("Drug") == :drug_exposure
        @test OMOPCDMFeasibility.domain_id_to_table("Procedure") == :procedure_occurrence
        @test OMOPCDMFeasibility.domain_id_to_table("Measurement") == :measurement
        @test OMOPCDMFeasibility.domain_id_to_table("Observation") == :observation
        
        result = OMOPCDMFeasibility.domain_id_to_table("UnknownDomain")
        @test result isa Symbol
        @test String(result) == "unknowndomain_occurrence"
    end
    
    @testset "format_number" begin
        @test OMOPCDMFeasibility.format_number(1500000) == "1.5M"
        @test OMOPCDMFeasibility.format_number(2000000) == "2.0M"
        @test OMOPCDMFeasibility.format_number(1500) == "1.5K"
        @test OMOPCDMFeasibility.format_number(2000) == "2.0K"
        @test OMOPCDMFeasibility.format_number(999) == "999"
        @test OMOPCDMFeasibility.format_number(100) == "100"
        @test OMOPCDMFeasibility.format_number(0) == "0"
    end
    
@testset "Internal Helper Functions" begin
    fconn = OMOPCDMFeasibility._funsql(TEST_CONN; schema="main")
    @test fconn isa OMOPCDMFeasibility.FunSQL.SQLConnection
    
    concept_table = OMOPCDMFeasibility._resolve_table(fconn, :concept)
    @test concept_table isa OMOPCDMFeasibility.FunSQL.SQLTable
    
    test_data = [1, 2, 3]
    identity_func = x -> x
    result = OMOPCDMFeasibility._counter_reducer(test_data, [identity_func])
    @test result == test_data
end

@testset "Edge Cases and Error Handling" begin
    # Test get_concept_name edge cases
    @test OMOPCDMFeasibility.get_concept_name(0, TEST_CONN; schema="main") == "No matching concept"
    @test OMOPCDMFeasibility.get_concept_name(-1, TEST_CONN; schema="main") == "Unknown"
        
    # Test format_number with edge cases
    @test OMOPCDMFeasibility.format_number(0.5) == "1"
    @test OMOPCDMFeasibility.format_number(-100) == "-100"
    
    # Test domain_id_to_table with edge cases
    @test OMOPCDMFeasibility.domain_id_to_table("") == :_occurrence
    @test OMOPCDMFeasibility.domain_id_to_table("UPPERCASE") == :uppercase_occurrence
    
    # Test internal function error handling
    fconn = OMOPCDMFeasibility._funsql(TEST_CONN; schema="main")
    @test_throws ErrorException OMOPCDMFeasibility._resolve_table(fconn, :nonexistent_table)
end
