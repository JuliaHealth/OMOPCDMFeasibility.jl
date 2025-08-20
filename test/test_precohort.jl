
@testset "analyze_concept_distribution" begin
    concept_ids = [201820, 192671]  
    
    result = analyze_concept_distribution(TEST_CONN; concept_set=concept_ids, schema="main")
    @test result isa DataFrame
    @test !isempty(result)
    @test "concept_id" in names(result)
    @test "concept_name" in names(result)
    @test "domain" in names(result)
    @test "count" in names(result)
    
    @test eltype(result.concept_id) <: Number
    @test eltype(result.concept_name) <: AbstractString
    @test eltype(result.domain) <: AbstractString
    @test eltype(result.count) <: Number
    
    @test all(result.count .>= 0)
    
    result_with_covariates = analyze_concept_distribution(
        TEST_CONN; 
        concept_set=concept_ids, 
        covariate_funcs=Function[],
        schema="main"
    )
    @test result_with_covariates isa DataFrame
    @test !isempty(result_with_covariates)
    
    if nrow(result) > 1
        @test issorted(result.count, rev=true)
    end
    
    # Error handling tests
    @test_throws ArgumentError analyze_concept_distribution(TEST_CONN; concept_set=Int[], schema="main")
    
    invalid_concepts = [999999999, 888888888]
    result_invalid = analyze_concept_distribution(TEST_CONN; concept_set=invalid_concepts, schema="main")
    @test result_invalid isa DataFrame
    @test nrow(result_invalid) == 0 || all(result_invalid.count .== 0)
    
    single_concept = [201820]
    result_single = analyze_concept_distribution(TEST_CONN; concept_set=single_concept, schema="main")
    @test result_single isa DataFrame
end

@testset "generate_feasibility_report" begin
    concept_ids = [201820, 192671] 
    
    result = generate_feasibility_report(TEST_CONN; concept_set=concept_ids, schema="main")
    @test result isa DataFrame
    @test !isempty(result)
    @test "metric" in names(result)
    @test "value" in names(result)
    @test "interpretation" in names(result)
    @test "domain" in names(result)
    
    @test eltype(result.metric) <: AbstractString
    @test eltype(result.value) <: AbstractString
    @test eltype(result.interpretation) <: AbstractString
    @test eltype(result.domain) <: AbstractString
    
    metrics = result.metric
    @test "Total Patients" in metrics
    @test "Eligible Patients" in metrics
    @test "Total Target Records" in metrics
    @test "Population Coverage (%)" in metrics
    
    summary_rows = result[result.domain .== "Summary", :]
    @test nrow(summary_rows) >= 4
    
    result_no_covariates = generate_feasibility_report(
        TEST_CONN; 
        concept_set=concept_ids,
        covariate_funcs=Function[],
        schema="main"
    )
    @test result_no_covariates isa DataFrame
    @test !isempty(result_no_covariates)
    
    # Error handling tests
    @test_throws ArgumentError generate_feasibility_report(TEST_CONN; concept_set=Int[], schema="main")
    
    invalid_concepts = [999999999, 888888888]
    result_invalid = generate_feasibility_report(TEST_CONN; concept_set=invalid_concepts, schema="main")
    @test result_invalid isa DataFrame
    @test "No Valid Concepts" in result_invalid.metric || nrow(result_invalid) >= 1
    
    single_concept = [201820]
    result_single = generate_feasibility_report(TEST_CONN; concept_set=single_concept, schema="main")
    @test result_single isa DataFrame
end
