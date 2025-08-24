@testset "analyze_concept_distribution" begin
    concept_ids = [201820, 192671]

    result = analyze_concept_distribution(TEST_CONN; concept_set=concept_ids, schema="main", dialect=:sqlite)
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
        TEST_CONN; concept_set=concept_ids, covariate_funcs=Function[], schema="main", dialect=:sqlite
    )
    @test result_with_covariates isa DataFrame
    @test !isempty(result_with_covariates)

    if nrow(result) > 1
        @test issorted(result.count, rev=true)
    end

    # Error handling tests
    @test_throws ArgumentError analyze_concept_distribution(
        TEST_CONN; concept_set=Int[], schema="main", dialect=:sqlite
    )

    invalid_concepts = [999999999, 888888888]
    result_invalid = analyze_concept_distribution(
        TEST_CONN; concept_set=invalid_concepts, schema="main", dialect=:sqlite
    )
    @test result_invalid isa DataFrame
    @test nrow(result_invalid) == 0 || all(result_invalid.count .== 0)

    single_concept = [201820]
    result_single = analyze_concept_distribution(
        TEST_CONN; concept_set=single_concept, schema="main", dialect=:sqlite
    )
    @test result_single isa DataFrame
end

@testset "generate_summary" begin
    concept_ids = [201820, 192671]

    # Test with formatted values (default)
    result = generate_summary(TEST_CONN; concept_set=concept_ids, schema="main", dialect=:sqlite)
    @test result isa DataFrame
    @test !isempty(result)
    @test "metric" in names(result)
    @test "value" in names(result)
    @test "interpretation" in names(result)
    @test "domain" in names(result)

    @test eltype(result.metric) <: AbstractString
    @test eltype(result.value) <: Union{AbstractString, Number}
    @test eltype(result.interpretation) <: AbstractString
    @test eltype(result.domain) <: AbstractString

    # Check that we get only Summary domain results
    @test all(result.domain .== "Summary")
    
    # Check for expected metrics
    metrics = result.metric
    @test "Total Patients" in metrics
    @test "Eligible Patients" in metrics
    @test "Total Target Records" in metrics
    @test "Population Coverage (%)" in metrics

    # Test with raw values
    result_raw = generate_summary(TEST_CONN; concept_set=concept_ids, schema="main", dialect=:sqlite, raw_values=true)
    @test result_raw isa DataFrame
    @test !isempty(result_raw)
    
    # When raw_values=true, numeric metrics should be actual numbers
    total_patients_row = result_raw[result_raw.metric .== "Total Patients", :]
    if !isempty(total_patients_row)
        @test total_patients_row.value[1] isa Number
    end

    # Error handling tests
    @test_throws ArgumentError generate_summary(
        TEST_CONN; concept_set=Int[], schema="main", dialect=:sqlite
    )

    invalid_concepts = [999999999, 888888888]
    result_invalid = generate_summary(
        TEST_CONN; concept_set=invalid_concepts, schema="main", dialect=:sqlite
    )
    @test result_invalid isa DataFrame
    @test "No Valid Concepts" in result_invalid.metric
end

@testset "generate_domain_breakdown" begin
    concept_ids = [201820, 192671]

    # Test with formatted values (default)
    result = generate_domain_breakdown(TEST_CONN; concept_set=concept_ids, schema="main", dialect=:sqlite)
    @test result isa DataFrame
    @test "metric" in names(result)
    @test "value" in names(result)
    @test "interpretation" in names(result)
    @test "domain" in names(result)

    @test eltype(result.metric) <: AbstractString
    @test eltype(result.value) <: Union{AbstractString, Number}
    @test eltype(result.interpretation) <: AbstractString
    @test eltype(result.domain) <: AbstractString

    # Should NOT contain Summary domain (that's in generate_summary)
    if !isempty(result)
        @test !("Summary" in result.domain)
        
        # Check for domain-specific metrics
        metrics = result.metric
        domain_metrics = filter(m -> contains(m, " - "), metrics)
        @test !isempty(domain_metrics)
    end

    # Test with raw values
    result_raw = generate_domain_breakdown(TEST_CONN; concept_set=concept_ids, schema="main", dialect=:sqlite, raw_values=true)
    @test result_raw isa DataFrame

    # Error handling tests
    @test_throws ArgumentError generate_domain_breakdown(
        TEST_CONN; concept_set=Int[], schema="main", dialect=:sqlite
    )

    invalid_concepts = [999999999, 888888888]
    result_invalid = generate_domain_breakdown(
        TEST_CONN; concept_set=invalid_concepts, schema="main", dialect=:sqlite
    )
    @test result_invalid isa DataFrame
end
