@testset "create_individual_profiles" begin
    # Test basic functionality
    test_cohort_df = DataFrame(person_id=[1, 2, 3, 4, 5])
    result = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    @test result isa NamedTuple
    @test :gender in keys(result)
    
    # Test structure and data types
    gender_profile = result.gender
    @test gender_profile isa DataFrame
    expected_cols = ["gender", "cohort_numerator", "cohort_denominator", "database_denominator", "percent_cohort", "percent_database"]
    for col in expected_cols
        @test col in names(gender_profile)
    end
    @test all(gender_profile.cohort_denominator .== 5)
    
    # Test with multiple covariates
    result_multi = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    @test length(result_multi) == 2
    @test :gender in keys(result_multi)
    @test :race in keys(result_multi)
    
    # Test error cases
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=Function[],
        schema="main",
        dialect=:sqlite
    )
    
    @test_throws ArgumentError create_individual_profiles(
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    @test_throws ArgumentError create_individual_profiles(
        cohort_definition_id=1,
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    # Test empty cohort warning
    empty_cohort_df = DataFrame(person_id=Int[])
    result_empty = @test_logs (:warn, "Cohort is empty - no analysis will be performed") create_individual_profiles(
        cohort_df=empty_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    @test result_empty isa NamedTuple
    @test length(result_empty) == 0
end

@testset "create_cartesian_profiles" begin
    # Test basic functionality
    test_cohort_df = DataFrame(person_id=[1, 2, 3, 4, 5])
    result = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    @test result isa DataFrame
    @test :gender in names(result)
    @test :race in names(result)
    
    # Check expected columns
    expected_cols = [:gender, :race, :cohort_numerator, :cohort_denominator, :database_denominator, :percent_cohort, :percent_database]
    for col in expected_cols
        @test col in names(result)
    end
    
    # Test error cases
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],  # Only one function
        schema="main",
        dialect=:sqlite
    )
    
    @test_throws ArgumentError create_cartesian_profiles(
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    
    # Test empty cohort warning
    empty_cohort_df = DataFrame(person_id=Int[])
    result_empty = @test_logs (:warn, "Cohort is empty - no analysis will be performed") create_cartesian_profiles(
        cohort_df=empty_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    @test result_empty isa DataFrame
    @test nrow(result_empty) == 0
end

@testset "Edge Cases and Data Validation" begin
    # Test with invalid DataFrame structure
    invalid_df = DataFrame(patient_id=[1, 2, 3])  # Wrong column name
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=invalid_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=invalid_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
end
