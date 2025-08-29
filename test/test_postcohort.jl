@testset "create_individual_profiles" begin
    test_cohort_df = DataFrame(person_id=[1, 2, 3, 4, 5])

    # Test basic functionality
    result = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite,
    )
    @test result isa NamedTuple
    @test :gender in keys(result)

    # Test structure
    gender_profile = result.gender
    @test gender_profile isa DataFrame
    expected_cols = [
        "gender",
        "cohort_numerator",
        "cohort_denominator",
        "database_denominator",
        "percent_cohort",
        "percent_database",
    ]
    for col in expected_cols
        @test col in names(gender_profile)
    end
    @test all(gender_profile.cohort_denominator .== 5)

    # Test multiple covariates
    result_multi = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )
    @test length(result_multi) == 2
    @test :gender in keys(result_multi) && :race in keys(result_multi)

    # Test error cases
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=Function[],
        schema="main",
        dialect=:sqlite,
    )
    @test_throws ArgumentError create_individual_profiles(
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite,
    )
    @test_throws ArgumentError create_individual_profiles(
        cohort_definition_id=1,
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite,
    )

    # Test empty cohort
    empty_cohort_df = DataFrame(person_id=Int[])
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=empty_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite,
    )
end

@testset "create_cartesian_profiles" begin
    test_cohort_df = DataFrame(person_id=[1, 2, 3, 4, 5])

    # Test basic functionality
    result = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )
    @test result isa DataFrame
    @test "gender" in names(result) && "race" in names(result)
    @test all(result.cohort_denominator .== 5)

    # Test error cases
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite,
    )
    @test_throws ArgumentError create_cartesian_profiles(
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )

    # Test empty cohort
    empty_cohort_df = DataFrame(person_id=Int[])
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=empty_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )
end

@testset "Edge Cases and Integration" begin
    test_cohort_df = DataFrame(person_id=[1, 2, 3])

    # Test integration between both functions
    individual_result = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )
    cartesian_result = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )

    @test all(individual_result.gender.cohort_denominator .== 3)
    @test all(cartesian_result.cohort_denominator .== 3)

    # Test invalid DataFrame structure
    invalid_df = DataFrame(patient_id=[1, 2, 3])
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=invalid_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite,
    )
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=invalid_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite,
    )
end
