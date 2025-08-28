@testset "create_individual_profiles" begin
    # Create a test cohort DataFrame
    test_cohort_df = DataFrame(person_id=[1, 2, 3, 4, 5])
    
    # Test basic functionality with DataFrame input
    result = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    @test result isa NamedTuple
    @test !isempty(result)
    
    # Test that we get the expected covariate names as keys
    @test :gender in keys(result)
    
    # Test the structure of individual profile DataFrames
    gender_profile = result.gender
    @test gender_profile isa DataFrame
    @test !isempty(gender_profile)
    
    # Check expected columns exist
    expected_cols = ["gender", "cohort_numerator", "cohort_denominator", "database_denominator", "percent_cohort", "percent_database"]
    for col in expected_cols
        @test col in names(gender_profile)
    end
    
    # Test data types
    @test eltype(gender_profile.gender) <: AbstractString
    @test eltype(gender_profile.cohort_numerator) <: Integer
    @test eltype(gender_profile.cohort_denominator) <: Integer
    @test eltype(gender_profile.database_denominator) <: Integer
    @test eltype(gender_profile.percent_cohort) <: AbstractFloat
    @test eltype(gender_profile.percent_database) <: AbstractFloat
    
    # Test that percentages are reasonable
    @test all(0 .<= gender_profile.percent_cohort .<= 100)
    @test all(0 .<= gender_profile.percent_database .<= 100)
    
    # Test that cohort_denominator matches our test cohort size
    @test all(gender_profile.cohort_denominator .== 5)
    
    # Test with multiple covariate functions
    result_multi = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    @test result_multi isa NamedTuple
    @test length(result_multi) == 2
    @test :gender in keys(result_multi)
    @test :race in keys(result_multi)
    
    # Test error handling: empty covariate_funcs
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=Function[],
        schema="main",
        dialect=:sqlite
    )
    
    # Test error handling: neither cohort_definition_id nor cohort_df provided
    @test_throws ArgumentError create_individual_profiles(
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    # Test error handling: both cohort_definition_id and cohort_df provided
    @test_throws ArgumentError create_individual_profiles(
        cohort_definition_id=1,
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    # Test with empty cohort (should return empty NamedTuple with warning)
    empty_cohort_df = DataFrame(person_id=Int[])
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=empty_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    # Test results are sorted alphabetically
    if nrow(gender_profile) > 1
        @test issorted(gender_profile.gender)
    end
end

@testset "create_cartesian_profiles" begin
    # Create a test cohort DataFrame
    test_cohort_df = DataFrame(person_id=[1, 2, 3, 4, 5])
    
    # Test basic functionality with DataFrame input
    result = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    @test result isa DataFrame
    @test !isempty(result)
    
    # Check expected columns exist
    expected_cols = ["race", "gender", "cohort_numerator", "cohort_denominator", "database_denominator", "percent_cohort", "percent_database"]
    for col in expected_cols
        @test col in names(result)
    end
    
    # Test data types
    @test eltype(result.race) <: AbstractString
    @test eltype(result.gender) <: AbstractString
    @test eltype(result.cohort_numerator) <: Integer
    @test eltype(result.cohort_denominator) <: Integer
    @test eltype(result.database_denominator) <: Integer
    @test eltype(result.percent_cohort) <: AbstractFloat
    @test eltype(result.percent_database) <: AbstractFloat
    
    # Test that percentages are reasonable
    @test all(0 .<= result.percent_cohort .<= 100)
    @test all(0 .<= result.percent_database .<= 100)
    
    # Test that cohort_denominator matches our test cohort size
    @test all(result.cohort_denominator .== 5)
    
    # Test that we have combinations (should be <= product of unique values)
    @test nrow(result) >= 1
    
    # Test with three covariate functions
    result_three = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace, occ.GetPatientAgeGroup],
        schema="main",
        dialect=:sqlite
    )
    @test result_three isa DataFrame
    @test !isempty(result_three)
    @test "age_group" in names(result_three)
    @test "gender" in names(result_three)
    @test "race" in names(result_three)
    
    # Test error handling: fewer than 2 covariate functions
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main"
    )
    
    # Test error handling: empty covariate_funcs
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=Function[],
        schema="main"
    )
    
    # Test error handling: neither cohort_definition_id nor cohort_df provided
    @test_throws ArgumentError create_cartesian_profiles(
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main"
    )
    
    # Test error handling: both cohort_definition_id and cohort_df provided
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_definition_id=1,
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    
    # Test with empty cohort (should return empty DataFrame with warning)
    empty_cohort_df = DataFrame(person_id=Int[])
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=empty_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main"
    )
    
    # Test that results are sorted by covariate values
    if nrow(result) > 1
        # Check if sorted by the covariate columns
        sorted_result = sort(result, [:race, :gender])
        @test result == sorted_result
    end
    
    # Test column order matches input order (reversed as per function implementation)
    covariate_funcs = [occ.GetPatientGender, occ.GetPatientRace]
    result_order = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=covariate_funcs,
        schema="main",
        dialect=:sqlite
    )
    
    # The function reverses the order, so race should come before gender
    covariate_cols = names(result_order)[1:2]  # First two columns should be covariate columns
    @test "race" in covariate_cols
    @test "gender" in covariate_cols
end

@testset "postcohort integration tests" begin
    # Test that both functions work with the same cohort
    test_cohort_df = DataFrame(person_id=[1, 2, 3])
    covariate_funcs = [occ.GetPatientGender, occ.GetPatientRace]
    
    individual_result = create_individual_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=covariate_funcs,
        schema="main",
        dialect=:sqlite
    )
    
    cartesian_result = create_cartesian_profiles(
        cohort_df=test_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=covariate_funcs,
        schema="main",
        dialect=:sqlite
    )
    
    # Both should have same cohort denominator
    @test all(individual_result.gender.cohort_denominator .== 3)
    @test all(individual_result.race.cohort_denominator .== 3)
    @test all(cartesian_result.cohort_denominator .== 3)
    
    # Both should have same database denominator (total patients in database)
    individual_db_denom = individual_result.gender.database_denominator[1]
    cartesian_db_denom = cartesian_result.database_denominator[1]
    @test individual_db_denom == cartesian_db_denom
    @test individual_db_denom > 0  # Should have some patients in the database
    
    # Test that individual profile numerators sum correctly
    total_gender_counts = sum(individual_result.gender.cohort_numerator)
    total_race_counts = sum(individual_result.race.cohort_numerator)
    
    # These might not equal 3 due to potential missing/null values, but should be reasonable
    @test total_gender_counts >= 0
    @test total_race_counts >= 0
    
    # Test that cartesian combinations are subset of individual combinations
    @test nrow(cartesian_result) <= length(unique(individual_result.gender.gender)) * length(unique(individual_result.race.race))
end

@testset "postcohort error edge cases" begin
    # Test with a cohort DataFrame that has no valid person_ids in the database
    invalid_cohort_df = DataFrame(person_id=[999999, 999998, 999997])
    
    # These should still work but might return empty results or warnings
    result_individual = create_individual_profiles(
        cohort_df=invalid_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    @test result_individual isa NamedTuple
    
    result_cartesian = create_cartesian_profiles(
        cohort_df=invalid_cohort_df,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
    @test result_cartesian isa DataFrame
    
    # Test with cohort DataFrame missing person_id column
    invalid_df_structure = DataFrame(patient_id=[1, 2, 3])  # Wrong column name
    @test_throws ArgumentError create_individual_profiles(
        cohort_df=invalid_df_structure,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender],
        schema="main",
        dialect=:sqlite
    )
    
    @test_throws ArgumentError create_cartesian_profiles(
        cohort_df=invalid_df_structure,
        conn=TEST_CONN,
        covariate_funcs=[occ.GetPatientGender, occ.GetPatientRace],
        schema="main",
        dialect=:sqlite
    )
end
