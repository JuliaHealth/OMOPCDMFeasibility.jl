@testset "_get_concept_name" begin
    result = OMOPCDMFeasibility._get_concept_name(
        8507, TEST_CONN; schema="main", dialect=:sqlite
    )
    @test result isa String
    @test result != "Unknown"

    result = OMOPCDMFeasibility._get_concept_name(
        999999999, TEST_CONN; schema="main", dialect=:sqlite
    )
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
    @test OMOPCDMFeasibility._get_concept_name(
        0, TEST_CONN; schema="main", dialect=:sqlite
    ) == "No matching concept"
    @test OMOPCDMFeasibility._get_concept_name(
        -1, TEST_CONN; schema="main", dialect=:sqlite
    ) == "Unknown"

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

@testset "Postcohort Internal Utility Functions" begin
    @testset "_get_cohort_person_ids" begin
        test_cohort_df = DataFrame(person_id=[1, 2, 3])
        result = OMOPCDMFeasibility._get_cohort_person_ids(
            nothing, test_cohort_df, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test result isa Vector
        @test length(result) == 3
    end

    @testset "_get_person_ids_from_dataframe" begin
        test_df = DataFrame(person_id=[1, 2, 3, 1])  # Test uniqueness
        result = OMOPCDMFeasibility._get_person_ids_from_dataframe(test_df)
        @test length(result) == 3  # Should be unique
        @test Set(result) == Set([1, 2, 3])
    end

    @testset "_get_category_name" begin
        # Test with concept ID
        result = OMOPCDMFeasibility._get_category_name(
            8507, :gender_concept_id, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test result isa String

        # Test with string value
        result_str = OMOPCDMFeasibility._get_category_name(
            "test", :some_column, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test result_str == "test"

        # Test with person_id column
        result_person = OMOPCDMFeasibility._get_category_name(
            123, :person_id, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test result_person == "123"
    end

    @testset "_create_individual_profile_table" begin
        test_df = DataFrame(
            person_id=[1, 2, 3, 4], gender_concept_id=[8507, 8507, 8532, 8507]
        )
        result = OMOPCDMFeasibility._create_individual_profile_table(
            test_df, :gender_concept_id, 4, 1000, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test result isa DataFrame
        @test "gender" in names(result)
        @test "cohort_numerator" in names(result)
    end

    @testset "_create_cartesian_profile_table" begin
        test_df = DataFrame(
            person_id=[1, 2, 3, 4],
            gender_concept_id=[8507, 8507, 8532, 8507],
            race_concept_id=[8527, 8516, 8527, 8516],
        )
        cols = [:gender_concept_id, :race_concept_id]
        result = OMOPCDMFeasibility._create_cartesian_profile_table(
            test_df, cols, 4, 1000, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test result isa DataFrame
        @test "gender" in names(result)
        @test "race" in names(result)
    end

    @testset "_build_cartesian_row" begin
        test_row = (gender_concept_id=8507, race_concept_id=8527, cohort_numerator=2)
        cols = [:gender_concept_id, :race_concept_id]
        all_covariate_names = ["gender", "race"]
        result = OMOPCDMFeasibility._build_cartesian_row(
            test_row,
            cols,
            all_covariate_names,
            4,
            1000,
            TEST_CONN;
            schema="main",
            dialect=:sqlite,
        )
        @test result isa Vector
        @test length(result) == 7  # 2 covariates + 5 stats
        @test result[end - 4] == 2   # cohort_numerator
    end

    @testset "Additional Error Cases" begin
        @test_throws ArgumentError OMOPCDMFeasibility._get_cohort_person_ids(
            nothing, nothing, TEST_CONN; schema="main", dialect=:sqlite
        )

        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_cohort_table(
            -1, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_cohort_table(
            0, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_cohort_table(
            "invalid", TEST_CONN; schema="main", dialect=:sqlite
        )

        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_cohort_table(
            99999, TEST_CONN; schema="main", dialect=:sqlite
        )

        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_dataframe(
            "not a dataframe"
        )
        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_dataframe([
            1, 2, 3
        ])

        missing_df = DataFrame(person_id=[missing, missing, missing])
        @test_throws ArgumentError OMOPCDMFeasibility._get_person_ids_from_dataframe(
            missing_df
        )

        # Test _get_category_name with invalid concept_id
        # Since _get_concept_name returns "Unknown" for invalid IDs, we get "Unknown"
        invalid_result = OMOPCDMFeasibility._get_category_name(
            999999999, :invalid_concept_id, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test invalid_result == "Unknown"  # _get_concept_name returns "Unknown" for invalid IDs

        # Test _get_category_name with string input
        string_result = OMOPCDMFeasibility._get_category_name(
            "test_string", :some_column, TEST_CONN; schema="main", dialect=:sqlite
        )
        @test string_result == "test_string"
    end
end
