"""
    create_individual_profiles(;
        cohort_definition_id::Union{Int, Nothing} = nothing,
        cohort_df::Union{DataFrame, Nothing} = nothing,
        conn,
        covariate_funcs::AbstractVector{<:Function},
        schema::String = "dbt_synthea_dev",
        dialect::Symbol = :postgresql
    )

Creates individual demographic profile tables for a cohort by analyzing each covariate separately.

This function generates separate DataFrames for each demographic covariate (e.g., gender, race, age group),
providing detailed statistics including cohort and database-level percentages for post-cohort feasibility analysis.
Results are sorted alphabetically by covariate values for consistent, readable output.

# Arguments
- `conn` - Database connection using DBInterface
- `covariate_funcs` - Vector of covariate functions from OMOPCDMCohortCreator (e.g., `GetPatientGender`, `GetPatientRace`)

# Keyword Arguments
- `cohort_definition_id` - ID of the cohort definition in the cohort table (or nothing). Either this or `cohort_df` must be provided
- `cohort_df` - DataFrame containing cohort with `person_id` column (or nothing). Either this or `cohort_definition_id` must be provided  
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `NamedTuple` - Named tuple with keys corresponding to covariate names, each containing a DataFrame with covariate categories and statistics

# Examples
```julia
using OMOPCDMCohortCreator: GetPatientGender, GetPatientRace, GetPatientAgeGroup

individual_profiles = create_individual_profiles(
    cohort_df = my_cohort_df,
    conn = conn,
    covariate_funcs = [GetPatientGender, GetPatientRace, GetPatientAgeGroup]
)
```
"""
function create_individual_profiles(;
    cohort_definition_id::Union{Int, Nothing} = nothing,
    cohort_df::Union{DataFrame, Nothing} = nothing,
    conn,
    covariate_funcs::AbstractVector{<:Function},
    schema::String = "dbt_synthea_dev",
    dialect::Symbol = :postgresql
)
    if cohort_definition_id === nothing && cohort_df === nothing
        throw(ArgumentError("Must provide either cohort_definition_id or cohort_df"))
    end
    
    if isempty(covariate_funcs)
        throw(ArgumentError("covariate_funcs cannot be empty"))
    end
    
    person_ids = _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema=schema, dialect=dialect)
    cohort_size = length(person_ids)
    
    database_size = _get_database_total_patients(conn; schema=schema, dialect=dialect)
    
    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
    demographics_df = _counter_reducer(person_ids, _funcs)
    
    result_tables = Dict{Symbol, DataFrame}()
    
    for col in names(demographics_df)
        if col != "person_id"
            covariate_stats = _create_individual_profile_table(
                demographics_df, col, cohort_size, database_size, conn; schema=schema, dialect=dialect
            )
            covariate_name = Symbol(replace(string(col), "_concept_id" => ""))
            result_tables[covariate_name] = covariate_stats
        end
    end
    
    return NamedTuple(result_tables)
end

"""
    create_cartesian_profiles(;
        cohort_definition_id::Union{Int, Nothing} = nothing,
        cohort_df::Union{DataFrame, Nothing} = nothing,
        conn,
        covariate_funcs::AbstractVector{<:Function},
        schema::String = "dbt_synthea_dev",
        dialect::Symbol = :postgresql
    )

Creates Cartesian product demographic profiles for a cohort by analyzing all combinations of covariates.

This function generates a single DataFrame containing all possible combinations of demographic 
covariates (e.g., gender × race × age_group), providing comprehensive cross-tabulated statistics 
for detailed post-cohort feasibility analysis. Column order matches the input `covariate_funcs` order,
and results are sorted by covariate values for interpretable output.

# Arguments
- `conn` - Database connection using DBInterface
- `covariate_funcs` - Vector of covariate functions from OMOPCDMCohortCreator (must contain at least 2 functions)

# Keyword Arguments
- `cohort_definition_id` - ID of the cohort definition in the cohort table (or nothing). Either this or `cohort_df` must be provided
- `cohort_df` - DataFrame containing cohort with `person_id` column (or nothing). Either this or `cohort_definition_id` must be provided
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `DataFrame` - Cross-tabulated profile table with all covariate combinations and statistics

# Examples
```julia
using OMOPCDMCohortCreator: GetPatientAgeGroup, GetPatientGender, GetPatientRace

cartesian_profiles = create_cartesian_profiles(
    cohort_df = my_cohort_df,
    conn = conn,
    covariate_funcs = [GetPatientAgeGroup, GetPatientGender, GetPatientRace]
)
```
"""
function create_cartesian_profiles(;
    cohort_definition_id::Union{Int, Nothing} = nothing,
    cohort_df::Union{DataFrame, Nothing} = nothing,
    conn,
    covariate_funcs::AbstractVector{<:Function},
    schema::String = "dbt_synthea_dev",
    dialect::Symbol = :postgresql
)
    if cohort_definition_id === nothing && cohort_df === nothing
        throw(ArgumentError("Must provide either cohort_definition_id or cohort_df"))
    end
    
    if length(covariate_funcs) < 2
        throw(ArgumentError("Need at least 2 covariate functions for Cartesian combinations"))
    end
    
    person_ids = _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema=schema, dialect=dialect)
    cohort_size = length(person_ids)
    
    database_size = _get_database_total_patients(conn; schema=schema, dialect=dialect)
    
    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
    demographics_df = _counter_reducer(person_ids, _funcs)
    
    demographic_cols = names(demographics_df)[names(demographics_df) .!= "person_id"]
    ordered_cols = reverse(demographic_cols)
    
    result_df = _create_cartesian_profile_table(
        demographics_df, ordered_cols, cohort_size, database_size, conn; schema=schema, dialect=dialect
    )
    
    return result_df
end