"""
    get_concept_name(concept_id, conn; schema="dbt_synthea_dev")

Retrieve the concept name for a given concept ID from the OMOP concept table.

# Arguments
- `concept_id`: The concept ID to look up
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `String`: The concept name, or "Unknown" if not found
"""
function get_concept_name(concept_id, conn; schema::String="dbt_synthea_dev")
    fconn = _funsql(conn; schema=schema)
    concept_table = _resolve_table(fconn, :concept)
    
    query = From(concept_table) |>
            Where(Get.concept_id .== concept_id) |>
            Select(Get.concept_name)
    
    result = DataFrame(query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    return isempty(result) ? "Unknown" : result.concept_name[1]
end

"""
    _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema="dbt_synthea_dev")

Extract person IDs from either a cohort definition ID or a cohort DataFrame.

# Arguments
- `cohort_definition_id`: ID of the cohort definition in the cohort table (or nothing)
- `cohort_df`: DataFrame containing cohort with `person_id` column (or nothing)
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `Vector`: Vector of unique person IDs
"""
function _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema::String="dbt_synthea_dev")
    if cohort_definition_id !== nothing
        return _get_person_ids_from_cohort_table(cohort_definition_id, conn; schema=schema)
    elseif cohort_df !== nothing
        return _get_person_ids_from_dataframe(cohort_df)
    else
        throw(ArgumentError("Must provide either cohort_definition_id or cohort_df"))
    end
end

"""
    _get_person_ids_from_cohort_table(cohort_definition_id, conn; schema="dbt_synthea_dev")

Extract person IDs from the cohort table using a cohort definition ID.

# Arguments
- `cohort_definition_id`: ID of the cohort definition
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `Vector`: Vector of unique person IDs (subject_id from cohort table)
"""
function _get_person_ids_from_cohort_table(cohort_definition_id, conn; schema::String="dbt_synthea_dev")
    if !isa(cohort_definition_id, Integer) || cohort_definition_id <= 0
        throw(ArgumentError("cohort_definition_id must be a positive integer"))
    end
    
    fconn = _funsql(conn; schema=schema)
    cohort_table = _resolve_table(fconn, :cohort)
    
    cohort_query = From(cohort_table) |>
                  Where(Get.cohort_definition_id .== cohort_definition_id) |>
                  Select(Get.subject_id, Get.cohort_start_date, Get.cohort_end_date)
    
    cohort_result = DataFrame(cohort_query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    
    if isempty(cohort_result)
        throw(ArgumentError("Cohort with definition ID $cohort_definition_id not found in database"))
    end
    
    return unique(cohort_result.subject_id)
end

"""
    _get_person_ids_from_dataframe(cohort_df)

Extract person IDs from a cohort DataFrame.

# Arguments
- `cohort_df`: DataFrame containing cohort with `person_id` column

# Returns
- `Vector`: Vector of unique person IDs from the DataFrame
"""
function _get_person_ids_from_dataframe(cohort_df)
    if !isa(cohort_df, DataFrame)
        throw(ArgumentError("cohort_df must be a DataFrame"))
    end
    
    if isempty(cohort_df)
        throw(ArgumentError("cohort_df cannot be empty"))
    end
    
    if !("person_id" in names(cohort_df))
        throw(ArgumentError("Cohort DataFrame must contain 'person_id' column. Found columns: $(names(cohort_df))"))
    end
    
    person_ids = unique(filter(!ismissing, cohort_df.person_id))
    
    if isempty(person_ids)
        throw(ArgumentError("No valid person_ids found in cohort_df"))
    end
    
    return person_ids
end

"""
    _get_database_total_patients(conn; schema="dbt_synthea_dev")

Get the total number of patients in the database.

# Arguments
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `Int`: Total count of people in the person table
"""
function _get_database_total_patients(conn; schema::String="dbt_synthea_dev")
    fconn = _funsql(conn; schema=schema)
    person_table = _resolve_table(fconn, :person)
    
    query = From(person_table) |> Select(Fun.count())
    result = DataFrame(query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    
    return result[1, 1]
end

"""
    _create_individual_profile_table(df, col, cohort_size, database_size, conn; schema="dbt_synthea_dev")

Create an individual profile table for a single covariate column.

# Arguments
- `df`: DataFrame with demographic data
- `col`: Column name to profile
- `cohort_size`: Total cohort size
- `database_size`: Total database population size
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `DataFrame`: Profile table with covariate categories and statistics
"""
function _create_individual_profile_table(df::DataFrame, col, cohort_size::Int, database_size::Int, conn; schema::String="dbt_synthea_dev")
    grouped_data = combine(groupby(df, col), nrow => :cohort_numerator)
    
    covariate_col_name = replace(string(col), "_concept_id" => "")
    result_df = DataFrame(
        Symbol(covariate_col_name) => String[],
        :cohort_numerator => Int[],
        :cohort_denominator => Int[],
        :database_denominator => Int[],
        :percent_cohort => Float64[],
        :percent_database => Float64[]
    )
    
    for row in eachrow(grouped_data)
        category = _get_category_name(row[col], col, conn; schema=schema)
        percent_cohort = round((row.cohort_numerator / cohort_size) * 100, digits=2)
        percent_database = round((row.cohort_numerator / database_size) * 100, digits=2)
        
        push!(result_df, (
            category,
            row.cohort_numerator,
            cohort_size,
            database_size,
            percent_cohort,
            percent_database
        ))
    end
    
    sort!(result_df, Symbol(covariate_col_name))
    return result_df
end

"""
    _get_category_name(value, col, conn; schema="dbt_synthea_dev")

Get the human-readable category name for a covariate value.

# Arguments
- `value`: The value to convert (concept ID or string)
- `col`: The column name
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `String`: Human-readable category name
"""
function _get_category_name(value, col, conn; schema::String="dbt_synthea_dev")
    if isa(value, Integer) && string(col) != "person_id"
        try
            return get_concept_name(value, conn; schema=schema)
        catch
            @warn "Could not retrieve concept name for concept_id $value, using ID as string"
            return string(value)
        end
    else
        return string(value)
    end
end

"""
    _create_cartesian_profile_table(df, cols, cohort_size, database_size, conn; schema="dbt_synthea_dev")

Create a Cartesian product profile table with all covariate combinations.

# Arguments
- `df`: DataFrame with demographic data
- `cols`: Vector of column names to include in combinations
- `cohort_size`: Total cohort size
- `database_size`: Total database population size
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")

# Returns
- `DataFrame`: Table with all covariate combinations and statistics
"""
function _create_cartesian_profile_table(df::DataFrame, cols, cohort_size::Int, database_size::Int, conn; schema::String="dbt_synthea_dev")
    all_covariate_names = [replace(string(c), "_concept_id" => "") for c in cols]
    
    result_df = DataFrame()
    for cov_name in all_covariate_names
        result_df[!, Symbol(cov_name)] = String[]
    end
    for stat_col in [:cohort_numerator, :cohort_denominator, :database_denominator, :percent_cohort, :percent_database]
        result_df[!, stat_col] = stat_col == :cohort_numerator ? Int[] : 
                                stat_col in [:cohort_denominator, :database_denominator] ? Int[] : Float64[]
    end
    
    grouped_data = combine(groupby(df, cols), nrow => :cohort_numerator)
    sort!(grouped_data, cols)
    
    for row in eachrow(grouped_data)
        result_row = _build_cartesian_row(row, cols, all_covariate_names, cohort_size, database_size, conn; schema=schema)
        push!(result_df, result_row)
    end
    
    column_order = [Symbol.(all_covariate_names)..., :cohort_numerator, :cohort_denominator, :database_denominator, :percent_cohort, :percent_database]
    return select(result_df, column_order...)
end

function _build_cartesian_row(row, cols, all_covariate_names, cohort_size::Int, database_size::Int, conn; schema::String="dbt_synthea_dev")
    result_row = Vector{Any}(undef, length(all_covariate_names) + 5)
    
    for (idx, col) in enumerate(cols)
        result_row[idx] = _get_category_name(row[col], col, conn; schema=schema)
    end
    
    stat_start_idx = length(all_covariate_names) + 1
    result_row[stat_start_idx] = row.cohort_numerator
    result_row[stat_start_idx + 1] = cohort_size
    result_row[stat_start_idx + 2] = database_size
    result_row[stat_start_idx + 3] = round((row.cohort_numerator / cohort_size) * 100, digits=2)
    result_row[stat_start_idx + 4] = round((row.cohort_numerator / database_size) * 100, digits=2)
    
    return result_row
end

function _funsql(conn; schema::String="dbt_synthea_dev")
    return SQLConnection(conn; catalog=reflect(conn; schema=schema, dialect=:postgresql))
end

function _resolve_table(fconn::SQLConnection, tblsym::Symbol)
    lname = lowercase(String(tblsym))
    for t in values(fconn.catalog.tables)
        if lowercase(String(t.name)) == lname
            return t
        end
    end
    error("Table not found: $(tblsym)")
end

function _counter_reducer(sub, funcs)
    for fun in funcs
        sub = fun(sub)
    end
    return sub
end
