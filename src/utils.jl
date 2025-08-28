"""
    _get_concept_name(concept_id, conn; schema="main", dialect=:postgresql) -> String

Retrieves the human-readable name for a given OMOP concept ID.

# Arguments
- `concept_id` - Thfunction _get_database_total_patients(conn; schema::String="dbt_synthea_dev", dialect::Symbol=:postgresql)
    fconn = _funsql(conn; schema=schema, dialect=dialect)
    person_table = _resolve_table(fconn, :person)MOP concept ID to look up
- `conn` - Database connection using DBInterface

# Keyword Arguments  
- `schema` - Database schema name. Default: `"main"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `String` - The concept name, or "Unknown" if the concept ID is not found

# Examples
```julia
name = _get_concept_name(8507, conn)
# Returns: "Male"

name = _get_concept_name(999999, conn) 
# Returns: "Unknown"
```
"""
function _get_concept_name(concept_id, conn; schema="main", dialect=:postgresql)
    fconn = _funsql(conn; schema=schema, dialect=dialect)
    concept_table = _resolve_table(fconn, :concept)
    
    query = From(concept_table) |>
            Where(Get.concept_id .== concept_id) |>
            Select(Get.concept_name)
    
    result = DataFrame(query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    return isempty(result) ? "Unknown" : result.concept_name[1]
end

"""
    _get_concepts_by_domain(concept_ids::Vector{<:Integer}, conn; schema="main", dialect=:postgresql) -> Dict{String, Vector{Int}}

Groups a list of OMOP concept IDs by their domain classification.

This function queries the concept table to determine which domain each concept belongs to
(e.g., "Condition", "Drug", "Procedure") and returns them grouped by domain.

# Arguments
- `concept_ids` - Vector of OMOP concept IDs to classify
- `conn` - Database connection using DBInterface

# Keyword Arguments
- `schema` - Database schema name. Default: `"main"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `Dict{String, Vector{Int}}` - Dictionary mapping domain names to vectors of concept IDs

# Examples
```julia
concepts = [201820, 192671, 1503297]
domains = _get_concepts_by_domain(concepts, conn)
# Returns: Dict("Condition" => [201820, 192671], "Drug" => [1503297])
```
"""
function _get_concepts_by_domain(concept_ids::Vector{<:Integer}, conn; schema="main", dialect=:postgresql)
    fconn = _funsql(conn; schema=schema, dialect=dialect)
    concept_table = _resolve_table(fconn, :concept)
    
    query = From(concept_table) |>
            Where(Fun.in(Get.concept_id, concept_ids...)) |>
            Select(Get.concept_id, Get.domain_id, Get.concept_name)
    
    result = DataFrame(query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    
    if isempty(result)
        return Dict{String, Vector{Int}}()
    end
    
    grouped = Dict{String, Vector{Int}}()
    for row in eachrow(result)
        domain = row.domain_id
        if !haskey(grouped, domain)
            grouped[domain] = Int[]
        end
        push!(grouped[domain], row.concept_id)
    end
    
    return grouped
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
function _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema::String="dbt_synthea_dev", dialect::Symbol=:postgresql)
    if cohort_definition_id !== nothing
        return _get_person_ids_from_cohort_table(cohort_definition_id, conn; schema=schema, dialect=dialect)
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
function _get_person_ids_from_cohort_table(cohort_definition_id, conn; schema::String="dbt_synthea_dev", dialect::Symbol=:postgresql)
    if !isa(cohort_definition_id, Integer) || cohort_definition_id <= 0
        throw(ArgumentError("cohort_definition_id must be a positive integer"))
    end
    
    fconn = _funsql(conn; schema=schema, dialect=dialect)
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
function _get_database_total_patients(conn; schema::String="dbt_synthea_dev", dialect::Symbol=:postgresql)
    fconn = _funsql(conn; schema=schema, dialect=dialect)
    person_table = _resolve_table(fconn, :person)
    
    query = From(person_table) |> Select(Fun.count())
    result = DataFrame(query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    
    return result[1, 1]
end

"""
    _create_individual_profile_table(df, col, cohort_size, database_size, conn; schema="dbt_synthea_dev", dialect=:postgresql)

Create an individual profile table for a single covariate column.

# Arguments
- `df`: DataFrame with demographic data
- `col`: Column name to profile
- `cohort_size`: Total cohort size
- `database_size`: Total database population size
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")
- `dialect`: SQL dialect (default: :postgresql)

# Returns
- `DataFrame`: Profile table with covariate categories and statistics
"""
function _create_individual_profile_table(df::DataFrame, col, cohort_size::Int, database_size::Int, conn; schema::String="dbt_synthea_dev", dialect=:postgresql)
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
        category = _get_category_name(row[col], col, conn; schema=schema, dialect=dialect)
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
    _get_category_name(value, col, conn; schema="dbt_synthea_dev", dialect=:postgresql)

Get the human-readable category name for a covariate value.

# Arguments
- `value`: The value to convert (concept ID or string)
- `col`: The column name
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")
- `dialect`: SQL dialect (default: :postgresql)

# Returns
- `String`: Human-readable category name
"""
function _get_category_name(value, col, conn; schema::String="dbt_synthea_dev", dialect=:postgresql)
    if isa(value, Integer) && string(col) != "person_id"
        try
            return _get_concept_name(value, conn; schema=schema, dialect=dialect)
        catch
            @warn "Could not retrieve concept name for concept_id $value, using ID as string"
            return string(value)
        end
    else
        return string(value)
    end
end

"""
    _create_cartesian_profile_table(df, cols, cohort_size, database_size, conn; schema="dbt_synthea_dev", dialect=:postgresql)

Create a Cartesian product profile table with all covariate combinations.

# Arguments
- `df`: DataFrame with demographic data
- `cols`: Vector of column names to include in combinations
- `cohort_size`: Total cohort size
- `database_size`: Total database population size
- `conn`: Database connection object
- `schema`: Database schema name (default: "dbt_synthea_dev")
- `dialect`: SQL dialect (default: :postgresql)

# Returns
- `DataFrame`: Table with all covariate combinations and statistics
"""
function _create_cartesian_profile_table(df::DataFrame, cols, cohort_size::Int, database_size::Int, conn; schema::String="dbt_synthea_dev", dialect=:postgresql)
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
        result_row = _build_cartesian_row(row, cols, all_covariate_names, cohort_size, database_size, conn; schema=schema, dialect=dialect)
        push!(result_df, result_row)
    end
    
    column_order = [Symbol.(all_covariate_names)..., :cohort_numerator, :cohort_denominator, :database_denominator, :percent_cohort, :percent_database]
    return select(result_df, column_order...)
end

function _build_cartesian_row(row, cols, all_covariate_names, cohort_size::Int, database_size::Int, conn; schema::String="dbt_synthea_dev", dialect=:postgresql)
    result_row = Vector{Any}(undef, length(all_covariate_names) + 5)
    
    for (idx, col) in enumerate(cols)
        result_row[idx] = _get_category_name(row[col], col, conn; schema=schema, dialect=dialect)
    end
    
    stat_start_idx = length(all_covariate_names) + 1
    result_row[stat_start_idx] = row.cohort_numerator
    result_row[stat_start_idx + 1] = cohort_size
    result_row[stat_start_idx + 2] = database_size
    result_row[stat_start_idx + 3] = round((row.cohort_numerator / cohort_size) * 100, digits=2)
    result_row[stat_start_idx + 4] = round((row.cohort_numerator / database_size) * 100, digits=2)
    
    return result_row
end

"""
    _concept_col(tblsym::Symbol) -> Symbol

Generates the concept column name for a given table symbol.

This is an internal helper function that constructs the appropriate concept column name
based on table naming conventions. Special handling is provided for the person table
which uses gender_concept_id.

# Arguments
- `tblsym` - The table symbol

# Returns
- `Symbol` - The concept column name for that table

# Examples
```julia
col = _concept_col(:condition_occurrence)
# Returns: :condition_concept_id

col = _concept_col(:person)
# Returns: :gender_concept_id
```
"""
function _concept_col(tblsym::Symbol) 
    if tblsym == :person
        return :gender_concept_id
    else
        return Symbol("$(split(String(tblsym), '_')[1])_concept_id")
    end
end

"""
    _funsql(conn; schema::String="main", dialect::Symbol=:postgresql) -> SQLConnection

Creates a FunSQL connection with database schema reflection.

This internal function sets up a FunSQL SQLConnection with the appropriate database
dialect and schema reflection for query building. Use :postgresql for DuckDB and :sqlite for SQLite.

# Arguments
- `conn` - Raw database connection

# Keyword Arguments
- `schema` - Database schema name. Default: `"main"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `SQLConnection` - FunSQL connection object with reflected schema
"""
function _funsql(conn; schema::String="main", dialect::Symbol=:postgresql)
    return SQLConnection(conn; catalog = reflect(conn; schema=schema, dialect=dialect))
end

"""
    _resolve_table(fconn::SQLConnection, tblsym::Symbol) -> Table

Resolves a table symbol to its corresponding FunSQL table object.

This internal function looks up a table by name in the FunSQL catalog, performing
case-insensitive matching.

# Arguments
- `fconn` - FunSQL SQLConnection object
- `tblsym` - Table symbol to resolve

# Returns
- `Table` - FunSQL table object

# Throws
- `ErrorException` - If the table is not found in the catalog
"""
function _resolve_table(fconn::SQLConnection, tblsym::Symbol)
    lname = lowercase(String(tblsym))
    for t in values(fconn.catalog.tables)
        if lowercase(String(t.name)) == lname
            return t
        end
    end
    error("table not found: $(tblsym)")
end

"""
    _counter_reducer(sub, funcs) -> Any

Applies a sequence of functions to a subject, reducing through function composition.

This internal helper function sequentially applies each function in the funcs vector
to the result of the previous function, starting with sub.

# Arguments
- `sub` - Initial subject/input to transform
- `funcs` - Vector of functions to apply sequentially

# Returns
- `Any` - Result after applying all functions

# Examples
```julia
result = _counter_reducer([1,2,3], [x -> x .* 2, sum])
# Equivalent to: sum([1,2,3] .* 2) = sum([2,4,6]) = 12
```
"""
function _counter_reducer(sub, funcs)
    for fun in funcs
        sub = fun(sub)  
    end
    return sub
end

"""
    _setup_domain_query(conn; domain::Symbol, schema::String="main", dialect::Symbol=:postgresql) -> NamedTuple

Sets up the necessary components for querying a specific domain table.

This internal function prepares all the components needed to query a domain-specific
table including the FunSQL connection, resolved table objects, and appropriate
concept column name.

# Arguments
- `conn` - Database connection

# Keyword Arguments
- `domain` - Domain table symbol (e.g., :condition_occurrence)
- `schema` - Database schema name. Default: `"main"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `NamedTuple` - Contains fconn, tbl, concept_table, and concept_col components

# Examples
```julia
setup = _setup_domain_query(conn; domain=:condition_occurrence)
# Returns: (fconn=..., tbl=..., concept_table=..., concept_col=:condition_concept_id)
```
"""
function _setup_domain_query(conn; domain::Symbol, schema::String="main", dialect::Symbol=:postgresql)
    tblsym = domain
    concept_col = _concept_col(tblsym)
    fconn = _funsql(conn; schema=schema, dialect=dialect)
    tbl = _resolve_table(fconn, tblsym)
    concept_table = _resolve_table(fconn, :concept)
    
    return (fconn=fconn, tbl=tbl, concept_table=concept_table, concept_col=concept_col)
end

"""
    _format_number(n) -> String

Formats a number into a human-readable string with appropriate scaling.

This utility function formats numbers using common abbreviations:
- Numbers ≥ 1,000,000 are formatted as "X.XM" (millions)
- Numbers ≥ 1,000 are formatted as "X.XK" (thousands)  
- Numbers < 1,000 are formatted as integers with ties rounded up

# Arguments
- `n` - Number to format

# Returns
- `String` - Formatted number string

# Examples
```julia
_format_number(1234567)  # Returns: "1.2M"
_format_number(5432)     # Returns: "5.4K" 
_format_number(123)      # Returns: "123"
_format_number(0.5)      # Returns: "1"
```
"""
function _format_number(n)
    if n >= 1_000_000
        return "$(round(n/1_000_000, digits=1))M"
    elseif n >= 1_000
        return "$(round(n/1_000, digits=1))K"
    else
        return string(Int(round(n, RoundNearestTiesUp)))
    end
end

"""
    _domain_id_to_table(domain_id::String) -> Symbol

Maps OMOP domain_id strings to their corresponding database table symbols.

This function provides the mapping between OMOP domain classifications and the actual
database tables where those concepts are stored. It includes special handling for
person-related domains and falls back to a naming convention for unknown domains.

# Arguments
- `domain_id` - OMOP domain identifier string (e.g., "Condition", "Drug")

# Returns
- `Symbol` - Database table symbol (e.g., :condition_occurrence, :drug_exposure)

# Examples
```julia
table = _domain_id_to_table("Condition")
# Returns: :condition_occurrence

table = _domain_id_to_table("Gender") 
# Returns: :person

table = _domain_id_to_table("CustomDomain")
# Returns: :customdomain_occurrence
```
"""
function _domain_id_to_table(domain_id::String)
    domain_mapping = Dict(
        "Condition" => :condition_occurrence,
        "Drug" => :drug_exposure,
        "Procedure" => :procedure_occurrence,
        "Measurement" => :measurement,
        "Observation" => :observation,
        "Visit" => :visit_occurrence,
        "Device" => :device_exposure,
        "Specimen" => :specimen,
        "Gender" => :person,
        "Race" => :person,
        "Ethnicity" => :person
    )
    
    return get(domain_mapping, domain_id, Symbol(lowercase(domain_id) * "_occurrence"))
end