"""
    DOMAIN_TABLE

A constant dictionary mapping OMOP domain symbols to their corresponding table symbols.
This is constructed from the serialized version information in the assets directory.
Used internally for mapping domains to database tables.
"""
const DOMAIN_TABLE = let
    versions  = deserialize(joinpath(@__DIR__, "..", "assets", "version_info"))
    latest    = maximum(keys(versions))
    tables    = versions[latest][:tables]
    Dict{Symbol,Symbol}(Symbol(lowercase(String(t))) => Symbol(lowercase(String(t))) for t in keys(tables))
end

"""
    _get_concept_name(concept_id, conn; schema="main", dialect=:postgresql) -> String

Retrieves the human-readable name for a given OMOP concept ID.

# Arguments
- `concept_id` - The OMOP concept ID to look up
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
    _domain_to_table(domain::Symbol) -> Symbol

Maps a domain symbol to its corresponding database table symbol using the DOMAIN_TABLE lookup.

# Arguments
- `domain` - The domain symbol to map

# Returns
- `Symbol` - The corresponding table symbol

# Throws
- `ArgumentError` - If the domain is not found in DOMAIN_TABLE

# Examples
```julia
table = _domain_to_table(:condition)
# Returns: :condition_occurrence
```
"""
function _domain_to_table(domain::Symbol)
    haskey(DOMAIN_TABLE, domain) || throw(ArgumentError("Unknown domain: $domain"))
    return DOMAIN_TABLE[domain]
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