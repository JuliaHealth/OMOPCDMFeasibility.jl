using DataFrames
using DuckDB
using PrettyTables
using DBInterface
using FunSQL:
    FunSQL, Agg, Append, As, Asc, Bind, CrossJoin, Define, Desc, Fun, From, Get, Group, Highlight, Iterate, Join, LeftJoin, Limit, Lit, Order, Partition, Select, Sort, Var, Where, With, WithExternal, render, reflect

"""
    _counter_reducer(person_ids, covariate_functions)

Apply a sequence of OMOPCDMCohortCreator covariate functions to person IDs.
This follows the pattern used in OMOPCDMMetrics for chaining covariate transformations.

# Arguments
- `person_ids` - Vector of person IDs or intermediate result from previous covariate function
- `covariate_functions` - Vector of OMOPCDMCohortCreator functions (e.g., GetPatientGender, GetPatientAgeGroup)

# Returns
- `DataFrame` - Result after applying all covariate functions in sequence
"""
function _counter_reducer(sub, funcs)
    for fun in funcs
        sub = fun(sub)  
    end
    return sub
end

"""
    scan_patients_with_concepts(
        conn;
        domain::Symbol,
        concept_set::Vector{<:Integer},
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "dbt_synthea_dev"
    )

Scan for patients who have specific medical concepts and optionally apply demographic covariates.

# Arguments
- `conn` - Database connection using DBInterface
- `domain` - Medical domain symbol (e.g., `:condition_occurrence`, `:drug_exposure`, `:procedure_occurrence`)
- `concept_set` - Vector of OMOP concept IDs to search for; must be subtype of `Integer`

# Keyword Arguments
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions to apply (e.g., `GetPatientGender`, `GetPatientAgeGroup`). Default: `Function[]`
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`

# Returns
- `DataFrame` - Patient-level data with columns: `person_id`, `concept_id`, `concept_name`, and any covariate columns from applied functions

# Examples
```julia
# Basic usage - find patients with specific conditions
df = scan_patients_with_concepts(conn; domain=:condition_occurrence, concept_set=[31967, 4059650])

# With demographic covariates
df = scan_patients_with_concepts(
    conn;
    domain=:condition_occurrence, 
    concept_set=[31967, 4059650],
    covariate_funcs=[occ.GetPatientGender, occ.GetPatientAgeGroup]
)
```
"""
function scan_patients_with_concepts(conn; domain::Symbol, concept_set::Vector{<:Integer}, covariate_funcs::AbstractVector{<:Function}=Function[], schema::String="dbt_synthea_dev")
    setup = _setup_domain_query(conn; domain=domain, schema=schema)
    
    base = From(setup.tbl) |> 
           Join(:main_concept => setup.concept_table, Get(setup.concept_col) .== Get.main_concept.concept_id) |>
           Where(Fun.in(Get(setup.concept_col), concept_set...))
    
    q = base |> Select(Get(:person_id), :concept_id => Get(setup.concept_col), :concept_name => Get.main_concept.concept_name)
    base_df = DataFrame(DBInterface.execute(setup.fconn, q))
    
    if isempty(covariate_funcs)
        return base_df
    end
    
    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
    person_ids = unique(base_df.person_id)
    covariate_df = _counter_reducer(person_ids, _funcs)
    result_df = leftjoin(base_df, covariate_df, on=:person_id)
    
    return result_df
end

"""
    analyze_concept_distribution(
        conn;
        domain::Symbol,
        concept_set::Vector{<:Integer} = Int[],
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "dbt_synthea_dev"
    )

Analyze the distribution of medical concepts across patient demographics by aggregating patient counts.

# Arguments
- `conn` - Database connection using DBInterface
- `domain` - Medical domain symbol (e.g., `:condition_occurrence`, `:drug_exposure`)

# Keyword Arguments
- `concept_set` - Vector of OMOP concept IDs to filter by; if empty, includes all concepts in domain. Default: `Int[]`
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions for demographic stratification. Default: `Function[]`
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`

# Returns
- `DataFrame` - Summary statistics with columns for concept information, covariate values, and patient counts (`n`)

# Examples
```julia
# Basic concept summary
df = analyze_concept_distribution(conn; domain=:condition_occurrence, concept_set=[31967, 4059650])

# Demographic breakdown
df = analyze_concept_distribution(
    conn;
    domain=:condition_occurrence,
    concept_set=[31967, 4059650], 
    covariate_funcs=[occ.GetPatientGender, occ.GetPatientAgeGroup]
)
```
"""
function analyze_concept_distribution(conn; domain::Symbol, concept_set::Vector{<:Integer}=Int[], covariate_funcs::AbstractVector{<:Function}=Function[], schema::String="dbt_synthea_dev")
    setup = _setup_domain_query(conn; domain=domain, schema=schema)

    base = From(setup.tbl) |> Join(:main_concept => setup.concept_table, Get(setup.concept_col) .== Get.main_concept.concept_id)
    if !isempty(concept_set)
        base = base |> Where(Fun.in(Get(setup.concept_col), concept_set...))
    end
    
    q = base |> Select(Get(:person_id), :concept_id => Get(setup.concept_col), :concept_name => Get.main_concept.concept_name)
    base_df = DataFrame(DBInterface.execute(setup.fconn, q))
    
    if isempty(covariate_funcs)
        summary_df = combine(groupby(base_df, [:concept_id, :concept_name]), nrow => :n)
        return sort(summary_df, :n, rev=true)
    end
    
    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
    person_ids = unique(base_df.person_id)
    covariate_df = _counter_reducer(person_ids, _funcs)
    
    result_df = leftjoin(base_df, covariate_df, on=:person_id)
    
    group_cols = [col for col in names(result_df) if col != "person_id"]
    summary_df = combine(groupby(result_df, group_cols), nrow => :n)
    
    return sort(summary_df, :n, rev=true)
end

