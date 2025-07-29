using DataFrames
using DBInterface
using FunSQL:
    FunSQL, Agg, Fun, From, Get, Group, Join, LeftJoin, Select, Where

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

"""
    generate_feasibility_report(
        conn;
        domain::Symbol,
        concept_set::Vector{<:Integer},
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "dbt_synthea_dev"
    )

Generate a comprehensive feasibility analysis report with key metrics for study planning.

This function provides a complete feasibility assessment including population coverage, 
patient eligibility, data availability, and demographic stratification.

# Arguments
- `conn` - Database connection using DBInterface
- `domain` - Medical domain symbol (e.g., `:condition_occurrence`, `:drug_exposure`, `:procedure_occurrence`)
- `concept_set` - Vector of OMOP concept IDs to analyze; must be subtype of `Integer`

# Keyword Arguments
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions for demographic analysis (e.g., `GetPatientGender`, `GetPatientRace`, `GetPatientEthnicity`). Default: `Function[]`
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`

# Returns
- `DataFrame` - Feasibility metrics with columns: `metric`, `value`, `interpretation`

# Examples
```julia
# Basic feasibility analysis
report = generate_feasibility_report(
    conn; 
    domain=:condition_occurrence, 
    concept_set=[31967, 4059650]
)

# With demographic stratification
report = generate_feasibility_report(
    conn;
    domain=:condition_occurrence,
    concept_set=[31967, 4059650],
    covariate_funcs=[GetPatientGender, GetPatientRace]
)
```
"""
function generate_feasibility_report(conn; domain::Symbol, concept_set::Vector{<:Integer}, covariate_funcs::AbstractVector{<:Function}=Function[], schema::String="dbt_synthea_dev")
    setup = _setup_domain_query(conn; domain=domain, schema=schema)
    
    person_table = _resolve_table(setup.fconn, :person)
    total_patients_q = From(person_table) |> Group() |> Select(:total_patients => Agg.count())
    total_patients = DataFrame(DBInterface.execute(setup.fconn, total_patients_q)).total_patients[1]
    
    concept_records_q = From(setup.tbl) |> 
        Where(Fun.in(Get(setup.concept_col), concept_set...)) |>
        Group() |>
        Select(:total_concept_records => Agg.count())
    total_concept_records = DataFrame(DBInterface.execute(setup.fconn, concept_records_q)).total_concept_records[1]
    
    unique_patients_q = From(setup.tbl) |>
        Where(Fun.in(Get(setup.concept_col), concept_set...)) |>
        Group(Get(:person_id)) |>
        Group() |>
        Select(:unique_patients_with_concepts => Agg.count())
    unique_patients_with_concepts = DataFrame(DBInterface.execute(setup.fconn, unique_patients_q)).unique_patients_with_concepts[1]
    
    avg_records_per_patient = unique_patients_with_concepts > 0 ? round(total_concept_records / unique_patients_with_concepts, digits=3) : 0.0
    population_coverage = round((unique_patients_with_concepts / total_patients) * 100, digits=3)
    
    return DataFrame(
        metric = [
            "Total Patients",
            "Eligible Patients", 
            "Total Target Records",
            "Records per Patient",
            "Population Coverage (%)"
        ],
        value = [
            format_number(total_patients),
            format_number(unique_patients_with_concepts),
            format_number(total_concept_records),
            string(avg_records_per_patient),
            string(population_coverage) * "%"
        ],
        interpretation = [
            "Total patients available in the database",
            "Patients who have your target medical conditions",
            "Number of medical records found for target conditions", 
            "Average medical records per eligible patient",
            "What percentage of all patients are eligible for your study"
        ]
    )
end

