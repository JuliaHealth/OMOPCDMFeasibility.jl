using DataFrames
using DBInterface
using FunSQL:
    FunSQL, Agg, Fun, From, Get, Group, Join, LeftJoin, Select, Where

"""
    analyze_concept_distribution(
        conn;
        concept_set::Vector{<:Integer},
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "dbt_synthea_dev"
    )

Analyze the distribution of medical concepts across patient demographics by automatically detecting domains.

# Arguments
- `conn` - Database connection using DBInterface
- `concept_set` - Vector of OMOP concept IDs to analyze; must be subtype of `Integer`

# Keyword Arguments
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions for demographic stratification. Default: `Function[]`
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`

# Returns
- `DataFrame` - Summary statistics with columns for concept information, domain, covariate values, and patient counts (`n`)

# Examples
```julia
# Basic concept summary with automatic domain detection
df = analyze_concept_distribution(conn; concept_set=[31967, 4059650])

# With demographic breakdown
df = analyze_concept_distribution(
    conn;
    concept_set=[31967, 4059650], 
    covariate_funcs=[GetPatientGender, GetPatientAgeGroup]
)
```
"""
function analyze_concept_distribution(
    conn; 
    concept_set::Vector{<:Integer}, 
    covariate_funcs::AbstractVector{<:Function}=Function[], 
    schema::String="dbt_synthea_dev"
    )
    
    # Get concepts grouped by domain
    concepts_by_domain = get_concepts_by_domain(concept_set, conn; schema=schema)
    
    if isempty(concepts_by_domain)
        return DataFrame(concept_id=Int[], concept_name=String[], domain=String[], n=Int[])
    end
    
    all_results = DataFrame()
    
    # Process each domain separately
    for (domain_id, domain_concepts) in concepts_by_domain
        try
            # Convert domain_id to table symbol
            table_symbol = domain_id_to_table(domain_id)
            
            setup = _setup_domain_query(conn; domain=table_symbol, schema=schema)
            
            base = From(setup.tbl) |> 
                   Join(:main_concept => setup.concept_table, Get(setup.concept_col) .== Get.main_concept.concept_id) |>
                   Where(Fun.in(Get(setup.concept_col), domain_concepts...))
            
            q = base |> Select(Get(:person_id), :concept_id => Get(setup.concept_col), :concept_name => Get.main_concept.concept_name)
            base_df = DataFrame(DBInterface.execute(setup.fconn, q))
            
            if !isempty(base_df)
                # Add domain information
                base_df.domain = fill(domain_id, nrow(base_df))
                
                if isempty(covariate_funcs)
                    summary_df = combine(groupby(base_df, [:concept_id, :concept_name, :domain]), nrow => :n)
                else
                    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
                    person_ids = unique(base_df.person_id)
                    covariate_df = _counter_reducer(person_ids, _funcs)
                    
                    result_df = leftjoin(base_df, covariate_df, on=:person_id)
                    group_cols = [col for col in names(result_df) if col != "person_id"]
                    summary_df = combine(groupby(result_df, group_cols), nrow => :n)
                end
                
                all_results = vcat(all_results, summary_df)
            end
        catch e
            @warn "Error processing domain $domain_id: $e"
            continue
        end
    end
    
    return sort(all_results, :n, rev=true)
end

"""
    generate_feasibility_report(
        conn;
        concept_set::Vector{<:Integer},
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "dbt_synthea_dev"
    )

Generate a comprehensive feasibility analysis report with automatic domain detection.

This function provides a complete feasibility assessment including population coverage, 
patient eligibility, data availability across multiple domains, and demographic stratification.

# Arguments
- `conn` - Database connection using DBInterface
- `concept_set` - Vector of OMOP concept IDs to analyze; must be subtype of `Integer`

# Keyword Arguments
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions for demographic analysis (e.g., `GetPatientGender`, `GetPatientRace`, `GetPatientEthnicity`). Default: `Function[]`
- `schema` - Database schema name. Default: `"dbt_synthea_dev"`

# Returns
- `DataFrame` - Feasibility metrics with columns: `metric`, `value`, `interpretation`, and optionally `domain`

# Examples
```julia
# Basic feasibility analysis with automatic domain detection
report = generate_feasibility_report(conn; concept_set=[31967, 4059650])

# With demographic stratification
report = generate_feasibility_report(
    conn;
    concept_set=[31967, 4059650],
    covariate_funcs=[GetPatientGender, GetPatientRace]
)
```
"""
function generate_feasibility_report(
    conn; 
    concept_set::Vector{<:Integer}, 
    covariate_funcs::AbstractVector{<:Function}=Function[], 
    schema::String="dbt_synthea_dev"
    )
    
    # Get concepts grouped by domain
    concepts_by_domain = get_concepts_by_domain(concept_set, conn; schema=schema)
    
    if isempty(concepts_by_domain)
        return DataFrame(
            metric=["No Valid Concepts"], 
            value=["0"], 
            interpretation=["No concepts found in database"],
            domain=["N/A"]
        )
    end
    
    # Get total patients in database
    fconn = _funsql(conn; schema=schema)
    person_table = _resolve_table(fconn, :person)
    total_patients_q = From(person_table) |> Group() |> Select(:total_patients => Agg.count())
    total_patients = DataFrame(DBInterface.execute(fconn, total_patients_q)).total_patients[1]
    
    # Initialize aggregated metrics
    total_records_across_domains = 0
    all_eligible_patients = Set{Int}()
    domain_details = DataFrame()
    
    # Process each domain
    for (domain_id, domain_concepts) in concepts_by_domain
        try
            table_symbol = domain_id_to_table(domain_id)
            setup = _setup_domain_query(conn; domain=table_symbol, schema=schema)
            
            # Get records and patients for this domain
            concept_records_q = From(setup.tbl) |> 
                Where(Fun.in(Get(setup.concept_col), domain_concepts...)) |>
                Group() |>
                Select(:total_concept_records => Agg.count())
            domain_records = DataFrame(DBInterface.execute(setup.fconn, concept_records_q)).total_concept_records[1]
            
            unique_patients_q = From(setup.tbl) |>
                Where(Fun.in(Get(setup.concept_col), domain_concepts...)) |>
                Select(Get(:person_id))
            domain_patients_df = DataFrame(DBInterface.execute(setup.fconn, unique_patients_q))
            domain_patients = Set(domain_patients_df.person_id)
            
            # Accumulate totals
            total_records_across_domains += domain_records
            union!(all_eligible_patients, domain_patients)
            
            # Store domain-specific details
            push!(domain_details, (
                domain = domain_id,
                concepts = length(domain_concepts),
                patients = length(domain_patients),
                records = domain_records,
                concepts_list = join(domain_concepts, ", ")
            ))
            
        catch e
            @warn "Error processing domain $domain_id: $e"
            continue
        end
    end
    
    unique_patients_with_concepts = length(all_eligible_patients)
    avg_records_per_patient = unique_patients_with_concepts > 0 ? round(total_records_across_domains / unique_patients_with_concepts, digits=3) : 0.0
    population_coverage = round((unique_patients_with_concepts / total_patients) * 100, digits=3)
    
    # Create summary report
    summary_report = DataFrame(
        metric = [
            "Total Patients",
            "Eligible Patients", 
            "Total Target Records",
            "Records per Patient",
            "Population Coverage (%)",
            "Domains Analyzed"
        ],
        value = [
            format_number(total_patients),
            format_number(unique_patients_with_concepts),
            format_number(total_records_across_domains),
            string(avg_records_per_patient),
            string(population_coverage) * "%",
            string(length(concepts_by_domain))
        ],
        interpretation = [
            "Total patients available in the database",
            "Patients who have ANY of your target medical concepts",
            "Number of medical records found across all domains", 
            "Average medical records per eligible patient",
            "What percentage of all patients are eligible for your study",
            "Number of different medical domains analyzed"
        ],
        domain = fill("Summary", 6)
    )
    
    # Add domain-specific breakdown
    domain_breakdown = DataFrame()
    for row in eachrow(domain_details)
        domain_coverage = round((row.patients / total_patients) * 100, digits=3)
        domain_metrics = DataFrame(
            metric = [
                "$(row.domain) - Concepts",
                "$(row.domain) - Patients", 
                "$(row.domain) - Records",
                "$(row.domain) - Coverage (%)"
            ],
            value = [
                string(row.concepts),
                format_number(row.patients),
                format_number(row.records),
                string(domain_coverage) * "%"
            ],
            interpretation = [
                "Number of concepts analyzed in $(row.domain) domain",
                "Patients with $(row.domain) concepts",
                "Records found in $(row.domain) domain",
                "Population coverage for $(row.domain) domain"
            ],
            domain = fill(row.domain, 4)
        )
        domain_breakdown = vcat(domain_breakdown, domain_metrics)
    end
    
    return vcat(summary_report, domain_breakdown)
end

