"""
    analyze_concept_distribution(
        conn;
        concept_set::Vector{<:Integer},
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "main",
        dialect::Symbol = :postgresql
    )

Analyzes the distribution of medical concepts across patient demographics by automatically detecting domains.

# Arguments
- `conn` - Database connection using DBInterface
- `concept_set` - Vector of OMOP concept IDs to analyze; must be subtype of `Integer`

# Keyword Arguments
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions for demographic stratification. Default: `Function[]`
- `schema` - Database schema name. Default: `"main"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `DataFrame` - Summary statistics with columns for concept information, domain, covariate values, and patient counts (`count`)

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
    schema::String="main",
    dialect::Symbol=:postgresql,
)
    isempty(concept_set) && throw(ArgumentError("concept_set cannot be empty"))

    concepts_by_domain = get_concepts_by_domain(concept_set, conn; schema=schema, dialect=dialect)

    if isempty(concepts_by_domain)
        return DataFrame(;
            concept_id=Int[], concept_name=String[], domain=String[], count=Int[]
        )
    end

    all_results = DataFrame()

    for (domain_id, domain_concepts) in concepts_by_domain
        try
            table_symbol = domain_id_to_table(domain_id)

            setup = _setup_domain_query(conn; domain=table_symbol, schema=schema, dialect=dialect)

            base = Where(Fun.in(Get(setup.concept_col), domain_concepts...))(Join(
                :main_concept => setup.concept_table,
                Get(setup.concept_col) .== Get.main_concept.concept_id,
            )(From(setup.tbl)))

            q = Select(
                Get(:person_id),
                :concept_id => Get(setup.concept_col),
                :concept_name => Get.main_concept.concept_name,
            )(base)
            base_df = DataFrame(DBInterface.execute(setup.fconn, q))

            if !isempty(base_df)
                base_df.domain = fill(domain_id, nrow(base_df))

                if isempty(covariate_funcs)
                    summary_df = combine(
                        groupby(base_df, [:concept_id, :concept_name, :domain]),
                        nrow => :count,
                    )
                else
                    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
                    person_ids = unique(base_df.person_id)
                    covariate_df = _counter_reducer(person_ids, _funcs)

                    result_df = leftjoin(base_df, covariate_df; on=:person_id)
                    group_cols = [col for col in names(result_df) if col != "person_id"]
                    summary_df = combine(groupby(result_df, group_cols), nrow => :count)
                end

                all_results = vcat(all_results, summary_df)
            end
        catch e
            @warn "Error processing domain $domain_id: $e"
            continue
        end
    end

    return isempty(all_results) ? all_results : sort(all_results, :count; rev=true)
end

"""
    generate_feasibility_report(
        conn;
        concept_set::Vector{<:Integer},
        covariate_funcs::AbstractVector{<:Function} = Function[],
        schema::String = "main",
        dialect::Symbol = :postgresql
    )

Generates a comprehensive feasibility analysis report with automatic domain detection.

This function provides a complete feasibility assessment including population coverage, 
patient eligibility, data availability across multiple domains, and demographic stratification.

# Arguments
- `conn` - Database connection using DBInterface
- `concept_set` - Vector of OMOP concept IDs to analyze; must be subtype of `Integer`

# Keyword Arguments
- `covariate_funcs` - Vector of OMOPCDMCohortCreator functions for demographic analysis (e.g., `GetPatientGender`, `GetPatientRace`, `GetPatientEthnicity`). Default: `Function[]`
- `schema` - Database schema name. Default: `"main"`
- `dialect` - Database dialect. Default: `:postgresql` (for DuckDB compatibility)

# Returns
- `DataFrame` - Feasibility metrics with columns: `metric`, `value`, `interpretation`, and `domain`

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
    schema::String="main",
    dialect::Symbol=:postgresql,
)
    isempty(concept_set) && throw(ArgumentError("concept_set cannot be empty"))

    concepts_by_domain = get_concepts_by_domain(concept_set, conn; schema=schema, dialect=dialect)

    if isempty(concepts_by_domain)
        return DataFrame(;
            metric=["No Valid Concepts"],
            value=["0"],
            interpretation=["No concepts found in database"],
            domain=["N/A"],
        )
    end

    fconn = _funsql(conn; schema=schema, dialect=dialect)
    person_table = _resolve_table(fconn, :person)
    total_patients_q = Select(:total_patients => Agg.count())(Group()(From(person_table)))
    total_patients = DataFrame(DBInterface.execute(fconn, total_patients_q)).total_patients[1]

    total_records_across_domains = 0
    all_eligible_patients = Set{Int}()
    domain_details = DataFrame()

    for (domain_id, domain_concepts) in concepts_by_domain
        try
            table_symbol = domain_id_to_table(domain_id)
            setup = _setup_domain_query(conn; domain=table_symbol, schema=schema, dialect=dialect)

            concept_records_q = Select(:total_concept_records => Agg.count())(Group()(Where(
                Fun.in(Get(setup.concept_col), domain_concepts...)
            )(From(setup.tbl))))
            domain_records = DataFrame(DBInterface.execute(setup.fconn, concept_records_q)).total_concept_records[1]

            unique_patients_q = Select(Get(:person_id))(Where(
                Fun.in(Get(setup.concept_col), domain_concepts...)
            )(From(setup.tbl)))
            domain_patients_df = DataFrame(
                DBInterface.execute(setup.fconn, unique_patients_q)
            )
            domain_patients = Set(domain_patients_df.person_id)

            total_records_across_domains += domain_records
            union!(all_eligible_patients, domain_patients)

            push!(
                domain_details,
                (
                    domain=domain_id,
                    concepts=length(domain_concepts),
                    patients=length(domain_patients),
                    records=domain_records,
                    concepts_list=join(domain_concepts, ", "),
                ),
            )

        catch e
            @warn "Error processing domain $domain_id: $e"
            continue
        end
    end

    unique_patients_with_concepts = length(all_eligible_patients)
    avg_records_per_patient = if unique_patients_with_concepts > 0
        round(total_records_across_domains / unique_patients_with_concepts; digits=3)
    else
        0.0
    end
    population_coverage = round(
        (unique_patients_with_concepts / total_patients) * 100; digits=3
    )

    summary_report = DataFrame(;
        metric=[
            "Total Patients",
            "Eligible Patients",
            "Total Target Records",
            "Records per Patient",
            "Population Coverage (%)",
            "Domains Analyzed",
        ],
        value=[
            format_number(total_patients),
            format_number(unique_patients_with_concepts),
            format_number(total_records_across_domains),
            string(avg_records_per_patient),
            string(population_coverage) * "%",
            string(length(concepts_by_domain)),
        ],
        interpretation=[
            "Total patients available in the database",
            "Patients who have ANY of your target medical concepts",
            "Number of medical records found across all domains",
            "Average medical records per eligible patient",
            "What percentage of all patients are eligible for your study",
            "Number of different medical domains analyzed",
        ],
        domain=fill("Summary", 6),
    )

    domain_breakdown = DataFrame()
    for row in eachrow(domain_details)
        domain_coverage = round((row.patients / total_patients) * 100; digits=3)
        domain_metrics = DataFrame(;
            metric=[
                "$(row.domain) - Concepts",
                "$(row.domain) - Patients",
                "$(row.domain) - Records",
                "$(row.domain) - Coverage (%)",
            ],
            value=[
                string(row.concepts),
                format_number(row.patients),
                format_number(row.records),
                string(domain_coverage) * "%",
            ],
            interpretation=[
                "Number of concepts analyzed in $(row.domain) domain",
                "Patients with $(row.domain) concepts",
                "Records found in $(row.domain) domain",
                "Population coverage for $(row.domain) domain",
            ],
            domain=fill(row.domain, 4),
        )
        domain_breakdown = vcat(domain_breakdown, domain_metrics)
    end

    return vcat(summary_report, domain_breakdown)
end

