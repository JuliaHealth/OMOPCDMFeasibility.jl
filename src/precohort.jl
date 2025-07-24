using DataFrames
using DuckDB
using PrettyTables
using FunSQL:
    FunSQL, Agg, Append, As, Asc, Bind, CrossJoin, Define, Desc, Fun, From, Get, Group, Highlight, Iterate, Join, LeftJoin, Limit, Lit, Order, Partition, Select, Sort, Var, Where, With, WithExternal, render, reflect

include("conceptsets.jl")

function scan_domain_presence(conn; domain::Symbol, concept_set::Vector{<:Integer}, limit::Int=10)
    tbl_name = domain_to_table(domain)
    tables = reflect(conn; schema = "dbt_synthea_dev", dialect = :postgresql)
    table = tables[tbl_name]

    concept_col = domain_to_concept_column(domain)

    q = From(table) |>
        Define(:concept_id => Get(concept_col)) |> 
        Where(Fun.in(Get(:concept_id), map(Lit, concept_set)...)) |>
        Select(Get(:person_id), Get(:concept_id)) |>
        Limit(limit)

    sql = render(tables, q)
    df = DBInterface.execute(conn, sql) |> DataFrame
    
    total_q = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Where(Fun.in(Get(:concept_id), map(Lit, concept_set)...)) |>
        Group() |>
        Select(:total_records => Agg.count())
    
    total_sql = render(tables, total_q)
    summary_df = DBInterface.execute(conn, total_sql) |> DataFrame
    
    unique_patients_q = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Where(Fun.in(Get(:concept_id), map(Lit, concept_set)...)) |>
        Group(Get(:person_id)) |>
        Group() |>
        Select(:unique_patients => Agg.count())
    
    unique_patients_sql = render(tables, unique_patients_q)
    unique_patients_df = DBInterface.execute(conn, unique_patients_sql) |> DataFrame
    
    println("\nDOMAIN PRESENCE SCAN: $(uppercase(string(domain)))\n")
    println("Concept Set: $(concept_set)")
    println("Total Records Found: $(summary_df.total_records[1])")
    println("Unique Patients: $(unique_patients_df.unique_patients[1])")
    println()
    
    pretty_table(df, 
        header=["Person ID", "Concept ID"],
        alignment=[:r, :r],
        crop=:none,
        title="First $(min(limit, nrow(df))) Patient Records with Target Concepts",
        title_alignment=:c)
    
    return nothing
end

function summarize_covariate_availability(conn; domain::Symbol, top_n::Int=20)
    tbl_name = domain_to_table(domain)
    tables = reflect(conn; schema = "dbt_synthea_dev", dialect = :postgresql)
    table = tables[tbl_name]

    concept_col = domain_to_concept_column(domain)

    q = From(table) |>
        Define(:concept_id => Get(concept_col)) |> 
        Group(Get(:concept_id)) |>
        Select(Get(:concept_id), :n_records => Agg.count()) |>
        Order(Get(:n_records) |> Desc()) |>
        Limit(top_n)

    sql = render(tables, q)
    df = DBInterface.execute(conn, sql) |> DataFrame
    
    total_q = From(table) |>
        Group() |>
        Select(:total_records => Agg.count())
    
    unique_concepts_q = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Group(Get(:concept_id)) |>
        Group() |>
        Select(:unique_concepts => Agg.count())
    
    total_sql = render(tables, total_q)
    unique_concepts_sql = render(tables, unique_concepts_q)
    summary_df = DBInterface.execute(conn, total_sql) |> DataFrame
    unique_concepts_df = DBInterface.execute(conn, unique_concepts_sql) |> DataFrame
    
    df.percentage = round.((df.n_records ./ summary_df.total_records[1]) .* 100, digits=2)

    println("\nCOVARIATE AVAILABILITY SUMMARY: $(uppercase(string(domain)))\n")
    println("Total Records in Domain: $(summary_df.total_records[1])")
    println("Unique Concepts: $(unique_concepts_df.unique_concepts[1])")
    println()
    
    pretty_table(df,
        header=["Concept ID", "Record Count", "% of Total"],
        alignment=[:r, :r, :r],
        crop=:none,
        title="Top $(top_n) Most Frequent Concepts in $(uppercase(string(domain)))",
        title_alignment=:c,
        formatters = ft_printf("%.2f", 3))
    
    return df
end
