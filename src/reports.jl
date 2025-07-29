include("precohort.jl")
include("utils.jl")

function _execute_query(conn, tables, query)
    sql = render(tables, query)
    return DBInterface.execute(conn, sql) |> DataFrame
end

function _get_table_reflection(conn, domain)
    tbl_name = domain_to_table(domain)
    tables = reflect(conn; schema = "dbt_synthea_dev", dialect = :postgresql)
    table = tables[tbl_name]
    concept_col = domain_to_concept_column(domain)
    return tables, table, concept_col
end

function lookup_concept(conn; concept_id::Int)
    tables = reflect(conn; schema = "dbt_synthea_dev", dialect = :postgresql)
    concept_table = tables[:concept]
    
    query = From(concept_table) |>
        Where(Get(:concept_id) .== concept_id) |>
        Select(Get(:concept_id), Get(:concept_name), Get(:domain_id), Get(:vocabulary_id), Get(:concept_class_id))
    
    df = _execute_query(conn, tables, query)
    
    if nrow(df) == 0
        println("No concept found with ID: $concept_id")
        return nothing
    end
    
    println("\nCONCEPT LOOKUP RESULTS\n")
    println("Search: ID: $concept_id")
    println("Found $(nrow(df)) concept(s):")
    println()
    
    pretty_table(df,
        header=["Concept ID", "Concept Name", "Domain", "Vocabulary", "Class"],
        alignment=[:r, :l, :l, :l, :l],
        crop=:none,
        title="Concept Search Results",
        title_alignment=:c)
    
    return df
end

function scan_domain_presence_report!(conn; domain::Symbol, concept_set::Vector{<:Integer}, limit::Int=10)
    println("SCAN: Finding patients who have specific medical concepts")
    println("TARGET: Domain = $(uppercase(string(domain))), Concept IDs = $(concept_set)")
    println()
    
    tables, table, concept_col = _get_table_reflection(conn, domain)

    concept_table = tables[:concept]
    query = From(table) |>
        Define(:concept_id => Get(concept_col)) |> 
        Where(Fun.in(Get(:concept_id), map(Lit, concept_set)...)) |>
        Join(:concept => concept_table, Get(:concept_id) .== Get.concept.concept_id) |>
        Select(Get(:person_id), Get(:concept_id), :concept_name => Get.concept.concept_name) |>
        Limit(limit)

    df = _execute_query(conn, tables, query)
    
    total_query = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Where(Fun.in(Get(:concept_id), map(Lit, concept_set)...)) |>
        Group() |>
        Select(:total_records => Agg.count())
    
    summary_df = _execute_query(conn, tables, total_query)
    
    unique_patients_query = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Where(Fun.in(Get(:concept_id), map(Lit, concept_set)...)) |>
        Group(Get(:person_id)) |>
        Group() |>
        Select(:unique_patients => Agg.count())
    
    unique_patients_df = _execute_query(conn, tables, unique_patients_query)
    
    total_records = summary_df.total_records[1]
    unique_patients = unique_patients_df.unique_patients[1]
    records_per_patient = total_records > 0 ? round(total_records / unique_patients, digits=2) : 0.0
    
    println("SCAN RESULTS FOR CONCEPTS $(concept_set) IN $(uppercase(string(domain))):")
    println("-" ^ 60)
    println("Total Medical Records: $(format_number(total_records)) records contain these concepts")
    println("Different Patients: $(format_number(unique_patients)) patients have these concepts")
    println("Average per Patient: $(records_per_patient) times per patient on average")
    if unique_patients > 0
        println("Population Coverage: $(round((unique_patients / _get_total_patients(conn, tables)) * 100, digits=2))% of all patients have these concepts")
    end
    println()
    
    if nrow(df) > 0
        println("SAMPLE PATIENT DATA (First $(min(limit, nrow(df))) records):")
        pretty_table(df, 
            header=["Patient ID", "Concept ID", "Medical Concept Name"],
            alignment=[:r, :r, :l],
            crop=:none,
            title="Which Patients Have These Medical Concepts",
            title_alignment=:c)
    else
        println("NO PATIENTS FOUND: No patients have these concept IDs in their medical records")
    end
    
    return nothing
end

function _get_total_patients(conn, tables)
    person_table = tables[:person]
    query = From(person_table) |> Group() |> Select(:total => Agg.count())
    result = _execute_query(conn, tables, query)
    return result.total[1]
end

function summarize_domain_availability_report!(conn; domain::Symbol, top_n::Int=10)
    println("SUMMARY: Analyzing most common medical concepts in database")
    println("DOMAIN: $(uppercase(string(domain))) (medical $(lowercase(string(domain))))")
    println()
    
    tables, table, concept_col = _get_table_reflection(conn, domain)

    concept_table = tables[:concept]
    query = From(table) |>
        Define(:concept_id => Get(concept_col)) |> 
        Group(Get(:concept_id)) |>
        Select(Get(:concept_id), :n_records => Agg.count()) |>
        Join(:concept => concept_table, Get(:concept_id) .== Get.concept.concept_id) |>
        Select(Get(:concept_id), :concept_name => Get.concept.concept_name, Get(:n_records)) |>
        Order(Get(:n_records) |> Desc()) |>
        Limit(top_n)

    df = _execute_query(conn, tables, query)
    
    total_query = From(table) |>
        Group() |>
        Select(:total_records => Agg.count())
    
    unique_concepts_query = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Group(Get(:concept_id)) |>
        Group() |>
        Select(:unique_concepts => Agg.count())
    
    summary_df = _execute_query(conn, tables, total_query)
    unique_concepts_df = _execute_query(conn, tables, unique_concepts_query)
    
    total_records = summary_df.total_records[1]
    unique_concepts = unique_concepts_df.unique_concepts[1]
    
    df.percentage = round.((df.n_records ./ total_records) .* 100, digits=2)
    
    total_patients = _get_total_patients(conn, tables)
    coverage_query = From(table) |>
        Define(:concept_id => Get(concept_col)) |>
        Group(Get(:person_id)) |>
        Group() |>
        Select(:patients_with_data => Agg.count())
    
    coverage_df = _execute_query(conn, tables, coverage_query)
    patients_with_data = coverage_df.patients_with_data[1]
    domain_coverage = round((patients_with_data / total_patients) * 100, digits=2)

    println("DATABASE OVERVIEW FOR $(uppercase(string(domain))):")
    println("-" ^ 60)
    println("Total Medical Records: $(format_number(total_records)) records in this domain")
    println("Different Medical Concepts: $(format_number(unique_concepts)) unique concepts exist")
    println("Patients with Data: $(format_number(patients_with_data)) patients ($(domain_coverage)% of all patients)")
    println("Average per Patient: $(round(total_records / patients_with_data, digits=2)) records per patient")
    println("Data Density: $(round(unique_concepts / patients_with_data, digits=5)) concepts per patient")
    println()
    println("MOST COMMON CONCEPTS (Top $(top_n) by frequency):")
    
    pretty_table(df,
        header=["Concept ID", "Medical Concept Name", "How Many Times", "Percentage"],
        alignment=[:r, :l, :r, :r],
        crop=:none,
        title="What Medical Concepts Appear Most Often",
        title_alignment=:c,
        formatters = ft_printf("%.2f", 4))
    
    return nothing
end

function format_number(n)
    if n >= 1_000_000
        return "$(round(n/1_000_000, digits=1))M"
    elseif n >= 1_000
        return "$(round(n/1_000, digits=1))K"
    else
        return string(n)
    end
end
