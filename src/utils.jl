using FunSQL: SQLConnection, reflect, FunSQL, From, Get, Select, Where
using OMOPCommonDataModel
using Serialization
using DataFrames
using DBInterface

const DOMAIN_TABLE = let
    versions  = deserialize(joinpath(@__DIR__, "..", "assets", "version_info"))
    latest    = maximum(keys(versions))
    tables    = versions[latest][:tables]
    Dict{Symbol,Symbol}(Symbol(lowercase(String(t))) => Symbol(lowercase(String(t))) for t in keys(tables))
end

function domain_to_table(domain::Symbol)
    haskey(DOMAIN_TABLE, domain) || throw(ArgumentError("Unknown domain: $domain"))
    return DOMAIN_TABLE[domain]
end

function _concept_col(tblsym::Symbol) 
    Symbol("$(split(String(tblsym), '_')[1])_concept_id")
end

function _funsql(conn; schema::String="dbt_synthea_dev")
    return SQLConnection(conn; catalog = reflect(conn; schema=schema, dialect=:postgresql))
end

function _resolve_table(fconn::SQLConnection, tblsym::Symbol)
    lname = lowercase(String(tblsym))
    for t in values(fconn.catalog.tables)
        if lowercase(String(t.name)) == lname
            return t
        end
    end
    error("table not found: $(tblsym)")
end

function _setup_domain_query(conn; domain::Symbol, schema::String="dbt_synthea_dev")
    tblsym = domain_to_table(domain)
    concept_col = _concept_col(tblsym)
    fconn = _funsql(conn; schema=schema)
    tbl = _resolve_table(fconn, tblsym)
    concept_table = _resolve_table(fconn, :concept)
    
    return (fconn=fconn, tbl=tbl, concept_table=concept_table, concept_col=concept_col)
end

function format_number(n)
    if n >= 1_000_000
        return "$(round(n/1_000_000, digits=1))M"
    elseif n >= 1_000
        return "$(round(n/1_000, digits=1))K"
    else
        return string(Int(round(n)))
    end
end

function _counter_reducer(sub, funcs)
    for fun in funcs
        sub = fun(sub)  
    end
    return sub
end

function get_concept_name(concept_id, conn; schema="dbt_synthea_dev")
    fconn = _funsql(conn; schema=schema)
    concept_table = _resolve_table(fconn, :concept)
    
    query = From(concept_table) |>
            Where(Get.concept_id .== concept_id) |>
            Select(Get.concept_name)
    
    result = DataFrame(query |> FunSQL.render |> (sql -> DBInterface.execute(conn, String(sql))))
    return isempty(result) ? "Unknown" : result.concept_name[1]
end

function get_concepts_by_domain(concept_ids::Vector{<:Integer}, conn; schema="dbt_synthea_dev")
    """Get domain information for concept IDs and group them by domain"""
    fconn = _funsql(conn; schema=schema)
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

function domain_id_to_table(domain_id::String)
    """Map OMOP domain_id to actual table names"""
    domain_mapping = Dict(
        "Condition" => :condition_occurrence,
        "Drug" => :drug_exposure,
        "Procedure" => :procedure_occurrence,
        "Measurement" => :measurement,
        "Observation" => :observation,
        "Visit" => :visit_occurrence,
        "Device" => :device_exposure,
        "Specimen" => :specimen
    )
    
    return get(domain_mapping, domain_id, Symbol(lowercase(domain_id) * "_occurrence"))
end