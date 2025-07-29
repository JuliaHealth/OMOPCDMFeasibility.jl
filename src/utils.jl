using FunSQL: SQLConnection, reflect
using OMOPCommonDataModel
using Serialization

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