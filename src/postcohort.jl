using DataFrames
using DBInterface
using FunSQL:
    FunSQL, Agg, Fun, From, Get, Group, Join, LeftJoin, Select, Where
using Dates
using Statistics

include("utils.jl")

function create_individual_profiles(;
    cohort_definition_id::Union{Int, Nothing} = nothing,
    cohort_df::Union{DataFrame, Nothing} = nothing,
    conn,
    covariate_funcs::AbstractVector{<:Function},
    schema::String = "dbt_synthea_dev"
)
    person_ids = _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema=schema)
    cohort_size = length(person_ids)
    
    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
    demographics_df = _counter_reducer(person_ids, _funcs)
    
    result_tables = Dict{Symbol, DataFrame}()
    
    for col in names(demographics_df)
        if col != "person_id"
            covariate_stats = _create_profile_table(demographics_df, col, cohort_size, conn; schema=schema)
            covariate_name = Symbol(replace(string(col), "_concept_id" => ""))
            result_tables[covariate_name] = covariate_stats
        end
    end
    
    summary_df = DataFrame(
        metric = ["Cohort Size"],
        count = [cohort_size],
        percentage = [100.0]
    )
    result_tables[:summary] = summary_df
    
    return NamedTuple(result_tables)
end

function create_cartesian_profiles(;
    cohort_definition_id::Union{Int, Nothing} = nothing,
    cohort_df::Union{DataFrame, Nothing} = nothing,
    conn,
    covariate_funcs::AbstractVector{<:Function},
    schema::String = "dbt_synthea_dev"
)
    person_ids = _get_cohort_person_ids(cohort_definition_id, cohort_df, conn; schema=schema)
    cohort_size = length(person_ids)
    
    _funcs = [Base.Fix2(fun, conn) for fun in covariate_funcs]
    demographics_df = _counter_reducer(person_ids, _funcs)
    
    result_tables = Dict{Symbol, DataFrame}()
    
    demographic_cols = names(demographics_df)[names(demographics_df) .!= "person_id"]
    
    for i in 1:length(demographic_cols)
        for j in (i+1):length(demographic_cols)
            combo = [demographic_cols[i], demographic_cols[j]]
            combo_stats = _create_combined_profile_table(demographics_df, combo, cohort_size, conn; schema=schema)
            
            combo_name = Symbol(join([replace(string(c), "_concept_id" => "") for c in combo], "_"))
            result_tables[combo_name] = combo_stats
        end
    end
    
    summary_df = DataFrame(
        metric = ["Cohort Size"],
        count = [cohort_size],
        percentage = [100.0]
    )
    result_tables[:summary] = summary_df
    
    return NamedTuple(result_tables)
end

