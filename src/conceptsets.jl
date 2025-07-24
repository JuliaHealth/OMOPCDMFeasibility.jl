using OMOPCommonDataModel
using Serialization
using InlineStrings

const DOMAIN_TABLE = let
    versions  = deserialize(joinpath(@__DIR__, "..", "assets", "version_info"))
    latest    = maximum(keys(versions))
    tables    = versions[latest][:tables]
    Dict{Symbol,Symbol}(Symbol(lowercase(String(t))) => Symbol(lowercase(String(t))) for t in keys(tables))
end

const DOMAIN_CONCEPT_COLUMN = Dict{Symbol,Symbol}(
    :condition_occurrence => :condition_concept_id,
    :drug_exposure => :drug_concept_id,
    :procedure_occurrence => :procedure_concept_id,
    :measurement => :measurement_concept_id,
    :observation => :observation_concept_id,
    :visit_occurrence => :visit_concept_id,
    :visit_detail => :visit_detail_concept_id,
    :device_exposure => :device_concept_id,
    :death => :cause_concept_id,
    :note => :note_type_concept_id,
    :specimen => :specimen_concept_id,
    :fact_relationship => :domain_concept_id_1,  
    :location => :country_concept_id,
    :care_site => :place_of_service_concept_id,
    :provider => :specialty_concept_id,
    :payer_plan_period => :payer_concept_id,
    :cost => :cost_type_concept_id,
    :drug_era => :drug_concept_id,
    :dose_era => :drug_concept_id,
    :condition_era => :condition_concept_id
)

function domain_to_table(domain::Symbol)
    haskey(DOMAIN_TABLE, domain) || throw(ArgumentError("Unknown domain: $domain"))
    return DOMAIN_TABLE[domain]
end

function domain_to_concept_column(domain::Symbol)
    haskey(DOMAIN_CONCEPT_COLUMN, domain) || throw(ArgumentError("Unknown domain concept column for: $domain"))
    return DOMAIN_CONCEPT_COLUMN[domain]
end
