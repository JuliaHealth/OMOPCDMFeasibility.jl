using OMOPCDMFeasibility
using Documenter

DocMeta.setdocmeta!(
    OMOPCDMFeasibility, :DocTestSetup, :(using OMOPCDMFeasibility); recursive=true
)

makedocs(;
    modules=[OMOPCDMFeasibility],
    checkdocs = :none,
    authors="Kosuri Lakshmi Indu <kosurilindu@gmail.com> and contributors",
    repo = "https://github.com/JuliaHealth/OMOPCDMFeasibility.jl/blob/{commit}{path}#{line}",
    sitename="OMOPCDMFeasibility.jl",
    format=Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical="https://JuliaHealth.github.io/OMOPCDMFeasibility.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
        "Quickstart" => "quickstart.md",

        "Pre-Cohort Analysis" => "precohort.md",
        "Post-Cohort Analysis" => "postcohort.md",
        
        "API" => "api.md",
    ],
    doctest = false,
)

deploydocs(; repo="github.com/JuliaHealth/OMOPCDMFeasibility.jl")
