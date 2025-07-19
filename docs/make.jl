using OMOPCDMFeasibility
using Documenter

DocMeta.setdocmeta!(OMOPCDMFeasibility, :DocTestSetup, :(using OMOPCDMFeasibility); recursive=true)

makedocs(;
    modules=[OMOPCDMFeasibility],
    authors="Kosuri Lakshmi Indu <kosurilindu@gmail.com> and contributors",
    sitename="OMOPCDMFeasibility.jl",
    format=Documenter.HTML(;
        canonical="https://JuliaHealth.github.io/OMOPCDMFeasibility.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/JuliaHealth/OMOPCDMFeasibility.jl",
    devbranch="master",
)
