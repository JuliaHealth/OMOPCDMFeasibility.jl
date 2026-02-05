using OMOPCDMFeasibility
using Documenter
using DocumenterVitepress

DocMeta.setdocmeta!(
    OMOPCDMFeasibility,
    :DocTestSetup,
    :(using OMOPCDMFeasibility);
    recursive = true
)

makedocs(;
    modules = [OMOPCDMFeasibility],
    repo = Remotes.GitHub("JuliaHealth", "OMOPCDMFeasibility.jl"),
    authors = "Kosuri Lakshmi Indu <kosurilindu@gmail.com>, and contributors",
    sitename = "OMOPCDMFeasibility.jl",
    format = DocumenterVitepress.MarkdownVitepress(
        repo = "github.com/JuliaHealth/OMOPCDMFeasibility.jl",
        devurl = "dev",
        devbranch = "master",
        deploy_url = "https://juliahealth.github.io/OMOPCDMFeasibility.jl",
    ),
    pages = [
        "Home" => "index.md",
        "About" => "about.md",
        "Quickstart" => "quickstart.md",
        "Pre-Cohort Analysis" => "precohort.md",
        "Post-Cohort Analysis" => "postcohort.md",
        "API" => "api.md",
    ],
    checkdocs = :none,
    doctest = false,
    warnonly = true,
)

DocumenterVitepress.deploydocs(;
    repo = "github.com/JuliaHealth/OMOPCDMFeasibility.jl",
    target = "build",      
    devbranch = "master",
    branch = "gh-pages",
    push_preview = true,
)
