# Quickstart 🎉

Welcome to the Quickstart guide for OMOPCDMFeasibility.jl! This guide shows you how to set up your Julia environment and use OMOPCDMFeasibility.jl for pre- and post-cohort analysis-after you have created a cohort using the recommended observational window template workflow.

## 1. Getting Started

### Launch Julia and Enter Your Project Environment

To get started:

1. **Open your terminal or Julia REPL.**
2. **Navigate to your project folder (where `Project.toml` is located):**

```sh
cd path/to/your/project
```

3. **Activate the project:**

```sh
julia --project=.
```

4. **(Optional for docs) For working on documentation:**

```sh
julia --project=docs
```

## 2. Create Your Cohort with the Observation Window Template

> For a robust, reproducible template for observational study setup and cohort creation, follow the official workflow :
>
> **[Observational Template Workflow](https://juliahealth.org/HealthBase.jl/dev/observational_template_workflow/#2.-Download-OHDSI-Cohort-Definitions)**
>
> **1.** Go through steps 2–5 in the workflow to define and create your cohort table in your database.
>
> **2.** Once your cohort is created, return here to analyze it with OMOPCDMFeasibility.jl.

## 3. Pre-Cohort Analysis

Explore your data before defining a cohort.

**Pre-Cohort Analysis:**

- Pre-cohort functions (like `analyze_concept_distribution`) do **not** accept a cohort table or DataFrame. They always analyze the full database, but you can optionally stratify by covariates (e.g., gender, race, age group) using the `covariate_funcs` argument.
- The `covariate_funcs` argument is optional-include it if you want to stratify by covariates.

To use covariate getter functions, import them from [OMOPCDMCohortCreator.jl](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/blob/dev/src/getters.jl):

```julia
using OMOPCDMCohortCreator: GetPatientGender, GetPatientRace, GetPatientAgeGroup
```

For more advanced understanding and options, see [Pre-Cohort Analysis](precohort.md).

```julia
# Check how common specific OMOP concepts are in your database
# 201826 = "Hypertension", 3004249 = "Metformin"
analyze_concept_distribution(conn; concept_set=[201826, 3004249], schema="main")

# Example with covariate_funcs (optional)
analyze_concept_distribution(
   conn;
   concept_set=[201826, 3004249],
   covariate_funcs=[GetPatientGender, GetPatientRace],
   schema="main"
)

# Get summary statistics for a set of concepts
generate_summary(conn; concept_set=[201826, 3004249], schema="main")

# See which OMOP domains your concepts belong to
generate_domain_breakdown(conn; concept_set=[201826, 3004249], schema="main")
```

## 4. Post-Cohort Analysis

After extracting your cohort, you can perform post-cohort analyses as shown below.

**Post-Cohort Analysis:**

- Post-cohort functions (like `create_individual_profiles`) require you to provide either:
  - `cohort_definition_id` (to use a cohort table in the database), or
  - `cohort_df` (a DataFrame of person IDs).
- You should provide only one of them.
- The `covariate_funcs` argument is optional-include it if you want to stratify by covariates (e.g., gender, race, age group).

To use covariate getter functions, import them from [OMOPCDMCohortCreator.jl](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/blob/dev/src/getters.jl):

```julia
using OMOPCDMCohortCreator: GetPatientGender, GetPatientRace, GetPatientAgeGroup
```

For more advanced understanding and options, see [Post-Cohort Analysis](postcohort.md).

```julia
# Example: Using a DataFrame
create_individual_profiles(
   cohort_df = sample_cohort,
   conn = conn,
   # covariate_funcs = [GetPatientGender, GetPatientRace], # optional
   schema = "main"
)

# Example: Using a cohort_definition_id
create_individual_profiles(
   cohort_definition_id = 1,
   conn = conn,
   # covariate_funcs = [GetPatientGender, GetPatientRace], # optional
   schema = "main"
)
```

Happy experimenting with OMOPCDMFeasibility.jl! 🎉
