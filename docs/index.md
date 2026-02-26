# Welcome to HonestRoles Docs

<div class="hr-hero">
  <p class="hr-hero__kicker">Task-First Documentation</p>
  <h1 class="hr-hero__title">Clean, score, and rank job data with confidence.</h1>
  <p class="hr-hero__lead">Use this docs site to get from raw job posting data to validated, ranked opportunities. Start with installation, choose your entry point, and follow the workflows that match your use case.</p>
  <a class="md-button md-button--primary" href="start/installation.md">Install HonestRoles</a>
  <a class="md-button" href="start/quickstart.md">Run Quickstart</a>
  <a class="md-button" href="guides/troubleshooting.md">Troubleshoot Fast</a>
</div>

## Start Here

<div class="hr-card-grid">
  <a class="hr-card" href="start/installation.md">
    <span class="hr-card__title">Installation</span>
    <p class="hr-card__meta">Set up runtime, dev, and docs environments with sanity checks.</p>
  </a>
  <a class="hr-card" href="start/entry_points.md">
    <span class="hr-card__title">Choose Your Entry Point</span>
    <p class="hr-card__meta">Decide between Python API, CLI workflows, or plugin extension paths.</p>
  </a>
  <a class="hr-card" href="start/quickstart.md">
    <span class="hr-card__title">Contract-First Quickstart</span>
    <p class="hr-card__meta">Run the canonical `read -> normalize -> validate -> process -> rank` pipeline.</p>
  </a>
</div>

## Most-Used Workflows

<div class="hr-card-grid">
  <a class="hr-card" href="guides/index.md">
    <span class="hr-card__title">Workflow Overview</span>
    <p class="hr-card__meta">Browse all task-oriented guides from one landing page.</p>
  </a>
  <a class="hr-card" href="guides/cli.md">
    <span class="hr-card__title">CLI Guide</span>
    <p class="hr-card__meta">Use public commands for quality reports and plugin scaffolding.</p>
  </a>
  <a class="hr-card" href="guides/end_to_end_pipeline.md">
    <span class="hr-card__title">End-to-End Pipeline</span>
    <p class="hr-card__meta">Process raw records into scored and ranked outputs with repeatable steps.</p>
  </a>
  <a class="hr-card" href="guides/troubleshooting.md">
    <span class="hr-card__title">Troubleshooting</span>
    <p class="hr-card__meta">Resolve common schema, DuckDB, LLM, and packaging failures quickly.</p>
  </a>
  <a class="hr-card" href="guides/llm_operations.md">
    <span class="hr-card__title">LLM Operations</span>
    <p class="hr-card__meta">Operate local Ollama-backed enrichment paths safely and predictably.</p>
  </a>
  <a class="hr-card" href="guides/output_columns.md">
    <span class="hr-card__title">Output Columns by Stage</span>
    <p class="hr-card__meta">Audit schema additions and row effects by processing stage.</p>
  </a>
</div>

## Deep Reference

- [Architecture](concepts/architecture.md)
- [Framework Boundary](concepts/framework.md)
- [Compatibility and Versioning](concepts/compatibility_and_versioning.md)
- [Source Data Contract v1](reference/source_data_contract_v1.md)
- [Python API Reference](reference/api/reference.md)
- [Plugin Author Guide](reference/plugins/author_guide.md)
- [FAQ](reference/faq.md)

## Maintainer Paths

- [Packaging](maintainers/packaging.md)
- [Release Process](maintainers/release_process.md)
- [Performance Guardrails](maintainers/performance.md)
- [Docs Stack](maintainers/docs_stack.md)
