# AGENTS

## Default workflow

Use the repo in this order:

1. Write or update tests first.
2. Implement the code.
3. Verify the relevant tests are green before considering the work done.

## Pre-commit checks

Run the same categories of checks that CI enforces before committing.

> Note: Do not run `pnpm`/Web UI checks before committing; they are not necessary for local pre-commit validation.

| Category | Commands |
| --- | --- |
| Formatter | `cargo fmt -- --check` |
| Linter | `cargo clippy --all-features --all-targets --workspace -- -D warnings` |
| Compiler | `cargo build --all-features` |
| Tests | `cargo nextest run -E 'kind(lib)' --all-features` |
