# AGENTS

## Default workflow

Use the repo in this order:

1. Write or update tests first.
2. Implement the code.
3. Verify the relevant tests are green before considering the work done.

## Pre-commit checks

Run the same categories of checks that CI enforces before committing.

| Category | Commands |
| --- | --- |
| Formatter | `cargo fmt -- --check` |
| Linter | `cargo clippy --all-features --all-targets --workspace -- -D warnings` |
| Compiler | `cd webui && pnpm build`<br>`cargo build --all-features` |
| Static checks | `cd webui && pnpm check` |
| Tests | `cargo nextest run -E 'kind(lib)' --all-features` |

