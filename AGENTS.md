# OneTUI agent instructions

@/Users/syndbg/.codex/RTK.md

## Style

- Always use `$humanizer:humanizer`, `$ponytail:ponytail` and `$caveman`. Read available skills; follow these rules if unavailable.
- Keep replies and docs short. Explain only what the reader needs to act.
- Link to source and `make help` instead of narrating files or repeating command descriptions. Use `onetui schema` for settings reference.
- Keep agent progress notes out of user-facing docs. Avoid repeated test reports and exhaustive option/edge-case descriptions. Comments explain why or risk.

## Development

- Keep `hack/` for local setup and fixtures. Release tooling belongs in `scripts/` or the repository root.
- Read the relevant source and ADRs. Stay in scope; reuse existing code and dependencies.
- Keep implementation and tests in their owning package. No shared loops over datasource test suites; small duplicated fixture helpers are fine.
- Add regression tests for bugs. Preserve configuration, security and error handling; never weaken tests to pass.
- Do not validate datasource queries on the client. Send them to the datasource and show its errors. Do not assume a datasource version or restrict its query syntax to a client-defined subset.
- Update affected usage docs, config examples, CLI help and schema when behavior changes. Document non-obvious limits, not everything.
- Keep lasting decisions and rejected alternatives in ADRs, and user guidance in the public docs. Keep delivery checklists and test reports in issues and pull requests.

## Checks

Do not add tests that inspect GitHub workflow files or assert their configuration. Use `make workflow-lint` for workflow validation.

Use [Makefile](Makefile) targets. Features require passing `make verify`, `make test-integration` and `make workflow-lint`. Docs-only edits need content/link checks and `git diff --check`. Report failed or unrun checks; local success is not hosted CI proof.

Prefix agent shell commands with `rtk`; use `rtk proxy` when needed. Keep user-facing commands plain.

## Safety

- Use code-review-graph first for code exploration/review. Refresh and retry missing results before falling back to `rg` and source reads.
- Preserve existing edits. Ask before Git-state changes. Never run `git commit` or add `Co-Authored-By`.
- Write only to disposable fixtures, never real datasources. Do not bypass reset guards or delete running fixtures without authorization. `make dev-down` deletes fixture data.
- Follow [CONTRIBUTING.md](CONTRIBUTING.md) for SemVer and GitHub UI releases. No version bumps, tags or publishing without authorization.
