# OneTUI agent instructions

@/Users/syndbg/.codex/RTK.md

## Style

- Always apply `$humanizer:humanizer`, `$ponytail:ponytail` (full) and `$caveman` (full), without waiting for the user to mention them. Read available skills; preserve these rules when unavailable.
- Keep commentary and handoffs brief: changes, checks, blockers. Preserve exact commands and errors; explain more only when requested or needed for safety.
- Write normal, concise prose in code, docs and proposed commit/PR text. Comments explain why or risk, not obvious operations.
- Link to existing sources instead of narrating files or repeating their contents. For commands, point to [Makefile](Makefile) and `make help`; say which targets to run. Skip file tours, implementation diaries and repeated feature lists.

## Development

- Read relevant ADRs and source before changing behavior. Stay within the requested scope.
- Reuse existing code, standard libraries and installed dependencies. Add abstractions or dependencies only for a concrete need. Fix root causes; preserve validation, security and error handling.
- Keep implementation and tests in their owning package. Never combine datasource suites in a shared backend loop. Small duplicated fixture helpers are acceptable; root tests cover CLI orchestration.
- Test changed behavior, failures and boundaries. Add regression tests for bugs. Never weaken assertions or hide failures to pass a gate.
- Keep affected docs, configuration examples, CLI help and schema output current. Document purpose, usage and limits; distinguish supported from planned behavior.
- For settings, document types, values, defaults, required/empty behavior, units/bounds, config location/precedence, path and secret handling, errors and a working example. Record compatibility changes. Prefer `onetui schema` for the complete settings catalog.
- ADRs record decisions and rejected alternatives. Keep delivery checklists in issues and pull requests.

## Commands and validation

Use [Makefile](Makefile): `make help`, `make dev-up`, `make run`, `make dev-traffic`, `make check-local`, `make fmt`, `make build`, `make lint`, `make test`.
Prefix agent shell commands with `rtk`; use `rtk proxy` when needed. User-facing commands need no RTK wrapper.

Before declaring a feature complete, all tests must pass and docs/config must match. Run:

```sh
rtk make verify
rtk make test-integration
rtk make workflow-lint
```

Documentation-only changes need content/link checks and `rtk git diff --check`, unless behavior or executable examples changed. Report failed or unrun gates. Local checks do not prove hosted CI or release validation.

## Workflow and safety

- Use code-review-graph first: `get_minimal_context`, impact/test queries, and `detect_changes` for reviews. Refresh and retry missing/stale results before falling back to `rg` and source reads. If unavailable, state that and use local source.
- Preserve existing edits. Ask before staging, branching, stashing, restoring, rebasing, merging, pushing or other Git-state changes. Never run `git commit` or add `Co-Authored-By`.
- Use disposable fixtures for writes; never real datasource contents or credentials. Do not bypass fixture reset guards or delete running fixtures without authorization. `make dev-down` deletes temporary fixture data.
- Follow Semantic Versioning and [CONTRIBUTING.md](CONTRIBUTING.md). Releases use the GitHub Releases UI. Do not bump versions, create tags or publish without authorization.
