# OneTUI agent instructions

## Communication

- Always use `$caveman` in full mode for agent commentary and handoffs. Read the skill when available; otherwise use terse, precise language.
- Preserve technical detail, paths, commands, and actionable errors. Use complete explanations when brevity would obscure safety or correctness.
- Keep source code, documentation, and proposed commit/PR text clear and conventional; Caveman applies to conversation.

## Feature completion

- Read the relevant documentation and implementation before changing behavior. Keep work within the requested scope.
- A feature is complete only when its implementation, tests, documentation, and configuration examples agree and all tests pass.
- Add or update tests for changed behavior, including relevant error paths and boundary cases. Bug fixes need a regression test.
- Before declaring feature work complete, run these gates from the repository root:

  ```sh
  rtk make verify
  rtk make test-integration
  rtk make workflow-lint
  ```

- `make verify` covers build, formatting, lint, and default tests. Default Cargo tests skip the ignored Docker fixture tests; `make test-integration` is also required.
- Never disable tests, weaken assertions, or hide failing exit codes to pass a gate. Failed or unrun required checks mean the feature remains incomplete; report the blocker.
- Integration fixtures may refuse to run while existing containers are present. Do not bypass that protection or delete running fixtures without permission.
- Distinguish local validation from hosted CI and live-system evidence. Do not claim CI passed without checking its result.
- Documentation-only edits require content/link checks and `rtk git diff --check`; full feature gates are unnecessary unless behavior or executable examples changed.

## Documentation and configuration

- Update affected documentation and configuration examples in the same change as the feature. Review `README.md`, `hack/README.md`, and relevant `docs/` files; change only those affected.
- Explain the feature's purpose, how to use it, limitations, and at least one copy-ready example. Clearly separate implemented behavior from planned support.
- For every added or changed configuration key, CLI option, or environment variable, document:
  - Purpose and when it is needed.
  - Type, accepted values, formats, units, and bounds where applicable.
  - Default behavior, whether it is required, and what omission or an empty value means.
  - Configuration file location, discovery order, override precedence, and relative-path resolution where applicable.
  - Environment-variable expansion or secret lookup behavior, validation failures, and a working usage example.
- Keep parser validation, CLI help, checked-in examples, and any implemented schema output consistent. Do not document unsupported keys or imply planned schema generation already exists.
- Document compatibility changes and migration steps when behavior changes. Use placeholders or disposable fixture credentials, never real secrets.
- If a feature has no configuration impact, say so in the handoff; do not invent settings or make unrelated documentation edits.

## Repository workflow and safety

- Keep implementation and tests in the package that owns the behavior. PostgreSQL and Qdrant have separate connector packages and test suites; never parameterize one test or harness over both backends. Duplicate small setup/assertion helpers when needed for isolation. Root tests cover CLI orchestration only.
- Follow `@/Users/syndbg/.codex/RTK.md`. Prefix shell commands with `rtk`; use `rtk proxy` when no dedicated wrapper applies.
- For code exploration, use the code-review-graph tools first when available: start with `get_minimal_context`, inspect impact and test coverage for changes, and use `detect_changes` for reviews. When graph coverage is missing or stale, refresh and retry before falling back to `rg` and focused file reads. If tools are unavailable, state that limitation and use local source.
- Preserve unrelated and pre-existing work. Ask before staging, branching, stashing, restoring, rebasing, merging, pushing, or other Git-state mutations.
- Never run `git commit` on the user's behalf. Never add `Co-Authored-By` to commit messages.
- Do not mutate real datasource contents or expose credentials during development or verification. Use the disposable local fixtures for write-dependent tests.
- Keep changes minimal and reuse existing patterns. Add dependencies or abstractions only for a concrete requirement.
- Follow Semantic Versioning. Releases are published through the GitHub Releases UI; do not create tags, publish releases, or bump versions without authorization.

## Handoff

- State what changed, where usage/configuration is documented, and which checks passed, failed, or were not run.
- Call out remaining blockers explicitly. Never label unfinished or unverified feature work complete.
