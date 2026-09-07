---
status: accepted
date: 2026-09-07
---

# ADR-0000: Record architecture decisions

## Decision

Record significant architecture decisions as Markdown ADRs in `docs/adr/`. Keep each record focused on the decision, its context, rationale, alternatives and consequences. Include interface examples when they clarify the contract.

Keep delivery checklists and test results in issues and pull requests. An accepted decision does not mean its implementation is complete.

## Context

Separating durable decisions from changing delivery details preserves the reasons behind the design without turning each ADR into another task tracker.

## Convention

- Use `NNNN-short-decision-title.md`, starting with this `0000` record. Assign the next unused number after the highest existing number; never renumber or reuse a record's ID.
- Start with `status` and `date` YAML fields, followed by an `ADR-NNNN` title that names the decision. Dates use `YYYY-MM-DD`.
- Use `proposed` while a decision awaits approval, `accepted` when agreed, `rejected` when declined, and `superseded` when replaced. Status records the decision, not feature progress.
- Lead with the decision. Add only the context, evidence, examples, considered alternatives and consequences needed to understand it. No mandatory implementation sequence or proof checklist.
- Keep rejected approaches and their reasons visible. Distinguish an unsuitable choice for OneTUI from a technically invalid approach.
- Reference only earlier ADRs. Record extensions and replacements in the newer ADR and link back to the earlier decision. Mark superseded choices in the older record without rewriting its original rationale.

## Alternatives and consequences

Keeping decisions only in chat or changing task lists makes their rationale hard to recover as implementation tasks change. ADRs add a small documentation cost but keep decisions and rejected alternatives discoverable beside the code. No ADR generator, external decision tracker or additional tooling is required.
