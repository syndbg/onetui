---
status: accepted
date: 2026-09-07
---

# ADR-0000: Record architecture decisions

## Decision

Keep architecture decisions in `docs/adr/` as Markdown. Explain the choice, why it was made, the alternatives and its consequences. Include examples where they help.

Keep delivery checklists and test results in issues and pull requests. An accepted decision can still await implementation.

## Context

ADRs keep the reasons for decisions separate from delivery details.

## Convention

- Use `NNNN-short-decision-title.md`, starting with this `0000` record. Assign the next unused number after the highest existing number; never renumber or reuse a record's ID.
- Start with `status` and `date` YAML fields, followed by an `ADR-NNNN` title that names the decision. Dates use `YYYY-MM-DD`.
- Use `proposed` while a decision awaits approval, `accepted` when agreed, `rejected` when declined, and `superseded` when replaced. Status records the decision, not feature progress.
- Lead with the decision. Add only the context, evidence, examples, considered alternatives and consequences needed to understand it. No mandatory implementation sequence or proof checklist.
- Keep rejected approaches and their reasons visible. Distinguish an unsuitable choice for OneTUI from a technically invalid approach.
- Reference only earlier ADRs. Record extensions and replacements in the newer ADR and link back to the earlier decision. Mark superseded choices in the older record without rewriting its original rationale.

## Alternatives and consequences

Decisions left in chat or changing task lists are hard to find later. Markdown ADRs keep them beside the code without a generator or external tracker.
