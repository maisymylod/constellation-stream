# Contributing

This repo follows a lightweight issue plus pull-request workflow.

## Workflow
1. Open an issue (bug / enhancement / hardening / test / docs) with context and acceptance criteria.
2. Branch from the default branch: `type/short-description` (e.g. `hardening/validate-cli-inputs`).
3. Open a PR that links the issue with `Closes #N` and fills in the PR template.
4. Review the diff on the PR (note tradeoffs, edge cases, follow-ups) before merging.
5. Merge once CI is green; delete the branch.

## Labels
`bug`, `enhancement`, `hardening`, `test`, `documentation`, `good first issue`.

## Commit and PR style
- Small, focused changes; one logical concern per PR.
- Clear imperative commit subjects; explain the why in the body.
