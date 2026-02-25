# Community Launch Checklist

Use this before public announcements.

## Product and Docs

- [ ] `README.md` quickstart validated end-to-end.
- [ ] `GETTING_STARTED.md` validated on a clean environment.
- [ ] `docs/ARCHITECTURE.md` reflects community runtime only.
- [ ] No references to private-only docs in public docs.

## 5-Minute Onboarding Validation

- [ ] Fresh clone + env setup + startup completes.
- [ ] First successful question asked in <= 5 minutes.
- [ ] UI onboarding flow works (connection -> metadata generation -> first question).
- [ ] CLI onboarding flow works (`datachat onboarding wizard`).

## Reliability Checks

- [ ] `ruff check .` passes.
- [ ] Targeted test run passes (`pytest -q` or selected smoke suites).
- [ ] Basic chat and SQL mode confirmed in UI.
- [ ] Evidence/SQL/Table tabs render correctly for single and multi-question prompts.

## Community Workflow

- [ ] `CONTRIBUTING.md` is current.
- [ ] PR template is current.
- [ ] Labels exist: `needs-private-cherry-pick`, `community-sync`, `scope-change`.

## Release

- [ ] Tag/commit selected for announcement is stable.
- [ ] Announcement links tested (repo, docs, quickstart).
