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
- [ ] UI onboarding wizard flow works: connect DB -> generate metadata -> ask first question.
- [ ] CLI onboarding wizard flow works: `datachat onboarding wizard`.

## Reliability Checks

- [ ] `ruff check .` passes.
- [ ] Targeted test run passes (`pytest -q` or selected smoke suites).
- [ ] Frontend deps installed before UI tests (`cd frontend && npm ci`).
- [ ] Basic chat and SQL mode confirmed in UI.
- [ ] Evidence/SQL/Table tabs render correctly for single and multi-question prompts.

## Release Control

- [ ] Freeze docs after final review.
- [ ] Confirm release/announcement links.
- [ ] Publish launch post and collect feedback.
