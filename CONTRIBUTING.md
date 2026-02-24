# Contributing to DataChat Community

Thanks for contributing.

## Workflow

1. Fork or branch from `main`.
2. Keep changes scoped and testable.
3. Open a PR with clear validation steps.
4. Add the `needs-private-cherry-pick` label if this should be pulled into the private repo.

## PR Expectations

- Include a short summary of behavior changes.
- Include local validation commands you ran.
- Update docs when user-visible behavior changes.
- Do not include secrets, local data snapshots, or environment files.

## Upstream Sync Policy (Community -> Private)

This project uses a dual-repo model:

- `datachat-community` is public.
- `datachat` is private and remains the source for private planning and premium layers.

When a community PR is merged and should be carried upstream, maintainers cherry-pick the merge commit(s) into the private repo. Use the `needs-private-cherry-pick` label to flag this.

## Scope Notes

Current community scope is locked in:

- `docs/COMMUNITY_V0_1_SCOPE_LOCK.md`
- `docs/COMMUNITY_EXPORT_MANIFEST.md`

If your change expands scope, call it out explicitly in the PR.
