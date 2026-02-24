# Community Export Manifest (v0.1.0)

This manifest defines what is included/excluded when promoting from private `datachat` to public `datachat-community`.

## Include Roots

- `backend/`
- `frontend/`
- `config/`
- `scripts/`
- `tests/`
- `datapoints/examples/`
- `prompts/`
- `docs/` (except excluded items below)
- top-level essentials: `README.md`, `GETTING_STARTED.md`, `TESTING.md`, `pyproject.toml`, `uv.lock`, `.env.example`, `docker-compose.yml`, `docker-compose.override.yml`, `main.py`

## Exclude Paths (Private-Only)

### Strategy / planning docs

- `docs/ARCHITECTURE_DYNAMIC_DATA_AGENT.md`
- `docs/GTM_90_DAY_PLAN.md`
- `docs/LEVELS.md`
- `docs/PRD.md`
- `docs/ROADMAP.md`
- `docs/OSS_SPLIT_CHECKLIST.md`

### Internal specs and templates

- `docs/specs/`
- `docs/templates/COMMUNITY_REPO_README_TEMPLATE.md`

### Internal business-facing finance doc

- `docs/finance/FINANCE_WORKFLOW_VALUE_PROOF.md`

### Internal artifacts

- `reports/`
- `eval/`
- `workspace_demo/`

## Export Command Template

```bash
rsync -av --delete \
  --exclude-from=.community-export-ignore \
  /path/to/private/datachat/ \
  /path/to/public/datachat-community/
```

## Promotion Notes

- Keep runtime code public-safe by default.
- Keep proprietary strategy/roadmap artifacts private.
- Validate docs links after every export cut.

