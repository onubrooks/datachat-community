# Handy Reset Commands

Run from repo root (`datachat` or `datachat-community`).

## 1) Clear synced state only (recommended during UI testing)

Clears:
- system DB runtime state
- vector store + knowledge graph

Keeps:
- `~/.datachat/config.json` settings
- local DataPoint files (`datapoints/managed`, `datapoints/user`, `datapoints/examples`, `datapoints/demo`)
- seeded/demo SQL tables

```bash
uv run datachat reset \
  --keep-config \
  --keep-managed-datapoints \
  --keep-user-datapoints \
  --keep-example-datapoints \
  --yes
```

## 2) Clear generated/managed local DataPoint files too

Keeps config and example/demo files.

```bash
uv run datachat reset --keep-config --yes
```

## 3) Full clean start (including config)

Removes config and local managed/user datapoint files.

```bash
uv run datachat reset --yes
```

## 4) Also wipe target database data (safe table clear)

```bash
uv run datachat reset --include-target --yes
```

## 5) Drop all target database tables (destructive)

```bash
uv run datachat reset --include-target --drop-all-target --yes
```

## 6) Clear only saved settings/config

```bash
rm -f ~/.datachat/config.json
```

## 7) Quick verification after reset

```bash
uv run datachat status
```

Expected for a clean state:
- `Vector Store: 0 datapoints`
- `Knowledge Graph: 0 nodes, 0 edges`
