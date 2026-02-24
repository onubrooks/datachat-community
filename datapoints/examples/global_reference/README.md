# Global Reference DataPoints

These DataPoints are intentionally **global/shared** and not bound to one database.

Use them for cross-source definition questions such as:

- "How is gross margin calculated?"
- "What is loan default rate?"
- "What does failed transaction rate mean?"
- Additional prompts: see `TEST_QUERIES.md`

Load only this folder as global scope:

```bash
datachat dp sync --datapoints-dir datapoints/examples/global_reference --global-scope
```

Or in UI Database Manager, set Sync scope to `global` and run Sync.
