# Global Reference Test Queries

Use these after syncing the folder with global scope:

```bash
datachat dp sync --datapoints-dir datapoints/examples/global_reference --global-scope
```

## Definition-first prompts (should resolve from DataPoints)

- `How is gross margin calculated?`
- `Define loan default rate`
- `What does failed transaction rate mean?`
- `What are the business rules for gross margin?`

## Cross-database consistency prompts

Run each prompt with two different selected databases (for example, grocery and fintech):

- `What is loan default rate?`
- `Define failed transaction rate in plain language`

Expected:

- Answers are consistent across databases because these are global/shared definitions.
- `answer_source` is typically `context` unless the prompt explicitly asks for SQL/querying.

## SQL-intent prompts (should ask for tables or switch to SQL path)

- `Show loan default rate by day`
- `What is gross margin by store this month?`

Expected:

- System should ask clarifying questions or generate SQL only if relevant tables exist in the selected database.
- Global definitions should help interpretation, but should not force fabricated SQL/table names.
