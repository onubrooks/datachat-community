# Stakeholder SQL walkthrough

**Ask a business question. See the SQL and the source definitions behind it.**

This is a small, runnable companion to [DataChat Community](https://github.com/onubrooks/datachat-community). It illustrates one narrow workflow for stakeholders who need data but do not write SQL: retrieve relevant schema and metric definitions, draft a query, check it against a read-only synthetic database, then show the SQL for review. Query generation does **not** execute the SQL. An optional preview runs only against the bundled demo database.

The framing was inspired by Joseph Machado's [text-to-SQL RAG walkthrough](https://www.startdataengineering.com/post/data-democratize-llm/). The code and synthetic dataset here are original. DataChat Community is the broader product, with a web interface, multiple database connectors and governed execution.

## Try it

Python 3.11+; the offline demo uses only the standard library.

```bash
cd examples/stakeholder_sql
python app.py --list-examples
python app.py "What is net revenue by month?" --demo --preview
python -m unittest -v test_app.py
```

`--demo` uses a **clearly labelled canned SQL draft**, so the walkthrough works without an API key. To generate a new query with an LLM, install this repo's dependencies and set `OPENAI_API_KEY`:

```bash
python app.py "How many orders were placed by region?" --provider openai
```

The LLM provider uses the OpenAI Responses API. Override `OPENAI_MODEL` if you need a different model. The LLM sees only the supplied question and the retrieved **synthetic catalog**, not database rows or credentials. No LLM call is made in demo mode.

The output contains `question`, `mode`, `sql`, `explanation`, `context_used` and `preview` (only when requested). `context_used` lists catalog IDs selected by retrieval; it is evidence of *context supplied*, not proof that the SQL is semantically correct.

## What happens

1. A local catalog describes tables, join keys, grain, business terms and example metrics.
2. A deterministic lexical retriever selects up to three relevant catalog entries. This tiny corpus does not need embeddings; a larger catalog could use semantic retrieval.
3. The LLM drafts **SQLite SQL only** from that context. In offline demo mode the draft comes from fixed examples.
4. The query is compiled against a synthetic read-only SQLite database. An authorizer permits only SELECTs, approved tables, approved columns and a small function set.
5. The stakeholder sees SQL and relevant definitions. Preview is opt-in and limited to 20 rows and a bounded number of SQLite operations.

**The SQLite compilation check catches syntax and unauthorized columns; it cannot prove that a business metric or join is correct.** A domain owner must validate calculations, grain, totals and edge cases before using generated SQL for a decision. Do not point the example at a production database or submit private schema/context to a model without an approved arrangement.

## Examples to discuss with a stakeholder

| Question | Why it is useful | Check before relying on it |
| --- | --- | --- |
| What is net revenue by month? | Surfaces the business definition and the generated aggregation. | Refund treatment, date basis, and order status. |
| Which products have the highest net sales? | Shows a join and an ordered breakdown. | Product grain and refunds allocated to each product. |
| How many orders were placed by region? | Lets a nontechnical user inspect a segment query. | Customer region at purchase time and excluded statuses. |

## Portfolio scope

The sample data is fictional. No employer code, table names, screenshots, rules or records are used. The point of the example is not that an LLM can write SQL; it is that **retrieved business context, explicit SQL, restricted access and a human validation loop** make stakeholder self-service more reviewable.
