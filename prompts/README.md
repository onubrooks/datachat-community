# DataChat Prompts

This directory contains all prompt templates for DataChat's LLM interactions.

## Directory Structure

```
prompts/
├── system/           # Core system prompts
│   └── main.md      # Primary system prompt
├── agents/           # Agent-specific prompts
│   ├── sql_generator.md
│   ├── validator.md
│   ├── classifier.md
│   ├── context_answer.md
│   ├── executor_summary.md
│   └── sql_correction.md
└── templates/        # Reusable components (future)
```

## Quick Start

### Loading Prompts in Code

```python
from datachat.backend.prompts.loader import PromptLoader

# Initialize loader
prompts = PromptLoader("prompts/")

# Load system prompt
system_prompt = prompts.load("system/main.md")

# Load agent prompt with variables
sql_prompt = prompts.render(
    "agents/sql_generator.md",
    user_query="What was revenue last month?",
    schema=schema_json,
    datapoints=datapoint_json
)
```

## Prompt Files

### system/main.md
**Purpose:** Primary system prompt for all DataChat interactions  
**When to use:** Every LLM call  
**Key responsibilities:**
- Define DataChat's identity and capabilities
- Set security constraints
- Establish response format
- Define confidence scoring

### agents/sql_generator.md
**Purpose:** SQL generation from natural language  
**When to use:** Converting user questions to SQL  
**Key responsibilities:**
- Parse user intent
- Map to schema
- Apply DataPoint business logic
- Generate backend-specific SQL

### agents/classifier.md
**Purpose:** Intent classification and entity extraction  
**When to use:** Route user queries and extract entities  
**Key responsibilities:**
- Intent classification
- Entity extraction
- Complexity assessment
- Clarification detection

### agents/executor_summary.md
**Purpose:** Summarize query results  
**When to use:** After query execution  
**Key responsibilities:**
- Produce concise natural language answer
- Highlight key insights

### agents/sql_correction.md
**Purpose:** Fix SQL based on validation issues  
**When to use:** After validator reports errors  
**Key responsibilities:**
- Correct SQL without changing intent
- Use only schema context
- Return JSON output with corrections

### agents/validator.md
**Purpose:** Query validation before execution  
**When to use:** Before executing any generated SQL  
**Key responsibilities:**
- Syntax validation
- Safety checks (no SQL injection)
- Schema validation
- Performance checks

## Versioning

All prompts use semantic versioning (MAJOR.MINOR.PATCH):

```markdown
---
version: 1.2.0
last_updated: 2026-01-30
changelog:
  - version: 1.2.0
    date: 2026-01-30
    changes: Added confidence scoring examples
---
```

When updating prompts:
1. Increment version number
2. Add changelog entry
3. Test thoroughly
4. Consider A/B testing for critical changes

## Best Practices

### DO:
✅ Keep prompts in version control  
✅ Test prompt changes before deploying  
✅ Use clear, specific instructions  
✅ Include concrete examples  
✅ Log prompt versions in production  

### DON'T:
❌ Hardcode prompts in Python code  
❌ Deploy untested prompt changes  
❌ Use vague or ambiguous language  
❌ Forget to update version numbers  
❌ Skip changelog entries  

## Testing

Test prompts with:

```bash
# Run prompt regression tests
pytest tests/prompts/test_prompt_regression.py

# Test specific prompt version
pytest tests/prompts/ --prompt-version=1.2.0
```

## Monitoring

Track in production:
- Success rate per prompt version
- Confidence score distribution
- Error types and frequencies
- User satisfaction ratings

## Contributing

When modifying prompts:

1. Create feature branch
2. Update prompt file
3. Increment version
4. Add changelog entry
5. Write/update tests
6. Submit PR with test results

## Resources

- Full guide: See [PROMPTS.md](../PROMPTS.md) in docs/
- Examples: See each prompt file's "Examples" section
- Testing: See tests/prompts/ directory

---

*Prompts are code. Treat them with the same rigor.*
