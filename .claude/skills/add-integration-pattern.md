---
description: How to add a new integration or infrastructure pattern example to integration-automation-patterns
---

# Skill: Add a New Integration Pattern

Use this when adding a new pattern to integration-automation-patterns.

## Files to create

1. `examples/NN_<pattern>_patterns.py` — the pattern implementation
2. `tests/test_NN_<pattern>_patterns.py` — tests using importlib loader

## Pattern structure

Patterns use plain stdlib only (`dataclasses`, `threading`, `time`, `random`, `collections`, `enum`, `asyncio`). No external dependencies.

```python
"""
<Pattern Name> — one-line description.

Detailed description of what problem this solves and how.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import threading
import time

class MyPattern:
    """
    Docstring explaining the pattern, when to use it, guarantees.
    """
    def __init__(self, ...):
        self._lock = threading.Lock()

    def operation(self, ...) -> ...:
        with self._lock:
            ...
```

## Test import pattern (required)

```python
import importlib.util, sys, types, os

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

EXAMPLES_DIR = os.path.join(os.path.dirname(__file__), '..', 'examples')
m = load_module("my_pattern", os.path.join(EXAMPLES_DIR, "NN_pattern_patterns.py"))
```

## Using RetryPolicy (src/ package)

```python
from integration_automation_patterns.retry_policy import RetryPolicy, RetryExhausted

policy = RetryPolicy(max_attempts=3, base_delay=0.1, max_delay=10.0)
try:
    result = policy.execute(my_unreliable_function, arg1, arg2)
except RetryExhausted as e:
    logger.error("Failed after %d attempts: %s", e.attempts, e.last_error)
```

## `if __name__ == "__main__"` demo (required)

Every example must have a runnable demo with at least 4 scenarios showing the pattern in action. Print clear output.

## README update (required)

1. **Header** (line 9): `31 examples, 1044 tests` → `32 examples, NNNN tests`
2. **Catalog heading**: `## Example catalog — 31 patterns` → `32`
3. **Catalog row**: `| 32 | \`32_file.py\` | Pattern Name | Problem Solved |`
4. **Pattern reference table** (if applicable): add row under the right section
5. **Repository structure**: update `examples/ # NN runnable pattern examples`
6. **Trilogy footer** — update in ALL THREE repo READMEs

## CHANGELOG entry

```markdown
## [vX.Y.Z] — YYYY-MM-DD

### Added — Pattern Name (`NN_pattern_patterns.py`)

- `Class1` — what it does, key invariants
- `Class2` — what it does, key invariants
- ...

N new tests. Total: **NNNN passed**.
```

## Version bump

Bump `version` in `pyproject.toml` and `__version__` in `src/integration_automation_patterns/__init__.py` and citation in README.
