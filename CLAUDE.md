# CLAUDE.md — Project instructions for Claude Code

## Python environment

The project uses a shared virtualenv at `/Users/rhlbh/projects/claudeProjects/`.

**Always run Python scripts with:**
```
PYTHONPATH=/Users/rhlbh/projects/claudeProjects/quant /Users/rhlbh/projects/claudeProjects/bin/python <script>
```

Or set both before running multiple commands:
```
export PYTHONPATH=/Users/rhlbh/projects/claudeProjects/quant
export PYTHON=/Users/rhlbh/projects/claudeProjects/bin/python
```

## Database

- **Engine:** DuckDB
- **File:** `data/quant.db` (relative to project root)
- **Singleton:** imported via `from src.db.client import db`; always call `db.open()` before use and `db.close()` after.
