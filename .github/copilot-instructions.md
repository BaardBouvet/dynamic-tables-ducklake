# Copilot Instructions

## Python Tooling

- **Package Management**: Always use `uv` for package installation and dependency management instead of pip
  - Install packages: `uv pip install <package>`
  - Install from requirements: `uv pip install -r requirements.txt`
  - Install project dependencies: `uv pip install -e .`

- **Code Quality**: Always use `ruff` for linting and formatting (ruff replaces black for formatting)
  - Linting: `uv run ruff check .`
  - Formatting: `uv run ruff format .`
  - Auto-fix issues: `uv run ruff check --fix .`

- **Type Checking**: Always use `mypy` for static type checking
  - Type check: `uv run mypy .`
  - Type check specific file: `uv run mypy <file_path>`
