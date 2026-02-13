# Dynamic Tables DuckLake - Development Commands

# List all available commands
default:
    @just --list

# Install dependencies
install:
    uv pip install -e ".[dev]"

# Run tests
test:
    uv run pytest

# Run tests with coverage report
coverage:
    uv run pytest --cov=src/dynamic_tables --cov-report=term-missing --cov-report=html

# Run tests with coverage and open HTML report
coverage-html: coverage
    @echo "Opening coverage report..."
    @python -m webbrowser htmlcov/index.html || open htmlcov/index.html || xdg-open htmlcov/index.html

# Lint code with ruff
lint:
    uv run ruff check .

# Fix linting issues automatically
lint-fix:
    uv run ruff check --fix .

# Format code with ruff
format:
    uv run ruff format .

# Check code formatting without making changes
format-check:
    uv run ruff format --check .

# Run type checking with ty
typecheck:
    uv run ty check

# Run all checks (lint, format check, typecheck, test)
check: lint format-check typecheck test

# Clean up generated files
clean:
    rm -rf htmlcov/
    rm -rf .coverage
    rm -rf .pytest_cache/
    rm -rf .ty_cache/
    rm -rf .ruff_cache/
    rm -rf src/**/__pycache__/
    rm -rf tests/**/__pycache__/
    rm -rf **/*.pyc
    rm -rf dist/
    rm -rf build/
    rm -rf *.egg-info/

# Run a specific test by name
test-one TEST:
    uv run pytest -k {{TEST}} -v

# Run tests in watch mode (requires pytest-watch)
watch:
    uv run ptw -- --testmon

# Show project information
info:
    @echo "Python version:"
    @python --version
    @echo "\nInstalled packages:"
    @uv pip list
