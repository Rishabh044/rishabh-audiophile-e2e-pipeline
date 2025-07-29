# Makefile for Audiophile E2E Pipeline

.PHONY: help install install-dev test test-unit test-integration test-coverage lint format type-check clean

# Default target
help:
	@echo "Available commands:"
	@echo "  make install        Install production dependencies"
	@echo "  make install-dev    Install all dependencies (prod + dev)"
	@echo "  make test          Run all tests"
	@echo "  make test-unit     Run unit tests only"
	@echo "  make test-integration Run integration tests only"
	@echo "  make test-coverage Run tests with coverage report"
	@echo "  make lint          Run linting (flake8)"
	@echo "  make format        Format code with black"
	@echo "  make type-check    Run type checking with mypy"
	@echo "  make clean         Clean up generated files"

# Install dependencies
install:
	pip install -r requirements.txt

install-dev: install
	pip install -r requirements-dev.txt

# Testing commands
test:
	pytest

test-unit:
	pytest tests/unit -m "unit"

test-integration:
	pytest tests/integration -m "integration"

test-coverage:
	pytest --cov-report=html --cov-report=term

# Code quality commands
lint:
	flake8 models scraper silver_layer tests

format:
	black models scraper silver_layer tests

type-check:
	mypy models scraper silver_layer

# Clean up
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -type d -name '.pytest_cache' -exec rm -rf {} +
	find . -type d -name '.mypy_cache' -exec rm -rf {} +
	find . -type d -name 'htmlcov' -exec rm -rf {} +
	find . -type f -name '.coverage' -delete
	find . -type f -name 'coverage.xml' -delete