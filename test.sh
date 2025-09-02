#!/usr/bin/env bash

# Check if we're in GitHub Actions or if docker-compose is not available
if [ "$GITHUB_ACTIONS" = "true" ] || ! command -v docker-compose &> /dev/null; then
    # Run tests directly with Python
    python manage.py test
else
    # Run tests with Docker Compose
    docker-compose run --rm test ./manage.py test
fi