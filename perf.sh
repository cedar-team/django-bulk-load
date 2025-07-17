#!/usr/bin/env bash

echo "Starting Django Bulk Load Performance Benchmarks..."
echo "Testing Legacy vs MERGE approaches on HEAVILY INDEXED tables"
echo "This may take several minutes to complete."
echo ""

# Show the indexing scenario being tested
echo "To see the indexing details, run: docker-compose run --rm test python show_indexes.py"
echo ""

# Run the performance script in the Docker environment
docker-compose run --rm test python perf.py 