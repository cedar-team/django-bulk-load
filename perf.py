#!/usr/bin/env python
"""
Performance benchmarks for django-bulk-load

Compares legacy vs MERGE approaches for bulk_update_models and bulk_upsert_models
on heavily indexed tables to simulate real-world production scenarios.

Tests performance differences between traditional UPDATE/INSERT methods
and PostgreSQL 15+ MERGE statements under heavy indexing load.

The test table includes:
- 7 individual indexes on commonly queried fields
- 5 composite indexes for multi-field queries  
- 3 partial indexes for conditional queries
- 1 covering index for performance
- 2 unique constraints for data integrity

This creates realistic conditions where bulk operations must maintain
15+ indexes during each operation.
"""

import os
import sys
import django
from django.db import connections
from django.core.management import execute_from_command_line
from time import time
from datetime import datetime, timezone
from random import choice, randint, uniform

# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.test_project.settings")
django.setup()

from django_bulk_load import bulk_upsert_models
from tests.test_project.models import TestComplexModel

STATUSES = ['active', 'inactive', 'pending', 'completed', 'cancelled']
CATEGORIES = ['urgent', 'normal', 'low', 'critical', 'maintenance', 'feature', 'bugfix', 'enhancement']


def _generate_realistic_model(i, used_integers=None, used_strings=None):
    if used_integers is None:
        used_integers = set()
    if used_strings is None:
        used_strings = set()
    integer_field = i
    category = choice(CATEGORIES)
    while (integer_field, category) in used_integers:
        integer_field += 1
    used_integers.add((integer_field, category))
    string_field = f"record_{i}"
    status = choice(STATUSES)
    is_active = choice([True, False])
    if is_active:
        while (string_field, status) in used_strings:
            string_field = f"record_{i}_{randint(1000, 9999)}"
        used_strings.add((string_field, status))
    return TestComplexModel(
        integer_field=integer_field,
        string_field=string_field,
        datetime_field=datetime.now(timezone.utc),
        status=status,
        priority=randint(1, 10),
        category=category,
        score=uniform(0.0, 100.0),
        is_active=is_active,
    )

def _clear_database():
    try:
        TestComplexModel.objects.all().delete()
    except Exception as e:
        print(f"Warning: Failed to clear database: {e}")
        for conn in connections.all():
            conn.close()

def _process_in_batches(operation_func, models, batch_size=10000, operation_name="batch operation"):
    start = time()
    total_batches = (len(models) + batch_size - 1) // batch_size
    for i in range(0, len(models), batch_size):
        batch = models[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        try:
            operation_func(batch)
        except Exception as e:
            print(f"  Batch {batch_num} failed: {e}")
            raise e
    return time() - start

def _time_operation(operation_func, operation_name, use_batching=False, models=None, batch_size=10000):
    if use_batching:
        start = time()
        try:
            _process_in_batches(operation_func, models, batch_size, operation_name)
            elapsed_time = time() - start
            print(f"{operation_name}:      {elapsed_time}")
            return elapsed_time
        except Exception as e:
            print(f"{operation_name}:      Failed - {e}")
            return None
    else:
        start = time()
        try:
            operation_func()
            elapsed_time = time() - start
            print(f"{operation_name}:      {elapsed_time}")
            return elapsed_time
        except Exception as e:
            print(f"{operation_name}:      Failed - {e}")
            return None

def _setup_upsert_test_data(count, prefix):
    existing_models = _setup_test_database(count, pre_populate_ratio=0.5)
    used_integers = set()
    used_strings = set()
    models = []
    batch_size = 10000
    created = 0
    while created < count:
        batch = min(batch_size, count - created)
        models_batch = [_generate_realistic_model(i + created, used_integers, used_strings) for i in range(batch)]
        models.extend(models_batch)
        created += batch
    return models

def _setup_test_database(count, pre_populate_ratio=1.0, batch_size=10000):
    _clear_database()
    used_integers = set()
    used_strings = set()
    initial_count = int(count * pre_populate_ratio)
    created = 0
    while created < initial_count:
        batch = min(batch_size, initial_count - created)
        models_batch = [_generate_realistic_model(i + created, used_integers, used_strings) for i in range(batch)]
        TestComplexModel.objects.bulk_create(models_batch)
        created += batch
    return list(TestComplexModel.objects.all())

def run_bulk_upsert_benchmarks():
    print("=" * 60)
    print("BULK UPSERT BENCHMARKS (HEAVILY INDEXED TABLE)")
    print("=" * 60)
    print("bulk_upsert_models: Legacy vs use_unlogged vs use_merge vs use_copy")
    print("Testing with 15+ indexes, composite indexes, partial indexes, and unique constraints")
    print()
    counts = [1_000, 10_000, 100_000, 500_000]
    batch_size = 10000
    for count in counts:
        print(f"count: {count:,}")
        
        # Legacy
        models = _setup_upsert_test_data(count, "legacy")
        use_batching = count >= 100_000
        if use_batching:
            legacy_time = _time_operation(
                lambda batch: bulk_upsert_models(batch, use_merge=False, use_unlogged=False, use_copy=False),
                "bulk_upsert_models (legacy)",
                use_batching=True,
                models=models,
                batch_size=batch_size
            )
        else:
            legacy_time = _time_operation(
                lambda: bulk_upsert_models(models, use_merge=False, use_unlogged=False, use_copy=False),
                "bulk_upsert_models (legacy)",
                use_batching=False
            )
        
        # use_unlogged
        _clear_database()
        models = _setup_upsert_test_data(count, "unlogged")
        if use_batching:
            unlogged_time = _time_operation(
                lambda batch: bulk_upsert_models(batch, use_merge=False, use_unlogged=True, use_copy=False),
                "bulk_upsert_models (use_unlogged)",
                use_batching=True,
                models=models,
                batch_size=batch_size
            )
        else:
            unlogged_time = _time_operation(
                lambda: bulk_upsert_models(models, use_merge=False, use_unlogged=True, use_copy=False),
                "bulk_upsert_models (use_unlogged)",
                use_batching=False
            )
        
        # use_merge
        _clear_database()
        models = _setup_upsert_test_data(count, "merge")
        if use_batching:
            merge_time = _time_operation(
                lambda batch: bulk_upsert_models(batch, use_merge=True, use_unlogged=False, use_copy=False),
                "bulk_upsert_models (use_merge)",
                use_batching=True,
                models=models,
                batch_size=batch_size
            )
        else:
            merge_time = _time_operation(
                lambda: bulk_upsert_models(models, use_merge=True, use_unlogged=False, use_copy=False),
                "bulk_upsert_models (use_merge)",
                use_batching=False
            )
        
        # use_copy
        _clear_database()
        models = _setup_upsert_test_data(count, "copy")
        if use_batching:
            copy_time = _time_operation(
                lambda batch: bulk_upsert_models(batch, use_merge=False, use_unlogged=False, use_copy=True),
                "bulk_upsert_models (use_copy)",
                use_batching=True,
                models=models,
                batch_size=batch_size
            )
        else:
            copy_time = _time_operation(
                lambda: bulk_upsert_models(models, use_merge=False, use_unlogged=False, use_copy=True),
                "bulk_upsert_models (use_copy)",
                use_batching=False
            )
        
        print()
        print(f"Results for {count:,} records:")
        print(f"  Legacy:      {legacy_time:.3f} seconds")
        print(f"  Unlogged:    {unlogged_time:.3f} seconds")
        print(f"  Merge:       {merge_time:.3f} seconds")
        print(f"  Copy:        {copy_time:.3f} seconds")
        
        if legacy_time:
            if unlogged_time:
                if unlogged_time < legacy_time:
                    print(f"  Unlogged is {legacy_time/unlogged_time:.2f}x faster than legacy")
                else:
                    print(f"  Unlogged is {unlogged_time/legacy_time:.2f}x slower than legacy")
            
            if merge_time:
                if merge_time < legacy_time:
                    print(f"  Merge is {legacy_time/merge_time:.2f}x faster than legacy")
                else:
                    print(f"  Merge is {merge_time/legacy_time:.2f}x slower than legacy")
            
            if copy_time:
                if copy_time < legacy_time:
                    print(f"  Copy is {legacy_time/copy_time:.2f}x faster than legacy")
                else:
                    print(f"  Copy is {copy_time/legacy_time:.2f}x slower than legacy")
        
        print()
        _clear_database()

def main():
    print("Django Bulk Load Performance Benchmarks")
    print("Legacy vs use_unlogged Upsert Comparison")
    print("HEAVILY INDEXED TABLE SCENARIO")
    print("=" * 60)
    print()
    try:
        execute_from_command_line(['manage.py', 'migrate'])
        run_bulk_upsert_benchmarks()
    except Exception as e:
        print(f"Error running benchmarks: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 