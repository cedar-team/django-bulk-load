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
from time import time
from datetime import datetime, timezone
from random import choice, randint, uniform

# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.test_project.settings")
django.setup()

from django.db import transaction
from django_bulk_load import bulk_update_models, bulk_insert_models, bulk_upsert_models
from tests.test_project.models import TestComplexModel
from django.db import connection

# Data generation helpers for heavily indexed table
STATUSES = ['active', 'inactive', 'pending', 'completed', 'cancelled']
CATEGORIES = ['urgent', 'normal', 'low', 'critical', 'maintenance', 'feature', 'bugfix', 'enhancement']

def generate_realistic_model(i, used_integers=None, used_strings=None):
    """Generate a TestComplexModel with realistic data for all indexed fields"""
    if used_integers is None:
        used_integers = set()
    if used_strings is None:
        used_strings = set()
    
    # Ensure unique integer_field for unique constraint (integer_field, category)
    integer_field = i
    category = choice(CATEGORIES)
    while (integer_field, category) in used_integers:
        integer_field += 1
    used_integers.add((integer_field, category))
    
    # Ensure unique string_field for unique constraint (string_field, status) when is_active=True
    string_field = f"record_{i}"
    status = choice(STATUSES)
    is_active = choice([True, False])
    
    # Only check uniqueness for active records
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


def clear_database():
    """Clear all test models from the database"""
    try:
        TestComplexModel.objects.all().delete()
    except Exception as e:
        print(f"Warning: Failed to clear database: {e}")
        # Try to reconnect
        from django.db import connections
        for conn in connections.all():
            conn.close()


def safe_execute_with_retry(operation_func, operation_name, max_retries=2):
    """Execute an operation with retry logic for database connection issues"""
    for attempt in range(max_retries + 1):
        try:
            start = time()
            operation_func()
            return time() - start
        except Exception as e:
            error_msg = str(e).lower()
            if "server closed the connection" in error_msg or "connection" in error_msg:
                print(f"{operation_name}: Connection lost (attempt {attempt + 1}/{max_retries + 1})")
                if attempt < max_retries:
                    print("Retrying after connection reset...")
                    # Reset database connections
                    from django.db import connections
                    for conn in connections.all():
                        conn.close()
                    # Wait a moment before retry
                    import time as time_sleep
                    time_sleep.sleep(2)
                    continue
                else:
                    print(f"{operation_name}: Failed after {max_retries + 1} attempts - {e}")
                    return None
            else:
                print(f"{operation_name}: Failed - {e}")
                return None
    return None


def run_bulk_update_benchmarks():
    """Run benchmarks comparing bulk_update_models: legacy vs merge approaches"""
    print("=" * 60)
    print("BULK UPDATE BENCHMARKS (HEAVILY INDEXED TABLE)")
    print("=" * 60)
    print("bulk_update_models: Legacy vs MERGE approach")
    print("Testing with 15+ indexes, composite indexes, partial indexes, and unique constraints")
    print()
    
    counts = [1_000, 10_000, 100_000, 500_000]
    
    for count in counts:
        print(f"count: {count:,}")
        
        # Create test models in database first
        clear_database()
        used_integers = set()
        used_strings = set()
        initial_models = [generate_realistic_model(i, used_integers, used_strings) for i in range(count)]
        TestComplexModel.objects.bulk_create(initial_models)
        
        # Prepare models for update (need to fetch from DB to get IDs)
        models = list(TestComplexModel.objects.all())
        
        # Get all existing (integer_field, category) pairs from the database
        existing_pairs = set(TestComplexModel.objects.values_list('integer_field', 'category'))
        used_integers = existing_pairs.copy()
        used_strings = set()
        
        for i, model in enumerate(models):
            # Ensure unique integer_field for unique constraint (integer_field, category)
            integer_field = i + 1000
            category = choice(CATEGORIES)
            while (integer_field, category) in used_integers:
                integer_field += 1
            used_integers.add((integer_field, category))
            
            # Ensure unique string_field for unique constraint (string_field, status) when is_active=True
            string_field = f"updated_{i}"
            status = choice(STATUSES)
            is_active = choice([True, False])
            
            # Only check uniqueness for active records
            if is_active:
                while (string_field, status) in used_strings:
                    string_field = f"updated_{i}_{randint(1000, 9999)}"
                used_strings.add((string_field, status))
            
            model.integer_field = integer_field
            model.string_field = string_field
            model.status = status
            model.priority = randint(1, 10)
            model.category = category
            model.score = uniform(0.0, 100.0)
            model.is_active = is_active
        
        # Test bulk_update_models with use_merge=False (legacy approach)
        if count >= 100_000:
            legacy_time = safe_execute_with_retry(
                lambda: bulk_update_models(models, use_merge=False),
                "bulk_update_models (legacy)"
            )
            if legacy_time:
                print(f"bulk_update_models (legacy):      {legacy_time}")
        else:
            start = time()
            try:
                bulk_update_models(models, use_merge=False)
                legacy_time = time() - start
                print(f"bulk_update_models (legacy):      {legacy_time}")
            except Exception as e:
                print(f"bulk_update_models (legacy):      Failed - {e}")
                legacy_time = None
        
        # Reset models for next test
        # Get all existing (integer_field, category) pairs from the database
        existing_pairs = set(TestComplexModel.objects.values_list('integer_field', 'category'))
        used_integers = existing_pairs.copy()
        used_strings = set()
        
        for i, model in enumerate(models):
            # Ensure unique integer_field for unique constraint (integer_field, category)
            integer_field = i + 2000
            category = choice(CATEGORIES)
            while (integer_field, category) in used_integers:
                integer_field += 1
            used_integers.add((integer_field, category))
            
            # Ensure unique string_field for unique constraint (string_field, status) when is_active=True
            string_field = f"merge_test_{i}"
            status = choice(STATUSES)
            is_active = choice([True, False])
            
            # Only check uniqueness for active records
            if is_active:
                while (string_field, status) in used_strings:
                    string_field = f"merge_test_{i}_{randint(1000, 9999)}"
                used_strings.add((string_field, status))
            
            model.integer_field = integer_field
            model.string_field = string_field
            model.status = status
            model.priority = randint(1, 10)
            model.category = category
            model.score = uniform(0.0, 100.0)
            model.is_active = is_active
        
        # Test bulk_update_models with use_merge=True (MERGE approach)
        if count >= 100_000:
            merge_time = safe_execute_with_retry(
                lambda: bulk_update_models(models, use_merge=True),
                "bulk_update_models (merge)"
            )
            if merge_time:
                print(f"bulk_update_models (merge):       {merge_time}")
        else:
            start = time()
            try:
                bulk_update_models(models, use_merge=True)
                merge_time = time() - start
                print(f"bulk_update_models (merge):       {merge_time}")
            except Exception as e:
                print(f"bulk_update_models (merge):       Failed - {e}")
                merge_time = None
        
        # Test bulk_update_models with use_copy=True (fastest approach)
        start = time()
        try:
            bulk_update_models(models, use_copy=True)
            copy_time = time() - start
            print(f"bulk_update_models (COPY):         {copy_time}")
        except Exception as e:
            print(f"bulk_update_models (COPY):         Failed - {e}")
            copy_time = None
        
        # Show performance comparison
        if legacy_time and merge_time:
            if merge_time < legacy_time:
                improvement = legacy_time / merge_time
                print(f"MERGE vs Legacy:                  {improvement:.1f}x faster")
            else:
                improvement = merge_time / legacy_time
                print(f"MERGE vs Legacy:                  {improvement:.1f}x slower")
        if legacy_time and copy_time:
            if copy_time < legacy_time:
                improvement = legacy_time / copy_time
                print(f"COPY vs Legacy:                   {improvement:.1f}x faster")
            else:
                improvement = copy_time / legacy_time
                print(f"COPY vs Legacy:                   {improvement:.1f}x slower")
        
        print()
        clear_database()


def run_bulk_upsert_benchmarks():
    """Run benchmarks comparing bulk_upsert_models: legacy vs merge approaches"""
    print("=" * 60)
    print("BULK UPSERT BENCHMARKS (HEAVILY INDEXED TABLE)")
    print("=" * 60)
    print("bulk_upsert_models: Legacy vs MERGE approach")
    print("Testing with 15+ indexes, composite indexes, partial indexes, and unique constraints")
    print()
    
    counts = [1_000, 10_000, 100_000, 500_000]
    
    for count in counts:
        print(f"count: {count:,}")
        
        # Pre-populate half the records in database
        clear_database()
        used_integers = set()
        used_strings = set()
        existing_models = [generate_realistic_model(i, used_integers, used_strings) for i in range(0, count // 2)]
        TestComplexModel.objects.bulk_create(existing_models)
        
        # Prepare models for upsert (mix of new and existing)
        used_integers = set()
        used_strings = set()
        models = [generate_realistic_model(i, used_integers, used_strings) for i in range(count)]
        # Modify some fields for the upsert test
        for i, model in enumerate(models):
            model.string_field = f"upsert_{i}"
            model.status = choice(STATUSES)
            model.priority = randint(1, 10)
        # Set IDs for existing records to simulate updates
        existing_records = list(TestComplexModel.objects.all())
        for i, existing in enumerate(existing_records):
            if i < len(models):
                models[i].id = existing.id
        
        # Test bulk_upsert_models with use_merge=False (legacy approach)
        if count >= 100_000:
            legacy_time = safe_execute_with_retry(
                lambda: bulk_upsert_models(models, use_merge=False),
                "bulk_upsert_models (legacy)"
            )
            if legacy_time:
                print(f"bulk_upsert_models (legacy):      {legacy_time}")
        else:
            start = time()
            try:
                bulk_upsert_models(models, use_merge=False)
                legacy_time = time() - start
                print(f"bulk_upsert_models (legacy):      {legacy_time}")
            except Exception as e:
                print(f"bulk_upsert_models (legacy):      Failed - {e}")
                legacy_time = None
        
        # Reset database for next test
        clear_database()
        used_integers = set()
        used_strings = set()
        existing_models = [generate_realistic_model(i, used_integers, used_strings) for i in range(0, count // 2)]
        TestComplexModel.objects.bulk_create(existing_models)
        
        # Prepare models again
        used_integers = set()
        used_strings = set()
        models = [generate_realistic_model(i, used_integers, used_strings) for i in range(count)]
        # Modify some fields for the merge upsert test
        for i, model in enumerate(models):
            model.string_field = f"merge_upsert_{i}"
            model.status = choice(STATUSES)
            model.priority = randint(1, 10)
        existing_records = list(TestComplexModel.objects.all())
        for i, existing in enumerate(existing_records):
            if i < len(models):
                models[i].id = existing.id
        
        # Test bulk_upsert_models with use_merge=True (MERGE approach)
        if count >= 100_000:
            merge_time = safe_execute_with_retry(
                lambda: bulk_upsert_models(models, use_merge=True),
                "bulk_upsert_models (merge)"
            )
            if merge_time:
                print(f"bulk_upsert_models (merge):       {merge_time}")
        else:
            start = time()
            try:
                bulk_upsert_models(models, use_merge=True)
                merge_time = time() - start
                print(f"bulk_upsert_models (merge):       {merge_time}")
            except Exception as e:
                print(f"bulk_upsert_models (merge):       Failed - {e}")
                merge_time = None
        
        # Test bulk_upsert_models with use_copy=True (fastest approach)
        start = time()
        try:
            bulk_upsert_models(models, use_copy=True)
            copy_time = time() - start
            print(f"bulk_upsert_models (COPY):         {copy_time}")
        except Exception as e:
            print(f"bulk_upsert_models (COPY):         Failed - {e}")
            copy_time = None
        
        # Show performance comparison
        if legacy_time and merge_time:
            if merge_time < legacy_time:
                improvement = legacy_time / merge_time
                print(f"MERGE vs Legacy:                  {improvement:.1f}x faster")
            else:
                improvement = merge_time / legacy_time
                print(f"MERGE vs Legacy:                  {improvement:.1f}x slower")
        if legacy_time and copy_time:
            if copy_time < legacy_time:
                improvement = legacy_time / copy_time
                print(f"COPY vs Legacy:                   {improvement:.1f}x faster")
            else:
                improvement = copy_time / legacy_time
                print(f"COPY vs Legacy:                   {improvement:.1f}x slower")
        
        print()
        clear_database()


def main():
    """Run performance benchmarks comparing legacy vs MERGE approaches"""
    print("Django Bulk Load Performance Benchmarks")
    print("Legacy vs MERGE Approach Comparison")
    print("HEAVILY INDEXED TABLE SCENARIO")
    print("=" * 60)
    print()
    
    try:
        # Ensure database is ready
        from django.core.management import execute_from_command_line
        execute_from_command_line(['manage.py', 'migrate'])
        
        run_bulk_update_benchmarks()
        run_bulk_upsert_benchmarks()
        
        print("=" * 60)
        print("BENCHMARKS COMPLETE")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\nBenchmarks interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error running benchmarks: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 