from .bulk_load import (
    bulk_insert_changed_models,
    bulk_load_models_with_queries,
    bulk_select_model_dicts,
    bulk_insert_models,
    bulk_update_models,
    bulk_upsert_models,
    _bulk_upsert_models_with_copy,
    _bulk_update_models_with_copy,
)

from .queries import (
    generate_distinct_condition,
    generate_greater_than_condition,
    generate_merge_upsert_query,
    generate_merge_upsert_with_returning,
    generate_merge_conditional_query,
    generate_merge_update_query,
    add_merge_returning
)


# Configuration for bulk operations
class BulkLoadConfig:
    """
    Global configuration for django-bulk-load operations.
    
    Attributes:
        use_merge_by_default: Whether to use PostgreSQL 15+ MERGE by default for upsert operations.
                             Can be overridden per function call. Defaults to True.
        merge_fallback_enabled: Whether to automatically fall back to legacy approach when MERGE fails.
                               Defaults to True for maximum compatibility.
    """
    use_merge_by_default: bool = True
    merge_fallback_enabled: bool = True


# Global configuration instance
config = BulkLoadConfig()


def configure(**kwargs):
    """
    Configure global settings for django-bulk-load.
    
    Args:
        use_merge_by_default: Whether to use MERGE by default for upsert operations
        merge_fallback_enabled: Whether to enable automatic fallback to legacy approach
        
    Example:
        import django_bulk_load
        django_bulk_load.configure(use_merge_by_default=False)
    """
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
        else:
            raise ValueError(f"Unknown configuration option: {key}")


__all__ = [
    "bulk_select_model_dicts",
    "bulk_insert_models", 
    "bulk_update_models",
    "bulk_upsert_models",
    "bulk_insert_changed_models",
    "bulk_load_models_with_queries",
    "generate_distinct_condition",
    "generate_greater_than_condition",
    "generate_merge_upsert_query",
    "generate_merge_upsert_with_returning", 
    "generate_merge_conditional_query",
    "generate_merge_update_query",
    "add_merge_returning",
    "configure",
    "config"
]
