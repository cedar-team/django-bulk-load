import logging
from time import monotonic
from typing import Dict, Iterable, List, Optional, Sequence, Type, Callable
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.db import connections, router, transaction
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorWrapper
from django.db.models import AutoField, Model, Field
from psycopg2.extras import execute_values
from psycopg2.sql import Composable, SQL

from .django import (
    django_field_to_query_value,
    get_fields_and_names,
    get_fields_from_names,
    get_model_fields,
    get_pk_fields,
    models_to_tsv_buffer,
    records_to_models,
)
from .queries import (
    add_returning,
    create_temp_table,
    create_unlogged_temp_table,
    generate_insert_on_not_match_latest,
    generate_insert_query,
    generate_insert_for_update_query,
    generate_select_latest,
    generate_select_query,
    generate_update_query,
    generate_values_select_query,
    copy_query,
    generate_merge_upsert_query,
    generate_merge_upsert_with_returning,
    generate_merge_conditional_query,
    generate_merge_update_query,
    add_merge_returning,
    generate_parallel_insert_query,
    generate_parallel_update_query,
    generate_parallel_upsert_query,
)
from .utils import generate_table_name

logger = logging.getLogger(__name__)


def create_temp_table_and_load(
    models: Sequence[Model],
    connection: BaseDatabaseWrapper,
    cursor: CursorWrapper,
    field_names: Optional[Sequence[str]] = None,
    table_name: Optional[str] = None,
    use_unlogged: bool = False,
) -> str:
    if not models:
        raise ValueError("No models passed. Can't create table without models")

    model_meta = models[0]._meta
    source_table_name = model_meta.db_table
    table_name = table_name or generate_table_name(source_table_name=source_table_name)
    fields, field_names = get_fields_and_names(
        field_names, model_meta, include_auto_fields=True
    )
    
    # Use UNLOGGED tables for maximum performance when data can be regenerated
    if use_unlogged:
        temp_table_query = create_unlogged_temp_table(
            temp_table_name=table_name,
            source_table_name=source_table_name,
            column_names=[x.column for x in fields],
        )
    else:
        temp_table_query = create_temp_table(
            temp_table_name=table_name,
            source_table_name=source_table_name,
            column_names=[x.column for x in fields],
        )
    
    cursor.execute(temp_table_query)
    
    # Use COPY with optimized settings
    copy_sql = copy_query(table_name)
    tsv_buffer = models_to_tsv_buffer(models, fields, connection=connection)
    
    # Optimize COPY performance
    cursor.execute(SQL("SET synchronous_commit = off"))
    
    cursor.copy_expert(
        copy_sql,
        tsv_buffer,
    )

    return table_name


def execute_queries_and_return_models(
    load_queries: Sequence[Composable], cursor: CursorWrapper, model_class: Type[Model]
):
    results = []
    has_query_returning_results = False
    for query in load_queries:
        cursor.execute(query)

        if cursor.description:
            has_query_returning_results = True
            columns = [col[0] for col in cursor.description]
            results += records_to_models(cursor.fetchall(), columns, model_class)

    if not has_query_returning_results:
        raise ValueError(
            "No queries return results. execute_queries_and_return_results expects at least 1 query"
            " to return results. Use RETURNING or a SELECT to return models"
        )

    return results


def bulk_load_models_with_queries(
    *,
    models: Sequence[Model],
    loading_table_name: str,
    load_queries: Sequence[Composable],
    field_names: Sequence[str] = None,
    return_models: bool = False,
    use_unlogged: bool = False,
):
    start_time = monotonic()
    model = models[0]
    db_name = router.db_for_write(model.__class__)

    connection = connections[db_name]
    results = None

    with connection.cursor() as cursor, transaction.atomic(using=db_name):
        table_name = model._meta.db_table

        logger.info(
            "Starting loading models",
            extra=dict(model_count=len(models), table_name=table_name),
        )
        loading_table_name = create_temp_table_and_load(
            models=models,
            table_name=loading_table_name,
            field_names=field_names,
            cursor=cursor,
            connection=connection,
            use_unlogged=use_unlogged,
        )
        logger.info(
            "Starting execution of queries on loading table",
            extra=dict(table_name=table_name, loading_table_name=loading_table_name),
        )
        if return_models:
            results = execute_queries_and_return_models(
                load_queries=load_queries, cursor=cursor, model_class=model.__class__
            )
        else:
            for query in load_queries:
                cursor.execute(query)

        logger.info(
            "Finished loading models",
            extra=dict(
                model_count=len(models),
                table_name=table_name,
                duration=monotonic() - start_time,
            ),
        )

        return results


def bulk_insert_models(
    models: Sequence[Model],
    ignore_conflicts: bool = False,
    return_models: bool = False,
    use_unlogged: bool = False,
):
    """
    INSERT a batch of models. It makes use of Postgres COPY command to improve speed. If a row already exist, the entire
    insert will fail.

    :param models: Django model list/tuple
    :param ignore_conflicts: If there is an error on a unique constrain, ignore instead of erroring
    :param return_models: Query and return the models in the DB, whether updated or not.
    Defaults to False, since this can significantly degrade performance
    :param use_unlogged: Use UNLOGGED tables for maximum performance (data can be regenerated)
    :return: None or List[Model] depending upon returns_models param. Returns all models passed in,
    not just ones updated or inserted. Models will not be in the same order they were passed in
    """
    if not models:
        logger.warning("No models passed to bulk_insert_models")
        return [] if return_models else None

    # Verify the models either all have pk set or all don't. It's an issue when it's a mix
    # because we have to specify a list of fields to insert. We need to ignore the PK field if it's not
    # set, but if it is set, you need to add it to the list of fields. Adding the field causes
    # NULL errors for any models that don't have the PK set.
    has_pks = None
    for model in models:
        models_has_pks = model.pk is not None
        if has_pks is None:
            has_pks = models_has_pks

        if has_pks != models_has_pks:
            raise ValueError(
                "Mix of models with PK and no PK specified. This can cause issues. Split into 2 groups instead"
            )
    model_meta = models[0]._meta
    table_name = model_meta.db_table

    loading_table_name = generate_table_name(table_name)

    insert_fields = get_model_fields(model_meta, include_auto_fields=has_pks)
    insert_query = generate_insert_query(
        table_name=table_name,
        loading_table_name=loading_table_name,
        ignore_conflicts=ignore_conflicts,
        insert_fields=insert_fields,
    )

    if return_models:
        # Since we want to return ALL models (not just the ones actually inserted), we
        # need to run an additional select on all of the models in the loading table
        insert_query = add_returning(insert_query, table_name=table_name)

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        load_queries=[insert_query],
        return_models=return_models,
        use_unlogged=use_unlogged,
    )


def bulk_update_models(
    models: Sequence[Model],
    update_field_names: Sequence[str] = None,
    pk_field_names: Sequence[str] = None,
    model_changed_field_names: Sequence[str] = None,
    update_if_null_field_names: Sequence[str] = None,
    update_where: Callable[[Sequence[Field], str, str], Composable] = None,
    return_models: bool = False,
    use_merge: bool = False,
    use_copy: bool = False,
    use_unlogged: bool = False,
):
    """
    UPDATE a batch of models. If the model is not found in the database, it is ignored.

    :param models: Django model list/tuple
    :param update_field_names: Field to update (defaults to all fields)
    :param pk_field_names: Fields used to match existing models in the DB. By default uses model primary key.
    :param model_changed_field_names: Fields that only get updated when another field (outside this
    list is changed) (i.e. update_on/last_modified)
    :param update_if_null_field_names: Fields that only get updated if the new value is NULL or existing
    value in the DB is NULL.
    :param update_where: Function that returns a Composable that is used to filter the update query. Should not be used
    with model_changed_field_names or update_if_null_field_names (can lead to unexpected behavior)
    :param return_models: Query and return the models in the DB, whether updated or not.
    Defaults to False, since this can significantly degrade performance
    :param use_merge: Whether to use PostgreSQL 15+ MERGE statement for improved performance.
    Automatically falls back to legacy UPDATE ... FROM approach for older PostgreSQL versions.
    :param use_copy: Whether to use PostgreSQL COPY command for fastest possible performance.
    This approach loads data into a temp table using COPY, then uses UPDATE ... FROM.
    Overrides use_merge when True.
    :param use_unlogged: Use UNLOGGED tables for maximum performance (data can be regenerated)
    :return: None or List[Model] depending upon returns_models param. Returns all models passed in,
    not just ones updated or inserted. Models will not be in the same order they were passed in
    """
    if not models:
        logger.warning("No models passed to bulk_update_models")
        return [] if return_models else None

    # Use COPY-based approach if requested (fastest method)
    if use_copy:
        return _bulk_update_models_with_copy(
            models=models,
            update_field_names=update_field_names,
            pk_field_names=pk_field_names,
            model_changed_field_names=model_changed_field_names,
            update_if_null_field_names=update_if_null_field_names,
            update_where=update_where,
            return_models=return_models,
        )

    model_changed_field_names = model_changed_field_names or []
    update_if_null_field_names = update_if_null_field_names or []
    model_meta = models[0]._meta
    table_name = model_meta.db_table
    fields, field_names = get_fields_and_names(update_field_names, model_meta, include_auto_fields=True)

    pk_fields = get_pk_fields(pk_field_names, model_meta)
    pk_field_names = [field.name for field in pk_fields]
    ignore_on_compare = {
        *model_changed_field_names,
        *pk_field_names,
    }
    compare_fields = [field for field in fields if field.name not in ignore_on_compare]
    ignore_on_update = {
        *pk_field_names,
        *update_if_null_field_names,
    }
    update_fields = [field for field in fields if field.name not in ignore_on_update]
    fields_names_to_operate_on = [field.name for field in fields]
    loading_table_name = generate_table_name(table_name)
    
    # Use MERGE for PostgreSQL 15+ when enabled
    if use_merge:
        try:
            update_query = generate_merge_update_query(
                table_name=table_name,
                loading_table_name=loading_table_name,
                pk_fields=pk_fields,
                update_fields=update_fields,
                compare_fields=compare_fields,
                update_where=update_where,
                update_if_null_fields=get_fields_from_names(
                    update_if_null_field_names, model_meta
                ),
            )
        except Exception as e:
            # Fall back to legacy approach if MERGE fails
            logger.warning(f"MERGE update failed, falling back to legacy approach: {e}")
            use_merge = False
    
    # Legacy UPDATE ... FROM approach for older PostgreSQL versions or when MERGE fails
    if not use_merge:
        update_query = generate_update_query(
            table_name=table_name,
            compare_fields=compare_fields,
            update_fields=update_fields,
            update_if_null_fields=get_fields_from_names(
                update_if_null_field_names, model_meta
            ),
            pk_fields=pk_fields,
            update_where=update_where,
            loading_table_name=loading_table_name,
        )

    if return_models:
        select_query = generate_select_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            join_fields=pk_fields,
        )
        queries = [update_query, select_query]
    else:
        queries = [update_query]

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        field_names=fields_names_to_operate_on,
        load_queries=queries,
        return_models=return_models,
        use_unlogged=use_unlogged,
    )


def _bulk_update_models_with_copy(
    models: Sequence[Model],
    update_field_names: Sequence[str] = None,
    pk_field_names: Sequence[str] = None,
    model_changed_field_names: Sequence[str] = None,
    update_if_null_field_names: Sequence[str] = None,
    update_where: Callable[[Sequence[Field], str, str], Composable] = None,
    return_models: bool = False,
):
    """
    Fastest UPDATE implementation using PostgreSQL COPY command.
    Loads data into a temp table using COPY, then uses UPDATE ... FROM.
    """
    if not models:
        return [] if return_models else None

    model_changed_field_names = model_changed_field_names or []
    update_if_null_field_names = update_if_null_field_names or []
    model_meta = models[0]._meta
    table_name = model_meta.db_table
    fields, field_names = get_fields_and_names(update_field_names, model_meta, include_auto_fields=True)

    pk_fields = get_pk_fields(pk_field_names, model_meta)
    pk_field_names = [field.name for field in pk_fields]
    ignore_on_compare = {
        *model_changed_field_names,
        *pk_field_names,
    }
    compare_fields = [field for field in fields if field.name not in ignore_on_compare]
    ignore_on_update = {
        *pk_field_names,
        *update_if_null_field_names,
    }
    update_fields = [field for field in fields if field.name not in ignore_on_update]
    
    # Use the existing optimized bulk_load_models_with_queries infrastructure
    loading_table_name = generate_table_name(table_name)
    
    # Generate the UPDATE query
    update_query = generate_update_query(
        table_name=table_name,
        compare_fields=compare_fields,
        update_fields=update_fields,
        update_if_null_fields=get_fields_from_names(
            update_if_null_field_names, model_meta
        ),
        pk_fields=pk_fields,
        update_where=update_where,
        loading_table_name=loading_table_name,
    )

    if return_models:
        select_query = generate_select_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            join_fields=pk_fields,
        )
        queries = [update_query, select_query]
    else:
        queries = [update_query]

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        field_names=field_names,
        load_queries=queries,
        return_models=return_models,
    )


def bulk_upsert_models(
    models: Sequence[Model],
    pk_field_names: Sequence[str] = None,
    insert_only_field_names: Sequence[str] = None,
    model_changed_field_names: Sequence[str] = None,
    update_if_null_field_names: Sequence[str] = None,
    update_where: Callable[[Sequence[Field], str, str], Composable] = None,
    return_models: bool = False,
    use_merge: bool = False,
    use_copy: bool = False,
    use_unlogged: bool = False,
):
    """
    UPSERT a batch of models. Replicates [UPSERTing](https://wiki.postgresql.org/wiki/UPSERT) for a large set of models.
    By default, it matches existing models using the model `pk`, but you can specify matching on other fields with
    `pk_field_names`.

    :param models: Django model list/tuple
    :param insert_only_field_names: Names of model fields to only insert, never update (i.e. created_on)
    :param pk_field_names: Fields used to match existing models in the DB. By default uses model primary key.
    :param model_changed_field_names: Fields that only get updated when another field (outside this
    list is changed) (i.e. update_on/last_modified)
    :param update_if_null_field_names: Fields that only get updated if the new value is NULL or existing
    value in the DB is NULL.
    :param update_where: Function that returns a Composable that is used to filter the update query. Cannot be used
    with model_changed_field_names or update_if_null_field_names
    :param return_models: Query and return the models in the DB, whether updated or not.
    Defaults to False, since this can significantly degrade performance
    :param use_merge: Whether to use PostgreSQL 15+ MERGE statement for improved performance.
    If None, uses the global configuration (config.use_merge_by_default). 
    Automatically falls back to legacy approach for older PostgreSQL versions.
    :param use_copy: Whether to use PostgreSQL COPY command for fastest possible performance.
    This approach loads data into a temp table using COPY, then uses INSERT ... ON CONFLICT.
    Overrides use_merge when True.
    :param use_unlogged: Use UNLOGGED tables for maximum performance (data can be regenerated)
    :return: None or List[Model] depending upon returns_models param. Returns all models passed in,
    not just ones updated or inserted. Models will not be in the same order they were passed in
    """
    if not models:
        logger.warning("No models passed to bulk_upsert_models")
        return [] if return_models else None

    # Use COPY-based approach if requested (fastest method)
    if use_copy:
        return _bulk_upsert_models_with_copy(
            models=models,
            pk_field_names=pk_field_names,
            insert_only_field_names=insert_only_field_names,
            model_changed_field_names=model_changed_field_names,
            update_if_null_field_names=update_if_null_field_names,
            update_where=update_where,
            return_models=return_models,
        )

    insert_only_field_names = insert_only_field_names or []
    model_changed_field_names = model_changed_field_names or []
    update_if_null_field_names = update_if_null_field_names or []
    model_meta = models[0]._meta
    table_name = model_meta.db_table
    fields, field_names = get_fields_and_names(None, model_meta)

    pk_fields = get_pk_fields(pk_field_names, model_meta)
    pk_field_names = [field.name for field in pk_fields]
    ignore_on_compare = {
        *insert_only_field_names,
        *model_changed_field_names,
        *pk_field_names,
    }
    compare_fields = [field for field in fields if field.name not in ignore_on_compare]
    ignore_on_update = {
        *insert_only_field_names,
        *pk_field_names,
        *update_if_null_field_names,
    }
    update_fields = [field for field in fields if field.name not in ignore_on_update]
    insert_fields = [field for field in fields if not isinstance(field, AutoField)]
    loading_table_name = generate_table_name(table_name)

    # Use MERGE for PostgreSQL 15+ when enabled
    if use_merge:
        try:
            return _bulk_upsert_models_with_merge(
                models=models,
                table_name=table_name,
                loading_table_name=loading_table_name,
                pk_fields=pk_fields,
                update_fields=update_fields,
                insert_fields=insert_fields,
                compare_fields=compare_fields,
                update_where=update_where,
                update_if_null_field_names=update_if_null_field_names,
                model_meta=model_meta,
                return_models=return_models,
            )
        except Exception as e:
            # Fall back to legacy approach if MERGE fails and fallback is enabled
            logger.warning(f"MERGE operation failed, falling back to legacy approach: {e}")
            use_merge = False

    # Legacy two-step approach for older PostgreSQL versions or when MERGE fails
    return _bulk_upsert_models_legacy(
        models=models,
        table_name=table_name,
        loading_table_name=loading_table_name,
        pk_fields=pk_fields,
        update_fields=update_fields,
        insert_fields=insert_fields,
        compare_fields=compare_fields,
        update_where=update_where,
        update_if_null_field_names=update_if_null_field_names,
        model_meta=model_meta,
        return_models=return_models,
        use_unlogged=use_unlogged,
    )


def _bulk_upsert_models_with_merge(
    models: Sequence[Model],
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[Field],
    update_fields: Sequence[Field],
    insert_fields: Sequence[Field],
    compare_fields: Sequence[Field],
    update_where: Callable[[Sequence[Field], str, str], Composable] = None,
    update_if_null_field_names: Sequence[str] = None,
    model_meta = None,
    return_models: bool = False,
):
    """
    UPSERT implementation using PostgreSQL 15+ MERGE statement for improved performance.
    """
    update_if_null_fields = get_fields_from_names(update_if_null_field_names or [], model_meta)
    
    if return_models:
        # Use MERGE with RETURNING for returning models
        merge_query = generate_merge_upsert_with_returning(
            table_name=table_name,
            loading_table_name=loading_table_name,
            pk_fields=pk_fields,
            update_fields=update_fields,
            insert_fields=insert_fields,
            compare_fields=compare_fields,
            update_where=update_where,
            update_if_null_fields=update_if_null_fields,
            include_action=True,  # Include merge_action() for debugging
        )
        
        # Additional SELECT to ensure we return ALL models, not just modified ones
        select_query = generate_select_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            join_fields=pk_fields,
        )
        
        queries = [merge_query, select_query]
    else:
        # Simple MERGE without RETURNING
        merge_query = generate_merge_upsert_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            pk_fields=pk_fields,
            update_fields=update_fields,
            insert_fields=insert_fields,
            compare_fields=compare_fields,
            update_where=update_where,
            update_if_null_fields=update_if_null_fields,
        )
        
        queries = [merge_query]

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        load_queries=queries,
        return_models=return_models,
    )


def _bulk_upsert_models_legacy(
    models: Sequence[Model],
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[Field],
    update_fields: Sequence[Field],
    insert_fields: Sequence[Field],
    compare_fields: Sequence[Field],
    update_where: Callable[[Sequence[Field], str, str], Composable] = None,
    update_if_null_field_names: Sequence[str] = None,
    model_meta = None,
    return_models: bool = False,
    use_unlogged: bool = False,
):
    """
    Legacy UPSERT implementation using two-step UPDATE + INSERT approach.
    Used for PostgreSQL versions < 15 or as fallback when MERGE fails.
    """
    queries = []

    if update_fields:
        queries.append(
            generate_update_query(
                table_name=table_name,
                compare_fields=compare_fields,
                update_fields=update_fields,
                pk_fields=pk_fields,
                update_where=update_where,
                update_if_null_fields=get_fields_from_names(
                    update_if_null_field_names or [], model_meta
                ),
                loading_table_name=loading_table_name,
            )
        )

    insert_query = generate_insert_for_update_query(
        table_name=table_name,
        loading_table_name=loading_table_name,
        insert_fields=insert_fields,
        pk_fields=pk_fields,
    )

    if return_models:
        # Since we want to return ALL models (not just the ones actually inserted), we
        # need to run an additional select on all of the models in the loading table
        insert_query = add_returning(insert_query, table_name=table_name)
        select_query = generate_select_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            join_fields=pk_fields,
        )
        queries.append(select_query)

    queries.append(insert_query)

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        load_queries=queries,
        return_models=return_models,
        use_unlogged=use_unlogged,
    )


def _bulk_upsert_models_with_copy(
    models: Sequence[Model],
    pk_field_names: Sequence[str] = None,
    insert_only_field_names: Sequence[str] = None,
    model_changed_field_names: Sequence[str] = None,
    update_if_null_field_names: Sequence[str] = None,
    update_where: Callable[[Sequence[Field], str, str], Composable] = None,
    return_models: bool = False,
):
    """
    Fastest UPSERT implementation using PostgreSQL COPY command.
    Loads data into a temp table using COPY, then uses INSERT ... ON CONFLICT.
    """
    if not models:
        return [] if return_models else None

    insert_only_field_names = insert_only_field_names or []
    model_changed_field_names = model_changed_field_names or []
    update_if_null_field_names = update_if_null_field_names or []
    model_meta = models[0]._meta
    table_name = model_meta.db_table
    fields, field_names = get_fields_and_names(None, model_meta)

    pk_fields = get_pk_fields(pk_field_names, model_meta)
    pk_field_names = [field.name for field in pk_fields]
    ignore_on_compare = {
        *insert_only_field_names,
        *model_changed_field_names,
        *pk_field_names,
    }
    compare_fields = [field for field in fields if field.name not in ignore_on_compare]
    ignore_on_update = {
        *insert_only_field_names,
        *pk_field_names,
        *update_if_null_field_names,
    }
    update_fields = [field for field in fields if field.name not in ignore_on_update]
    insert_fields = [field for field in fields if not isinstance(field, AutoField)]
    loading_table_name = generate_table_name(table_name)

    # Use the existing optimized bulk_load_models_with_queries infrastructure
    # Generate the INSERT ... ON CONFLICT query
    insert_query = generate_insert_for_update_query(
        table_name=table_name,
        loading_table_name=loading_table_name,
        insert_fields=insert_fields,
        pk_fields=pk_fields,
    )

    if return_models:
        # Since we want to return ALL models (not just the ones actually inserted), we
        # need to run an additional select on all of the models in the loading table
        insert_query = add_returning(insert_query, table_name=table_name)
        select_query = generate_select_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            join_fields=pk_fields,
        )
        queries = [insert_query, select_query]
    else:
        queries = [insert_query]

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        load_queries=queries,
        return_models=return_models,
    )


def _get_postgres_type(field):
    """Get PostgreSQL type for a Django field."""
    # Try to get the database type from the field
    try:
        if hasattr(field, 'db_type'):
            # Create a minimal connection context for db_type
            from django.db import connection
            db_type = field.db_type(connection)
            if db_type:
                return db_type
    except Exception:
        pass
    
    # Fallback type mapping
    type_mapping = {
        'AutoField': 'bigint',
        'BigAutoField': 'bigint',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'SmallIntegerField': 'smallint',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'TextField': 'text',
        'CharField': f'varchar({field.max_length})',
        'DateTimeField': 'timestamptz',
        'DateField': 'date',
        'TimeField': 'time',
        'BooleanField': 'boolean',
        'FloatField': 'double precision',
        'DecimalField': 'numeric',
        'JSONField': 'jsonb',
        'BinaryField': 'bytea',
        'ForeignKey': 'bigint',
    }
    
    field_type = field.__class__.__name__
    return type_mapping.get(field_type, 'text')


def bulk_insert_changed_models(
    models: Sequence[Model],
    pk_field_names: Sequence[str],
    compare_field_names: Sequence[str],
    order_field_name=None,
    return_models=None,
):
    """
    INSERTs a new record in the database when a model field has changed in any of `compare_field_names`,
    with respect to its latest state, where "latest" is defined by ordering the records
    for a given primary key by sorting in descending order on the column passed in
    `order_field_name`. Does not INSERT a new record if the latest record has not changed.

    :param models: Django model list/tuple
    :param pk_field_names: Fields used to match existing models in the DB. By default uses model primary key.
    :param order_field_name: Field to determine the latest record (normally an AutoField or last_modified datetime type field)
    :param compare_field_names: Fields to compare. If the values are different, insert a new DB record
    :param return_models: Query and return the models in the DB, whether updated or not.
    Defaults to False, since this can significantly degrade performance
    :return: None or List[Model] depending upon returns_models param. Returns all models passed in,
    not just ones inserted. Models will not be in the same order they were passed in
    """
    if not models:
        logger.warning("No models passed to bulk_insert_changed_models")
        return [] if return_models else None

    model_meta = models[0]._meta
    table_name = model_meta.db_table

    order_field = (
        model_meta.get_field(order_field_name) if order_field_name else model_meta.pk
    )
    fields, field_names = get_fields_and_names(None, model_meta)
    pk_fields = get_pk_fields(pk_field_names, model_meta)

    if order_field in pk_fields:
        raise ValueError(
            "pk_field_names contains order_field_name. That will cause this function to always insert and not behave"
            "as expected."
        )

    pk_field_names = [field.name for field in pk_fields]
    not_allowed_on_compare = {*pk_field_names, order_field.name}
    for field_name in compare_field_names:
        if field_name in not_allowed_on_compare:
            raise ValueError(
                f"Field {field_name}' is not not allowed in compare_field_names, since it's in pk_field_names"
                f" or the order_field"
            )

    compare_fields = get_fields_from_names(compare_field_names, model_meta)
    insert_fields = [field for field in fields if not isinstance(field, AutoField)]
    loading_table_name = generate_table_name(table_name)

    insert_query = generate_insert_on_not_match_latest(
        table_name=table_name,
        loading_table_name=loading_table_name,
        insert_fields=insert_fields,
        compare_fields=compare_fields,
        order_field=order_field,
        pk_fields=pk_fields,
    )
    if return_models:
        select_query = generate_select_latest(
            table_name=table_name,
            loading_table_name=loading_table_name,
            pk_fields=pk_fields,
            order_field=order_field,
        )
        queries = [insert_query, select_query]
    else:
        queries = [insert_query]

    return bulk_load_models_with_queries(
        models=models,
        loading_table_name=loading_table_name,
        field_names=None,
        load_queries=queries,
        return_models=return_models,
    )


def bulk_select_model_dicts(
    *,
    model_class: Type[Model],
    filter_field_names: Iterable[str],
    select_field_names: Iterable[str],
    filter_data: Iterable[Sequence],
    skip_filter_transform=False,
    select_for_update=False
) -> List[Dict]:
    """
    Select/Get model dictionaries by filter_field_names. It returns dictionaries, not Django
    models for performance reasons. This is useful when querying a very large set of models
    or multiple field IN clauses.

    :param model_class: Model class to query. For instance django.contrib.auth.models.User
    :param filter_field_names: Fields to use in the query.
    :param select_field_names: The fields to return in the result dictionaries. The dictionaries will always contain
    the filter_field_names keys in addition to any fields in select_field_names
    :param filter_data: Values (normally tuples) of the filter_field_names. For instance if filter_field_names=["field1", "field2"],
    filter_data may be [(12, "hello"), (23, "world"), (35, "fun"), ...]
    :param skip_filter_transform: Normally the function converts the filter_data into DB specific values. This is useful
    for datetimes or other complex values that have different representation in the DB. The downside is the transform
    can be slow. If you know your data is simple values (strings, integers, etc.) and don't need
    transformation, you can pass True.
    :param select_for_update: Use `FOR UPDATE` clause in select query. This will lock the rows.

    :return: List of dictionaries that match the model_data. Returns dictionaries for performance reasons
    """
    if not filter_data:
        return []

    model_meta = model_class._meta
    table_name = model_meta.db_table

    # Assume that all dicts have the same fields
    filter_fields = get_fields_from_names(filter_field_names, model_meta)

    # Add the query fields to the select fields (if they aren't already), so they can link the query to results
    select_field_names = {*select_field_names, *filter_field_names}
    select_fields = get_fields_from_names(select_field_names, model_meta)

    start_time = monotonic()
    db_name = router.db_for_read(model_class)
    connection = connections[db_name]

    with connection.cursor() as cursor:
        # Grab all the filter data, so we can know the length
        filter_data = list(filter_data)
        if not skip_filter_transform:
            filter_data_transformed = []
            for filter_vals in filter_data:
                filter_data_transformed.append(
                    [
                        django_field_to_query_value(filter_fields[i], value)
                        for i, value in enumerate(filter_vals)
                    ]
                )
            filter_data = filter_data_transformed

        sql = generate_values_select_query(
            table_name=table_name,
            select_fields=select_fields,
            filter_fields=filter_fields,
            select_for_update=select_for_update
        )
        sql_string = sql.as_string(cursor.connection)

        logger.info(
            "Starting selecting models",
            extra=dict(query_dict_count=len(filter_data), table_name=table_name),
        )
        execute_values(cursor, sql_string, filter_data, page_size=len(filter_data))
        columns = [col[0] for col in cursor.description]

        # Map columns to fields so we can later correctly interpret column values
        select_field_map = {field.column: field for field in select_fields}
        results = []
        for row in cursor.fetchall():
            results.append(
                {
                    select_field_map[column]
                    .attname: select_field_map[column]
                    .from_db_value(value, expression=None,  connection=connection)
                    if hasattr(select_field_map[column], "from_db_value")
                    else value
                    for column, value in zip(columns, row)
                }
            )

        logger.info(
            "Finished querying models",
            extra=dict(
                result_count=len(results),
                table_name=table_name,
                duration=monotonic() - start_time,
            ),
        )

        return results
