from typing import Sequence, Callable

from django.db import models
from psycopg2.sql import SQL, Composable, Composed, Identifier


def create_temp_table(temp_table_name, source_table_name, column_names):
    return SQL(
        "CREATE TEMPORARY TABLE {temp_table_name} ON COMMIT DROP AS "
        "SELECT {column_list} FROM {source_table_name} WITH NO DATA"
    ).format(
        source_table_name=Identifier(source_table_name),
        temp_table_name=Identifier(temp_table_name),
        column_list=SQL(", ").join(
            [Identifier(column_name) for column_name in column_names]
        ),
    )

def copy_query(table_name: str):
    return SQL("COPY {table_name} FROM STDIN NULL '\\N' DELIMITER '\t' CSV").format(
        table_name=Identifier(table_name)
    )

def add_returning(query: Composable, table_name: str) -> Composable:
    return Composed(
        [
            query,
            SQL(" RETURNING {table_name}.*").format(table_name=Identifier(table_name)),
        ]
    )


def generate_join_condition(
    source_table_name: str, destination_table_name: str, fields: Sequence[models.Field]
) -> Composable:
    conditions = []
    for field in fields:
        conditions.append(
            SQL(
                "{source_table_name}.{column} = {destination_table_name}.{column}"
            ).format(
                source_table_name=Identifier(source_table_name),
                column=Identifier(field.column),
                destination_table_name=Identifier(destination_table_name),
            )
        )
    return SQL(" AND ").join(conditions)


def generate_distinct_condition(
    source_table_name: str,
    destination_table_name: str,
    compare_fields: Sequence[models.Field],
) -> Composable:
    conditions = []
    for field in compare_fields:
        conditions.append(
            SQL(
                "{source_table_name}.{column} IS DISTINCT FROM {destination_table_name}.{column}"
            ).format(
                source_table_name=Identifier(source_table_name),
                column=Identifier(field.column),
                destination_table_name=Identifier(destination_table_name),
            )
        )
    return SQL(" OR ").join(conditions)


def generate_greater_than_condition(
    source_table_name: str,
    destination_table_name: str,
    field: models.Field,
) -> Composable:
    return SQL(
        "{source_table_name}.{column} > {destination_table_name}.{column}"
    ).format(
        source_table_name=Identifier(source_table_name),
        column=Identifier(field.column),
        destination_table_name=Identifier(destination_table_name),
    )


def generate_distinct_null_condition(
    source_table_name: str,
    destination_table_name: str,
    compare_fields: Sequence[models.Field],
) -> Composable:
    conditions = []
    for field in compare_fields:
        conditions.append(
            SQL(
                "{source_table_name}.{column} IS DISTINCT FROM {destination_table_name}.{column} AND ({source_table_name}.{column} IS NULL OR {destination_table_name}.{column} IS NULL)"
            ).format(
                source_table_name=Identifier(source_table_name),
                column=Identifier(field.column),
                destination_table_name=Identifier(destination_table_name),
            )
        )
    return SQL(" OR ").join(conditions)


def generate_insert_query(
    table_name: str,
    loading_table_name: str,
    ignore_conflicts: bool,
    insert_fields: Sequence[models.Field],
) -> Composable:
    """
    Generate a query for INSERTing records from one table to another
    :param table_name: Name of the table to insert to
    :param loading_table_name: Name of the table to load from
    :param ignore_conflicts: Ignore records that cannot be inserted because of unique constraint
    :param insert_fields: Fields to insert on the table
    :return:
    """
    query = SQL(
        "INSERT INTO {table_name} ({insert_column_list}) "
        "SELECT {select_column_list} FROM {loading_table_name}"
    ).format(
        table_name=Identifier(table_name),
        insert_column_list=SQL(", ").join(
            Identifier(field.column) for field in insert_fields
        ),
        select_column_list=SQL(", ").join(
            SQL(".").join((Identifier(loading_table_name), Identifier(x.column)))
            for x in insert_fields
        ),
        loading_table_name=Identifier(loading_table_name),
    )

    if ignore_conflicts:
        return Composed(
            [
                query,
                SQL(" ON CONFLICT DO NOTHING"),
            ]
        )

    return query

def generate_insert_for_update_query(
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[models.Field],
    insert_fields: Sequence[models.Field],
) -> Composable:
    join_clause = generate_join_condition(loading_table_name, table_name, pk_fields)
    return SQL(
        "INSERT INTO {table_name} ({insert_column_list}) "
        "SELECT {select_column_list} FROM {loading_table_name} "
        "LEFT JOIN {table_name} ON {join_clause} "
        "WHERE {table_name}.{pk_column} IS NULL"
    ).format(
        table_name=Identifier(table_name),
        insert_column_list=SQL(", ").join(
            Identifier(field.column) for field in insert_fields
        ),
        select_column_list=SQL(", ").join(
            SQL(".").join((Identifier(loading_table_name), Identifier(x.column)))
            for x in insert_fields
        ),
        loading_table_name=Identifier(loading_table_name),
        join_clause=join_clause,
        pk_column=Identifier(pk_fields[0].column),
    )



def generate_select_latest(table_name, loading_table_name, pk_fields, order_field):
    join_clause = generate_join_condition(loading_table_name, table_name, pk_fields)
    return SQL(
        "SELECT DISTINCT ON ({pk_columns}) {table_name}.* from {table_name} INNER JOIN {loading_table_name} ON {join_clause} "
        "ORDER BY {pk_columns}, {table_name}.{order_column} DESC"
    ).format(
        table_name=Identifier(table_name),
        loading_table_name=Identifier(loading_table_name),
        join_clause=join_clause,
        pk_columns=SQL(", ").join(
            SQL("{table_name}.{column}").format(
                table_name=Identifier(table_name), column=Identifier(f.column)
            )
            for f in pk_fields
        ),
        order_column=Identifier(order_field.column),
    )


def generate_insert_on_not_match_latest(
    table_name,
    loading_table_name,
    pk_fields,
    compare_fields,
    order_field,
    insert_fields,
):
    join_clause = generate_join_condition(loading_table_name, table_name, pk_fields)
    select_query = generate_select_latest(
        table_name=table_name,
        loading_table_name=loading_table_name,
        pk_fields=pk_fields,
        order_field=order_field,
    )
    return SQL(
        "INSERT INTO {table_name} ({insert_column_list}) "
        "SELECT {select_column_list} FROM {loading_table_name} "
        "LEFT JOIN ({select_query}) {table_name} ON {join_clause} "
        "WHERE {table_name}.{pk_column} IS NULL OR {distinct_clause}"
    ).format(
        table_name=Identifier(table_name),
        loading_table_name=Identifier(loading_table_name),
        insert_column_list=SQL(", ").join(Identifier(x.column) for x in insert_fields),
        select_column_list=SQL(", ").join(
            SQL("{table_name}.{column}").format(
                table_name=Identifier(loading_table_name), column=Identifier(x.column)
            )
            for x in insert_fields
        ),
        select_query=select_query,
        join_clause=join_clause,
        pk_column=Identifier(pk_fields[0].column),
        distinct_clause=generate_distinct_condition(
            loading_table_name, table_name, compare_fields
        ),
    )


def generate_update_query(
    *,
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[models.Field],
    update_fields: Sequence[models.Field],
    compare_fields: Sequence[models.Field],
    update_where: Callable[[Sequence[models.Field], str, str], Composable] = None,
    update_if_null_fields: Sequence[models.Field] = None,
) -> Composable:
    update_if_null_fields = update_if_null_fields or []

    update_conditions = []
    for field in update_fields:
        if not getattr(field, "auto_now_add", False) and not isinstance(
            field, models.AutoField
        ):
            update_conditions.append(
                SQL("{column} = {loading_table_name}.{column}").format(
                    column=Identifier(field.column),
                    loading_table_name=Identifier(loading_table_name),
                )
            )

    for field in update_if_null_fields:
        update_conditions.append(
            SQL(
                "{column} = CASE "
                "WHEN {loading_table_name}.{column} IS NULL OR "
                "{table_name}.{column} IS NULL THEN {loading_table_name}.{column}"
                "ELSE {table_name}.{column} END"
            ).format(
                column=Identifier(field.column),
                table_name=Identifier(table_name),
                loading_table_name=Identifier(loading_table_name),
            )
        )
    update_clause = SQL(", ").join(update_conditions)
    join_clause = generate_join_condition(
        source_table_name=loading_table_name,
        destination_table_name=table_name,
        fields=pk_fields,
    )

    if update_where:
        where_clause = update_where(update_fields, loading_table_name, table_name)
    else:
        where_clause = generate_distinct_condition(
            source_table_name=loading_table_name,
            destination_table_name=table_name,
            compare_fields=compare_fields,
        )

        if update_if_null_fields:
            distinct_null_clause = generate_distinct_null_condition(
                source_table_name=loading_table_name,
                destination_table_name=table_name,
                compare_fields=update_if_null_fields,
            )
            if compare_fields:
                where_clause = SQL(" OR ").join(
                    [where_clause, distinct_null_clause]
                )
            else:
                where_clause = distinct_null_clause

    return SQL(
        "UPDATE {table_name} SET {update_clause} FROM {loading_table_name} WHERE {where_clause}"
    ).format(
        table_name=Identifier(table_name),
        update_clause=update_clause,
        loading_table_name=Identifier(loading_table_name),
        where_clause=SQL("({where_clause}) AND ({join_clause})").format(
            where_clause=where_clause, join_clause=join_clause
        ) if where_clause else join_clause
    )


def generate_select_query(
    table_name: str,
    loading_table_name: str,
    join_fields: Sequence[models.Field],
    select_fields: Sequence[models.Field] = None,
) -> Composable:
    join_clause = generate_join_condition(
        source_table_name=loading_table_name,
        destination_table_name=table_name,
        fields=join_fields,
    )
    if select_fields:
        fields = SQL(", ").join(
            [
                SQL("{table_name}.{column_name}").format(
                    table_name=Identifier(table_name),
                    column_name=Identifier(field.column),
                )
                for field in select_fields
            ]
        )
    else:
        fields = SQL("{table_name}.*").format(table_name=Identifier(table_name))

    return SQL(
        "SELECT {fields} FROM {table_name} INNER JOIN {loading_table_name} ON {join_clause}"
    ).format(
        loading_table_name=Identifier(loading_table_name),
        join_clause=join_clause,
        fields=fields,
        table_name=Identifier(table_name),
    )


def generate_values_select_query(
    table_name: str,
    filter_fields: Sequence[models.Field],
    select_fields: Sequence[models.Field],
    select_for_update: bool
):
    select_fields_sql = SQL(", ").join(
        [Identifier(field.column) for field in select_fields]
    )

    filter_fields_sql = SQL(", ").join(
        [Identifier(field.column) for field in filter_fields]
    )

    for_update = SQL("")
    if select_for_update:
        for_update = SQL(" FOR UPDATE")

    return SQL(
        "SELECT {select_fields_sql} from {table_name} where ({filter_fields_sql}) IN (VALUES %s){for_update}"
    ).format(
        table_name=Identifier(table_name),
        select_fields_sql=select_fields_sql,
        filter_fields_sql=filter_fields_sql,
        for_update=for_update,
    )


def add_merge_returning(query: Composable, table_name: str, action: bool = True) -> Composable:
    """
    Add RETURNING clause to MERGE query with optional merge_action() support.
    
    :param query: The MERGE query to add RETURNING to
    :param table_name: Name of the target table
    :param action: Whether to include merge_action() in the return
    :return: MERGE query with RETURNING clause
    """
    if action:
        return Composed(
            [
                query,
                SQL(" RETURNING merge_action(), {table_name}.*").format(table_name=Identifier(table_name)),
            ]
        )
    else:
        return Composed(
            [
                query,
                SQL(" RETURNING {table_name}.*").format(table_name=Identifier(table_name)),
            ]
        )


def generate_merge_upsert_with_returning(
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[models.Field],
    update_fields: Sequence[models.Field],
    insert_fields: Sequence[models.Field],
    compare_fields: Sequence[models.Field] = None,
    update_where: Callable[[Sequence[models.Field], str, str], Composable] = None,
    update_if_null_fields: Sequence[models.Field] = None,
    include_action: bool = True,
) -> Composable:
    """
    Generate a MERGE query with RETURNING clause for efficient upsert operations.
    
    :param table_name: Name of the target table
    :param loading_table_name: Name of the source/loading table
    :param pk_fields: Primary key fields for matching
    :param update_fields: Fields to update when matched
    :param insert_fields: Fields to insert when not matched
    :param compare_fields: Fields to compare for determining if update is needed
    :param update_where: Custom WHERE condition function for updates
    :param update_if_null_fields: Fields that only get updated if NULL
    :param include_action: Whether to include merge_action() in RETURNING clause
    :return: MERGE query with RETURNING as Composable
    """
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
    
    return add_merge_returning(merge_query, table_name, action=include_action)


def generate_merge_upsert_query(
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[models.Field],
    update_fields: Sequence[models.Field],
    insert_fields: Sequence[models.Field],
    compare_fields: Sequence[models.Field] = None,
    update_where: Callable[[Sequence[models.Field], str, str], Composable] = None,
    update_if_null_fields: Sequence[models.Field] = None,
) -> Composable:
    """
    Generate a MERGE query for efficient upsert operations using PostgreSQL 15+ MERGE statement.
    This replaces the traditional two-step UPDATE + INSERT approach with a single atomic operation.
    
    :param table_name: Name of the target table
    :param loading_table_name: Name of the source/loading table
    :param pk_fields: Primary key fields for matching
    :param update_fields: Fields to update when matched
    :param insert_fields: Fields to insert when not matched
    :param compare_fields: Fields to compare for determining if update is needed
    :param update_where: Custom WHERE condition function for updates
    :param update_if_null_fields: Fields that only get updated if NULL
    :return: MERGE query as Composable
    """
    update_if_null_fields = update_if_null_fields or []
    compare_fields = compare_fields or []
    
    # Generate JOIN condition for matching rows
    join_clause = generate_join_condition(
        source_table_name=loading_table_name,
        destination_table_name=table_name,
        fields=pk_fields,
    )
    
    # Build UPDATE SET clause
    update_conditions = []
    for field in update_fields:
        if not getattr(field, "auto_now_add", False) and not isinstance(field, models.AutoField):
            update_conditions.append(
                SQL("{column} = {loading_table_name}.{column}").format(
                    column=Identifier(field.column),
                    loading_table_name=Identifier(loading_table_name),
                )
            )
    
    # Handle update_if_null_fields
    for field in update_if_null_fields:
        update_conditions.append(
            SQL(
                "{column} = CASE "
                "WHEN {loading_table_name}.{column} IS NULL OR "
                "{table_name}.{column} IS NULL THEN {loading_table_name}.{column} "
                "ELSE {table_name}.{column} END"
            ).format(
                column=Identifier(field.column),
                table_name=Identifier(table_name),
                loading_table_name=Identifier(loading_table_name),
            )
        )
    
    # Build INSERT clause
    insert_columns = SQL(", ").join(Identifier(field.column) for field in insert_fields)
    insert_values = SQL(", ").join(
        SQL("{loading_table_name}.{column}").format(
            loading_table_name=Identifier(loading_table_name),
            column=Identifier(field.column)
        ) for field in insert_fields
    )
    
    # Determine WHEN MATCHED condition
    when_matched_condition = SQL("")
    if update_where:
        when_matched_condition = SQL(" AND ({condition})").format(
            condition=update_where(update_fields, loading_table_name, table_name)
        )
    elif compare_fields:
        distinct_condition = generate_distinct_condition(
            source_table_name=loading_table_name,
            destination_table_name=table_name,
            compare_fields=compare_fields,
        )
        when_matched_condition = SQL(" AND ({condition})").format(
            condition=distinct_condition
        )
        
        if update_if_null_fields:
            distinct_null_condition = generate_distinct_null_condition(
                source_table_name=loading_table_name,
                destination_table_name=table_name,
                compare_fields=update_if_null_fields,
            )
            when_matched_condition = SQL(" AND (({distinct_condition}) OR ({null_condition}))").format(
                distinct_condition=distinct_condition,
                null_condition=distinct_null_condition
            )
    elif update_if_null_fields:
        distinct_null_condition = generate_distinct_null_condition(
            source_table_name=loading_table_name,
            destination_table_name=table_name,
            compare_fields=update_if_null_fields,
        )
        when_matched_condition = SQL(" AND ({condition})").format(
            condition=distinct_null_condition
        )
    
    # Build the complete MERGE statement
    merge_query = SQL(
        "MERGE INTO {table_name} "
        "USING {loading_table_name} "
        "ON {join_clause} "
        "WHEN MATCHED{when_matched_condition} THEN "
        "UPDATE SET {update_clause} "
        "WHEN NOT MATCHED THEN "
        "INSERT ({insert_columns}) VALUES ({insert_values})"
    ).format(
        table_name=Identifier(table_name),
        loading_table_name=Identifier(loading_table_name),
        join_clause=join_clause,
        when_matched_condition=when_matched_condition,
        update_clause=SQL(", ").join(update_conditions) if update_conditions else SQL(""),
        insert_columns=insert_columns,
        insert_values=insert_values,
    )
    
    return merge_query


def generate_merge_conditional_query(
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[models.Field],
    insert_fields: Sequence[models.Field],
    conditions: list,
) -> Composable:
    """
    Generate a MERGE query with multiple conditional WHEN clauses.
    This supports complex logic like conditional updates, deletes, and inserts.
    
    :param table_name: Name of the target table
    :param loading_table_name: Name of the source/loading table  
    :param pk_fields: Primary key fields for matching
    :param insert_fields: Fields to insert when not matched
    :param conditions: List of dictionaries defining WHEN conditions
                      Format: [{"type": "matched|not_matched", "condition": SQL, "action": "update|delete|insert", "fields": {...}}]
    :return: MERGE query as Composable
    """
    # Generate JOIN condition
    join_clause = generate_join_condition(
        source_table_name=loading_table_name,
        destination_table_name=table_name,
        fields=pk_fields,
    )
    
    # Build WHEN clauses
    when_clauses = []
    
    for condition in conditions:
        if condition["type"] == "matched":
            if condition["action"] == "update":
                update_conditions = []
                for field_name, value_expr in condition["fields"].items():
                    update_conditions.append(
                        SQL("{column} = {value}").format(
                            column=Identifier(field_name),
                            value=value_expr
                        )
                    )
                
                when_clause = SQL(
                    "WHEN MATCHED AND {condition} THEN UPDATE SET {update_clause}"
                ).format(
                    condition=condition["condition"],
                    update_clause=SQL(", ").join(update_conditions)
                )
                
            elif condition["action"] == "delete":
                when_clause = SQL(
                    "WHEN MATCHED AND {condition} THEN DELETE"
                ).format(condition=condition["condition"])
                
        elif condition["type"] == "not_matched":
            if condition["action"] == "insert":
                insert_columns = SQL(", ").join(Identifier(field.column) for field in insert_fields)
                insert_values = SQL(", ").join(
                    SQL("{loading_table_name}.{column}").format(
                        loading_table_name=Identifier(loading_table_name),
                        column=Identifier(field.column)
                    ) for field in insert_fields
                )
                
                when_clause = SQL(
                    "WHEN NOT MATCHED AND {condition} THEN INSERT ({insert_columns}) VALUES ({insert_values})"
                ).format(
                    condition=condition["condition"],
                    insert_columns=insert_columns,
                    insert_values=insert_values
                )
            elif condition["action"] == "nothing":
                when_clause = SQL(
                    "WHEN NOT MATCHED AND {condition} THEN DO NOTHING"
                ).format(condition=condition["condition"])
        
        when_clauses.append(when_clause)
    
    # Build the complete MERGE statement
    merge_query = SQL(
        "MERGE INTO {table_name} "
        "USING {loading_table_name} "
        "ON {join_clause} "
        "{when_clauses}"
    ).format(
        table_name=Identifier(table_name),
        loading_table_name=Identifier(loading_table_name),
        join_clause=join_clause,
        when_clauses=SQL(" ").join(when_clauses),
    )
    
    return merge_query


def generate_merge_update_query(
    table_name: str,
    loading_table_name: str,
    pk_fields: Sequence[models.Field],
    update_fields: Sequence[models.Field],
    compare_fields: Sequence[models.Field] = None,
    update_where: Callable[[Sequence[models.Field], str, str], Composable] = None,
    update_if_null_fields: Sequence[models.Field] = None,
) -> Composable:
    """
    Generate a MERGE query for efficient update-only operations using PostgreSQL 15+ MERGE statement.
    This is optimized for pure updates (no inserts) and can be faster than UPDATE ... FROM.
    
    :param table_name: Name of the target table
    :param loading_table_name: Name of the source/loading table
    :param pk_fields: Primary key fields for matching
    :param update_fields: Fields to update when matched
    :param compare_fields: Fields to compare for determining if update is needed
    :param update_where: Custom WHERE condition function for updates
    :param update_if_null_fields: Fields that only get updated if NULL
    :return: MERGE query as Composable
    """
    update_if_null_fields = update_if_null_fields or []
    compare_fields = compare_fields or []
    
    # Generate JOIN condition for matching rows
    join_clause = generate_join_condition(
        source_table_name=loading_table_name,
        destination_table_name=table_name,
        fields=pk_fields,
    )
    
    # Build UPDATE SET clause
    update_conditions = []
    for field in update_fields:
        if not getattr(field, "auto_now_add", False) and not isinstance(field, models.AutoField):
            update_conditions.append(
                SQL("{column} = {loading_table_name}.{column}").format(
                    column=Identifier(field.column),
                    loading_table_name=Identifier(loading_table_name),
                )
            )
    
    # Handle update_if_null_fields
    for field in update_if_null_fields:
        update_conditions.append(
            SQL(
                "{column} = CASE "
                "WHEN {loading_table_name}.{column} IS NULL OR "
                "{table_name}.{column} IS NULL THEN {loading_table_name}.{column} "
                "ELSE {table_name}.{column} END"
            ).format(
                column=Identifier(field.column),
                table_name=Identifier(table_name),
                loading_table_name=Identifier(loading_table_name),
            )
        )
    
    # Determine WHEN MATCHED condition
    when_matched_condition = SQL("")
    if update_where:
        when_matched_condition = SQL(" AND ({condition})").format(
            condition=update_where(update_fields, loading_table_name, table_name)
        )
    elif compare_fields:
        distinct_condition = generate_distinct_condition(
            source_table_name=loading_table_name,
            destination_table_name=table_name,
            compare_fields=compare_fields,
        )
        when_matched_condition = SQL(" AND ({condition})").format(
            condition=distinct_condition
        )
        
        if update_if_null_fields:
            distinct_null_condition = generate_distinct_null_condition(
                source_table_name=loading_table_name,
                destination_table_name=table_name,
                compare_fields=update_if_null_fields,
            )
            when_matched_condition = SQL(" AND (({distinct_condition}) OR ({null_condition}))").format(
                distinct_condition=distinct_condition,
                null_condition=distinct_null_condition
            )
    elif update_if_null_fields:
        distinct_null_condition = generate_distinct_null_condition(
            source_table_name=loading_table_name,
            destination_table_name=table_name,
            compare_fields=update_if_null_fields,
        )
        when_matched_condition = SQL(" AND ({condition})").format(
            condition=distinct_null_condition
        )
    
    # Build the MERGE statement for update-only operations
    merge_query = SQL(
        "MERGE INTO {table_name} "
        "USING {loading_table_name} "
        "ON {join_clause} "
        "WHEN MATCHED{when_matched_condition} THEN "
        "UPDATE SET {update_clause} "
        "WHEN NOT MATCHED THEN DO NOTHING"
    ).format(
        table_name=Identifier(table_name),
        loading_table_name=Identifier(loading_table_name),
        join_clause=join_clause,
        when_matched_condition=when_matched_condition,
        update_clause=SQL(", ").join(update_conditions) if update_conditions else SQL("id = id"),  # Dummy update if no conditions
    )
    
    return merge_query
