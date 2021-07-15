from typing import Sequence

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
            # wrap column names in double quotes to handle columns starting with a number
            SQL(
                "{source_table_name}.{column} IS DISTINCT FROM {destination_table_name}.{column}"
            ).format(
                source_table_name=Identifier(source_table_name),
                column=Identifier(field.column),
                destination_table_name=Identifier(destination_table_name),
            )
        )
    return SQL(" OR ").join(conditions)


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
    distinct_from_clause = generate_distinct_condition(
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
            distinct_from_clause = SQL(" OR ").join(
                [distinct_from_clause, distinct_null_clause]
            )
        else:
            distinct_from_clause = distinct_null_clause

    return SQL(
        "UPDATE {table_name} SET {update_clause} FROM {loading_table_name} WHERE {where_clause}"
    ).format(
        table_name=Identifier(table_name),
        update_clause=update_clause,
        loading_table_name=Identifier(loading_table_name),
        where_clause=SQL("({distinct_from_clause}) AND ({join_clause})").format(
            distinct_from_clause=distinct_from_clause, join_clause=join_clause
        ),
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
):
    select_fields_sql = SQL(", ").join(
        [Identifier(field.column) for field in select_fields]
    )

    filter_fields_sql = SQL(", ").join(
        [Identifier(field.column) for field in filter_fields]
    )

    return SQL(
        "SELECT {select_fields_sql} from {table_name} where ({filter_fields_sql}) IN (VALUES %s)"
    ).format(
        table_name=Identifier(table_name),
        select_fields_sql=select_fields_sql,
        filter_fields_sql=filter_fields_sql,
    )
