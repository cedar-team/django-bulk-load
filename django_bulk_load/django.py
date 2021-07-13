import csv
from io import StringIO
from typing import Any, Iterable, List, NamedTuple, Optional, Tuple, Type

from django.db import models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models.options import Options
from psycopg2.extras import Json

from .utils import NULL_CHARACTER


def django_field_to_query_value(field, value):
    return field.get_prep_value(value)


class DjangoFieldInfo(NamedTuple):
    field_name: str
    deserializer: callable


def records_to_models(
    records: List[List[str]], columns: List[str], model_class: Type[models.Model]
) -> List[models.Model]:
    # TODO: Replace this deserialization logic with logic matching
    #  django.db.models.sql.compiler.SQLCompiler.get_converters
    django_fields = {
        field.column: DjangoFieldInfo(
            field_name=field.attname,
            deserializer=(
                field.from_db_value
                if hasattr(field, "from_db_value")
                else lambda value, expression, connection: value
            ),
        )
        for field in get_model_fields(model_class._meta, include_auto_fields=True)
    }

    results = []
    for row in records:
        zipped_results = dict(zip(columns, row))
        attrs = {
            django_field_info.field_name: (
                django_field_info.deserializer(zipped_results[column], None, None)
                if column in zipped_results
                else models.DEFERRED
            )
            for column, django_field_info in django_fields.items()
        }
        results.append(model_class(**attrs))

    return results


def _default_model_to_value(model, field, connection):
    field_val = field.pre_save(model, add=model.pk is None)
    return field.get_db_prep_save(field_val, connection=connection)


def models_to_tsv_buffer(
    models: Iterable[Any],
    include_fields: Iterable[models.Field],
    connection: BaseDatabaseWrapper,
    django_field_to_value=_default_model_to_value,
) -> StringIO:
    buffer = StringIO()
    tsv_writer = csv.writer(buffer, delimiter="\t", lineterminator="\n")
    for obj in models:
        row = []
        for include_field in include_fields:
            field_val = django_field_to_value(obj, include_field, connection)
            if field_val is None:
                row.append(NULL_CHARACTER)
            elif isinstance(field_val, Json):
                row.append(field_val.dumps(field_val.adapted))
            else:
                row.append(str(field_val))
        tsv_writer.writerow(row)
    buffer.seek(0)
    return buffer


def get_model_fields(
    model_meta: Options, include_auto_fields=False
) -> List[models.Field]:
    fields = []
    for field in model_meta.get_fields():
        if (
            getattr(field, "column", None)
            and (include_auto_fields or not isinstance(field, models.AutoField))
            and not isinstance(field, models.ManyToManyField)
        ):
            fields.append(field)

    return fields


def get_fields_from_names(
    field_names: Iterable[str], model_meta: Options
) -> List[models.Field]:
    return [model_meta.get_field(field_name) for field_name in field_names]


def get_fields_and_names(
    field_names: Optional[Iterable[str]], model_meta: Options, include_auto_fields=False
) -> Tuple[List[models.Field], List[str]]:
    if field_names is None:
        fields = get_model_fields(model_meta, include_auto_fields=include_auto_fields)
    else:
        fields = get_fields_from_names(field_names, model_meta)

    return fields, [field.name for field in fields]


def get_pk_fields(
    pk_field_names: Iterable[str], model_meta: Options
) -> List[models.Field]:
    if pk_field_names:
        return get_fields_from_names(pk_field_names, model_meta)

    return [model_meta.pk]
