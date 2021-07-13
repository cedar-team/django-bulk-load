from django.test import TestCase
from django_bulk_load import bulk_load_models_with_queries
from django_bulk_load.django import get_fields_from_names
from django_bulk_load.queries import (
    SQL,
    Identifier,
    add_returning,
    generate_insert_for_update_query,
    generate_update_query,
)
from .test_project.models import TestComplexModel
from django_bulk_load.utils import generate_table_name


class E2ETestBulkLoadModelsWithQueries(TestCase):
    def test_error_without_select_or_returning(self):
        models = [TestComplexModel(integer_field=1)]
        model_meta = models[0]._meta
        table_name = model_meta.db_table
        loading_table_name = generate_table_name(table_name)
        insert_query = generate_insert_for_update_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            insert_fields=get_fields_from_names(["integer_field"], model_meta),
            pk_fields=get_fields_from_names(["id"], model_meta),
        )

        with self.assertRaises(ValueError):
            bulk_load_models_with_queries(
                models=models,
                load_queries=[insert_query],
                loading_table_name=loading_table_name,
                field_names=["id", "integer_field"],
                return_models=True,
            )

    def test_empty_select_no_error(self):
        models = [TestComplexModel(integer_field=1)]
        model_meta = models[0]._meta
        table_name = model_meta.db_table
        loading_table_name = generate_table_name(table_name)
        insert_query = generate_insert_for_update_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            insert_fields=get_fields_from_names(["integer_field"], model_meta),
            pk_fields=get_fields_from_names(["id"], model_meta),
        )

        # This select should always be empty
        select_query = SQL("SELECT * FROM {table_name} where id=-1").format(
            table_name=Identifier(table_name)
        )

        bulk_load_models_with_queries(
            models=models,
            load_queries=[insert_query, select_query],
            loading_table_name=loading_table_name,
            field_names=["id", "integer_field"],
            return_models=True,
        )

    def test_empty_returning_no_error(self):
        models = [TestComplexModel(integer_field=1)]
        model_meta = models[0]._meta
        table_name = model_meta.db_table
        loading_table_name = generate_table_name(table_name)
        fields = get_fields_from_names(["integer_field"], model_meta)
        update_query = add_returning(
            generate_update_query(
                table_name=table_name,
                loading_table_name=loading_table_name,
                update_fields=fields,
                compare_fields=fields,
                pk_fields=get_fields_from_names(["id"], model_meta),
            ),
            table_name,
        )

        bulk_load_models_with_queries(
            models=models,
            load_queries=[update_query],
            loading_table_name=loading_table_name,
            field_names=["id", "integer_field"],
            return_models=True,
        )
