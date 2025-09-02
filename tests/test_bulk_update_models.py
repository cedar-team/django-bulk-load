from datetime import datetime, timezone

from django.test import TestCase
from django_bulk_load import bulk_update_models, generate_greater_than_condition
from django_bulk_load.queries import generate_update_query
from django_bulk_load.django import get_fields_from_names
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
)


class E2ETestBulkUpdateModels(TestCase):
    def test_integer_field_change(self):
        model1 = TestComplexModel(integer_field=1)
        model1.save()
        model1.integer_field = 2
        bulk_update_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.integer_field, 2)

    def test_string_field_change(self):
        model1 = TestComplexModel(string_field="hello")
        model1.save()
        model1.string_field = "world"
        bulk_update_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.string_field, "world")

    def test_datetime_field_change(self):
        model1 = TestComplexModel(
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        )
        model1.save()
        model1.datetime_field = datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc)
        bulk_update_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(
            saved_model.datetime_field,
            datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc),
        )

    def test_json_field_change(self):
        model1 = TestComplexModel(json_field=dict(a="b"))
        model1.save()
        model1.json_field = dict(c="d")
        bulk_update_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.json_field, dict(c="d"))

    def test_update_all_fields(self):
        foreign1 = TestForeignKeyModel()
        foreign1.save()
        foreign2 = TestForeignKeyModel()
        foreign2.save()
        model1 = TestComplexModel(
            integer_field=1,
            string_field="hello",
            json_field=dict(a="b"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign1,
        )
        model1.save()
        model1.integer_field = 2
        model1.string_field = "world"
        model1.json_field = dict(c="d")
        model1.test_foreign = foreign2
        model1.datetime_field = datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc)
        bulk_update_models([model1])
        saved_model = TestComplexModel.objects.get()

        self.assertEqual(saved_model.integer_field, 2)
        self.assertEqual(saved_model.string_field, "world")
        self.assertEqual(saved_model.json_field, dict(c="d"))
        self.assertEqual(saved_model.test_foreign_id, foreign2.id)
        self.assertEqual(
            saved_model.datetime_field,
            datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc),
        )

    def test_update_specific(self):
        foreign1 = TestForeignKeyModel()
        foreign1.save()
        foreign2 = TestForeignKeyModel()
        foreign2.save()
        model1 = TestComplexModel(
            integer_field=1,
            string_field="hello",
            json_field=dict(a="b"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign1,
        )
        model1.save()
        model1.integer_field = 2
        model1.string_field = "world"
        model1.json_field = dict(c="d")
        model1.test_foreign = foreign2
        model1.datetime_field = datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc)
        bulk_update_models([model1], update_field_names=["string_field"])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.string_field, "world")

        # Should keep the following fields unchanged, since field_names is specified
        self.assertEqual(saved_model.integer_field, 1)
        self.assertEqual(saved_model.json_field, dict(a="b"))
        self.assertEqual(saved_model.test_foreign_id, foreign1.id)
        self.assertEqual(
            saved_model.datetime_field,
            datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

    def test_update_on_changed_no_change(self):
        foreign1 = TestForeignKeyModel()
        foreign1.save()
        model1 = TestComplexModel(
            integer_field=1,
            string_field="hello",
            json_field=dict(a="b"),
            datetime_field=datetime(1987, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign1,
        )
        model1.save()
        model1.integer_field = 2
        model1.datetime_field = datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        bulk_update_models(
            [model1], model_changed_field_names=["integer_field", "datetime_field"]
        )
        saved_model = TestComplexModel.objects.get()

        # Should keep all fields unchanged, since ignore field only one changed
        self.assertEqual(saved_model.integer_field, 1)
        self.assertEqual(saved_model.string_field, "hello")
        self.assertEqual(saved_model.json_field, dict(a="b"))
        self.assertEqual(saved_model.test_foreign_id, foreign1.id)
        self.assertEqual(
            saved_model.datetime_field,
            datetime(1987, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

    def test_update_on_changed_with_changes(self):
        foreign1 = TestForeignKeyModel()
        foreign1.save()
        model1 = TestComplexModel(
            integer_field=1,
            string_field="hello",
            json_field=dict(a="b"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign1,
        )
        model1.save()
        model1.integer_field = 2
        model1.string_field = "world"
        model1.datetime_field = datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        bulk_update_models(
            [model1], model_changed_field_names=["integer_field", "datetime_field"]
        )
        saved_model = TestComplexModel.objects.get()

        # Should change string_field, integer_field and datetime_field since string_field changed
        self.assertEqual(saved_model.string_field, "world")
        self.assertEqual(saved_model.integer_field, 2)
        self.assertEqual(
            saved_model.datetime_field,
            datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(saved_model.json_field, dict(a="b"))
        self.assertEqual(saved_model.test_foreign_id, foreign1.id)

    def test_using_custom_pk_columns(self):
        model1 = TestComplexModel(
            integer_field=2, string_field="hello", json_field=dict(a="b")
        )
        model1.save()
        model2 = TestComplexModel(
            integer_field=2, string_field="world", json_field=dict(c="d")
        )
        model2.save()
        model3 = TestComplexModel(
            integer_field=3, string_field="world", json_field=dict(e="f")
        )
        model3.save()

        # Change 1 field on model2
        model2.json_field = dict(g="h")

        # This should update model instead of model2 because pk is integer_field
        bulk_update_models([model2], pk_field_names=["integer_field", "string_field"])

        # Check model1 not changed
        saved_model1 = TestComplexModel.objects.get(id=model1.id)
        self.assertEqual(saved_model1.integer_field, 2)
        self.assertEqual(saved_model1.string_field, "hello")
        self.assertEqual(saved_model1.json_field, dict(a="b"))

        # Check model2 changed
        saved_model2 = TestComplexModel.objects.get(id=model2.id)
        self.assertEqual(saved_model2.integer_field, 2)
        self.assertEqual(saved_model2.string_field, "world")
        self.assertEqual(saved_model2.json_field, dict(g="h"))

        # Check model3 not changed
        saved_model3 = TestComplexModel.objects.get(id=model3.id)
        self.assertEqual(saved_model3.integer_field, 3)
        self.assertEqual(saved_model3.string_field, "world")
        self.assertEqual(saved_model3.json_field, dict(e="f"))

    def test_multiple_updates(self):
        model1 = TestComplexModel(
            integer_field=1, string_field="hello", json_field=dict(a="b")
        )
        model1.save()
        model1.string_field = "hello_updated"
        model1.json_field = dict(a_changed="b")

        model2 = TestComplexModel(
            integer_field=2, string_field="world", json_field=dict(c="d")
        )
        model2.save()
        model2.string_field = "world_updated"
        model2.json_field = dict(c_changed="d")

        model3 = TestComplexModel(
            integer_field=3, string_field="text", json_field=dict(e="f")
        )
        model3.save()
        model3.string_field = "text_updated"
        model3.json_field = dict(e_changed="f")

        models = bulk_update_models(
            [model1, model2, model3],
            pk_field_names=["integer_field"],
            update_field_names=["string_field", "json_field"],
            return_models=True,
        )

        # We can't guarantee a return order
        saved_model1 = [model for model in models if model.integer_field == 1][0]
        saved_model2 = [model for model in models if model.integer_field == 2][0]
        saved_model3 = [model for model in models if model.integer_field == 3][0]

        # Check models changed
        self.assertEqual(saved_model1.string_field, "hello_updated")
        self.assertEqual(saved_model1.json_field, dict(a_changed="b"))

        self.assertEqual(saved_model2.string_field, "world_updated")
        self.assertEqual(saved_model2.json_field, dict(c_changed="d"))

        self.assertEqual(saved_model3.string_field, "text_updated")
        self.assertEqual(saved_model3.json_field, dict(e_changed="f"))

    def test_simple_update_if_null(self):
        # Should update model1 because it's NULL
        model1 = TestComplexModel(integer_field=None)
        model1.save()
        model1.integer_field = 1

        # Should NOT update model2 because it has a value
        model2 = TestComplexModel(integer_field=2)
        model2.save()
        model2.integer_field = 3

        # Should update model3 because it has a value, but new value is NULL
        model3 = TestComplexModel(integer_field=4)
        model3.save()
        model3.integer_field = None

        bulk_update_models(
            [model1, model2, model3], update_if_null_field_names=["integer_field"]
        )

        self.assertEqual(TestComplexModel.objects.count(), 3)

        # If any of these queries fail, the updates are incorrect above
        TestComplexModel.objects.get(integer_field=1)
        TestComplexModel.objects.get(integer_field=2)
        TestComplexModel.objects.get(integer_field=None)

    def test_complex_update_if_null(self):
        # Should only update integer_field and datetime_field
        model1 = TestComplexModel(
            integer_field=None,
            string_field="a",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model1.save()
        model1.integer_field = 1
        model1.string_field = "b"
        model1.datetime_field = datetime(2020, 1, 5, 3, 4, 5, tzinfo=timezone.utc)

        # Should only update datetime_field
        model2 = TestComplexModel(
            integer_field=2, string_field="c", datetime_field=None
        )
        model2.save()
        model2.integer_field = 3
        model2.string_field = None
        model2.datetime_field = datetime(2020, 11, 1, 22, 20, 15, tzinfo=timezone.utc)

        # Should update string_field and datetime_field
        model3 = TestComplexModel(
            integer_field=4,
            string_field=None,
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model3.save()
        model3.integer_field = None
        model3.datetime_field = None
        model3.string_field = "d"

        bulk_update_models(
            [model1, model2, model3],
            update_if_null_field_names=["integer_field", "string_field"],
        )

        # If any of these queries fail, the updates are incorrect above
        saved_model1 = TestComplexModel.objects.get(integer_field=1)
        saved_model2 = TestComplexModel.objects.get(integer_field=2)
        saved_model3 = TestComplexModel.objects.get(integer_field=None)

        self.assertEqual(saved_model1.string_field, "a")
        self.assertEqual(
            saved_model1.datetime_field,
            datetime(2020, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(
            saved_model2.datetime_field,
            datetime(2020, 11, 1, 22, 20, 15, tzinfo=timezone.utc),
        )
        self.assertIsNone(saved_model2.string_field)
        self.assertEqual(saved_model3.string_field, "d")
        self.assertIsNone(saved_model3.datetime_field)

    def test_custom_where(self):
        # Should only update integer_field and datetime_field
        model1 = TestComplexModel(
            integer_field=1,
            string_field="a",
        )
        model1.save()
        model1.integer_field = 5
        model1.string_field = "b"

        model2 = TestComplexModel(
            integer_field=3, string_field="c"
        )
        model2.save()
        model2.integer_field = 2
        model2.string_field = "c"


        def update_where(fields, source_table_name, destination_table_name):
            """
            Custom where clause where the new value must be greater than the old value
            """

            # This should only update if the new value is greater than previous value
            return generate_greater_than_condition(
                source_table_name=source_table_name,
                destination_table_name=destination_table_name,
                field=TestComplexModel._meta.get_field("integer_field"),
            )

        bulk_update_models(
            [model1, model2],
            update_field_names=["integer_field", "string_field"],
            update_where=update_where
        )

        # First model should be updated because 5 > 1
        saved_model1 = TestComplexModel.objects.get(integer_field=5)
        self.assertEqual(saved_model1.string_field, "b")

        # Second model should not be updated because 2 <  3
        saved_model2 = TestComplexModel.objects.get(integer_field=3)
        self.assertEqual(saved_model2.string_field, "c")

    def test_empty_where_clause_with_model_changed_fields_only(self):
        """
        Test the case where where_clause ends up empty in generate_update_query.
        This happens when all update fields are marked as model_changed_field_names,
        resulting in empty compare_fields and no update_if_null_fields.
        """
        # Create a model with some initial values
        model1 = TestComplexModel(
            integer_field=1,
            string_field="initial_value",
        )
        model1.save()
        
        # Modify the model
        model1.integer_field = 2
        model1.string_field = "updated_value"

        # Update using only model_changed_field_names
        # This will result in empty compare_fields because all update fields
        # are in the ignore_on_compare set, leading to an empty where_clause
        bulk_update_models(
            [model1],
            update_field_names=["integer_field", "string_field"],
            model_changed_field_names=["integer_field", "string_field"],
        )

        # Verify the model was updated (should update all matching records based on PK only)
        saved_model = TestComplexModel.objects.get(pk=model1.pk)
        self.assertEqual(saved_model.integer_field, 2)
        self.assertEqual(saved_model.string_field, "updated_value")

    def test_empty_where_clause_with_update_if_null_fields_only(self):
        """
        Test another case where where_clause ends up empty in generate_update_query.
        This happens when all update fields are marked as update_if_null_field_names,
        and no regular compare_fields exist.
        """
        # Create a model with some initial values
        model1 = TestComplexModel(
            integer_field=1,
            string_field="initial_value",
        )
        model1.save()
        
        # Modify the model
        model1.integer_field = 2
        model1.string_field = "updated_value"

        # Update using only update_if_null_field_names
        # This will result in empty compare_fields because all update fields
        # are in the ignore_on_compare set, leading to an empty where_clause
        bulk_update_models(
            [model1],
            update_field_names=["integer_field", "string_field"],
            update_if_null_field_names=["integer_field", "string_field"],
        )

        # Verify the model was updated appropriately
        # update_if_null_fields only update when source or destination is NULL
        saved_model = TestComplexModel.objects.get(pk=model1.pk)
        # Since neither field was NULL, they shouldn't be updated
        self.assertEqual(saved_model.integer_field, 1)  # Should remain original value
        self.assertEqual(saved_model.string_field, "initial_value")  # Should remain original value

    def test_generate_update_query_empty_where_clause_direct(self):
        """
        Direct unit test of generate_update_query with empty compare_fields to demonstrate the issue.
        This test directly calls generate_update_query with empty compare_fields and no update_if_null_fields.
        """
        model_meta = TestComplexModel._meta
        table_name = model_meta.db_table
        loading_table_name = f"temp_{table_name}"
        
        pk_fields = get_fields_from_names(["id"], model_meta)
        update_fields = get_fields_from_names(["integer_field"], model_meta)
        compare_fields = []  # Empty compare_fields - this is the key to the issue
        
        # This should not raise an exception and should generate valid SQL
        query = generate_update_query(
            table_name=table_name,
            loading_table_name=loading_table_name,
            pk_fields=pk_fields,
            update_fields=update_fields,
            compare_fields=compare_fields,  # Empty!
            update_if_null_fields=None,
        )
        
        # Convert to string to verify it doesn't contain invalid SQL like "WHERE () AND"
        query_str = str(query)
        self.assertNotIn("WHERE () AND", query_str)
        self.assertIn("WHERE", query_str)
        # Should only have the join clause in the WHERE - check for the pattern in the Composed structure
        self.assertIn("temp_test_project_testcomplexmodel", query_str)
        self.assertIn("test_project_testcomplexmodel", query_str)
        # Most importantly, verify no empty parentheses before AND
        self.assertNotIn("() AND", query_str)
