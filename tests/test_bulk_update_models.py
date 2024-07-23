from datetime import datetime, timezone

from django.test import TestCase, TransactionTestCase
from django_bulk_load import bulk_update_models, generate_greater_than_condition
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
)
import random
import threading

def do_update():
    objects = [x for x in TestComplexModel.objects.all()]
    for current in objects:
        current.integer_field = random.randint(10,100000)
    bulk_update_models(objects)

class E2ETestBulkUpdateModels(TransactionTestCase):
    def test_integer_field_change(self):
        models = [TestComplexModel(integer_field=1) for x in range(10000)]
        for model in models:
            model.save()
        threads = [
                threading.Thread(target=do_update)
                for x in range(80)
                ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        print("JOINED")

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
