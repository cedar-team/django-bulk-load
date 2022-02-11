from datetime import datetime, timezone

from django.test import TestCase
from django_bulk_load import bulk_upsert_models
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
    TestUUIDModel
)


class E2ETestBulkUpsertModels(TestCase):
    def test_empty_upsert(self):
        self.assertEqual(bulk_upsert_models([]), None)

    def test_single_upsert_new(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model = TestComplexModel(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        bulk_upsert_models([unsaved_model])

        saved_model = TestComplexModel.objects.get()
        self.assertIsNotNone(saved_model.id)
        for attr in ["integer_field", "string_field", "json_field", "test_foreign_id"]:
            self.assertEqual(getattr(saved_model, attr), getattr(unsaved_model, attr))

    def test_integer_field_change(self):
        model1 = TestComplexModel(integer_field=1)
        model1.save()
        model1.integer_field = 2
        bulk_upsert_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.integer_field, 2)

    def test_string_field_change(self):
        model1 = TestComplexModel(string_field="hello")
        model1.save()
        model1.string_field = "world"
        bulk_upsert_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.string_field, "world")

    def test_datetime_field_change(self):
        model1 = TestComplexModel(
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        )
        model1.save()
        model1.datetime_field = datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc)
        bulk_upsert_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(
            saved_model.datetime_field,
            datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc),
        )

    def test_json_field_change(self):
        model1 = TestComplexModel(json_field=dict(a="b"))
        model1.save()
        model1.json_field = dict(c="d")
        bulk_upsert_models([model1])
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.json_field, dict(c="d"))

    def test_multiple_upsert_new(self):
        foreign_1 = TestForeignKeyModel()
        foreign_1.save()

        foreign_2 = TestForeignKeyModel()
        foreign_2.save()

        foreign_3 = TestForeignKeyModel()
        foreign_3.save()

        unsaved_model_1 = TestComplexModel(
            integer_field=1,
            string_field="a",
            json_field=dict(z="y"),
            datetime_field=datetime(2020, 10, 1, 21, 2, 3, tzinfo=timezone.utc),
            test_foreign=foreign_1,
        )

        unsaved_model_2 = TestComplexModel(
            integer_field=2,
            string_field="b",
            json_field=dict(z="y"),
            datetime_field=datetime(2020, 11, 1, 0, 2, 3, tzinfo=timezone.utc),
            test_foreign=foreign_2,
        )

        unsaved_model_3 = TestComplexModel(
            integer_field=3,
            string_field="c",
            json_field=dict(z="y"),
            datetime_field=datetime(2020, 12, 1, 12, 2, 3, tzinfo=timezone.utc),
            test_foreign=foreign_3,
        )
        unsaved_models = [unsaved_model_1, unsaved_model_2, unsaved_model_3]
        bulk_upsert_models(unsaved_models)

        # Get the models by value. They may not have been inserted in order
        saved_model_1 = TestComplexModel.objects.get(integer_field=1)
        saved_model_2 = TestComplexModel.objects.get(integer_field=2)
        saved_model_3 = TestComplexModel.objects.get(integer_field=3)
        saved_models = [saved_model_1, saved_model_2, saved_model_3]

        for i in range(3):
            self.assertIsNotNone(saved_models[i].id)
            for attr in [
                "integer_field",
                "string_field",
                "json_field",
                "datetime_field",
                "test_foreign_id",
            ]:
                self.assertEqual(
                    getattr(saved_models[i], attr), getattr(unsaved_models[i], attr)
                )

    def test_upsert_saved_model_changes(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        foreign2 = TestForeignKeyModel()
        foreign2.save()

        # No items should be in the DB
        self.assertEqual(TestComplexModel.objects.count(), 0)
        model1 = TestComplexModel(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2020, 12, 1, 12, 2, 3, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        model1.save()
        model1.integer_field = 456
        model1.string_field = "fun"
        model1.json_field = dict(test="val")
        model1.datetime_field = datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        model1.test_foreign = foreign2

        # 1 item should be saved now
        self.assertEqual(TestComplexModel.objects.count(), 1)

        bulk_upsert_models([model1])

        db_model = TestComplexModel.objects.get()

        # There should still only be 1 item
        self.assertEqual(TestComplexModel.objects.count(), 1)
        self.assertEqual(db_model.id, model1.id)
        self.assertEqual(db_model.integer_field, 456)
        self.assertEqual(db_model.string_field, "fun")
        self.assertEqual(db_model.json_field, dict(test="val"))
        self.assertEqual(
            db_model.datetime_field, datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        )
        self.assertEqual(db_model.test_foreign.id, foreign2.id)

    def test_upsert_return_models(self):
        foreign_1 = TestForeignKeyModel()
        foreign_1.save()

        foreign_2 = TestForeignKeyModel()
        foreign_2.save()

        foreign_3 = TestForeignKeyModel()
        foreign_3.save()

        saved_model_1 = TestComplexModel(
            integer_field=1,
            string_field="a",
            json_field=dict(z="y"),
            test_foreign=foreign_1,
        )
        saved_model_1.save()
        saved_model_1.string_field = "b"

        unsaved_model_2 = TestComplexModel(
            integer_field=2,
            string_field="c",
            json_field=dict(z="y"),
            test_foreign=foreign_2,
        )

        saved_model_3 = TestComplexModel(
            integer_field=3,
            string_field="e",
            json_field=dict(z="y"),
            test_foreign=foreign_3,
        )
        saved_model_3.save()
        saved_model_3.string_field = "f"

        pre_models = [saved_model_1, unsaved_model_2, saved_model_3]
        post_models = bulk_upsert_models(pre_models, return_models=True)

        # Get the models by value. They may not have been inserted in order. This also checks the
        # saved values
        post_model_1 = [model for model in post_models if model.string_field == "b"][0]
        post_model_2 = [model for model in post_models if model.string_field == "c"][0]
        post_model_3 = [model for model in post_models if model.string_field == "f"][0]

        # These fields should be the same, since they were not changed
        self.assertEqual(saved_model_1.integer_field, post_model_1.integer_field)
        self.assertEqual(saved_model_1.json_field, post_model_1.json_field)
        self.assertEqual(saved_model_1.test_foreign_id, post_model_1.test_foreign_id)

        # These fields should saved
        self.assertEqual(post_model_2.integer_field, unsaved_model_2.integer_field)
        self.assertEqual(post_model_2.json_field, unsaved_model_2.json_field)
        self.assertEqual(post_model_2.test_foreign_id, unsaved_model_2.test_foreign_id)

        # These fields should be the same, since they were not changed
        self.assertEqual(saved_model_3.integer_field, post_model_3.integer_field)
        self.assertEqual(saved_model_3.json_field, post_model_3.json_field)
        self.assertEqual(saved_model_3.test_foreign_id, post_model_3.test_foreign_id)

        # Make sure we are getting back the same models
        self.assertEqual(TestComplexModel.objects.count(), 3)
        self.assertIsNotNone(post_model_2.id)
        self.assertEqual(saved_model_1.id, post_model_1.id)
        self.assertEqual(saved_model_3.id, post_model_3.id)

    def test_upsert_and_return_models_uuid(self):
        unsaved_model = TestUUIDModel()
        saved_model = TestUUIDModel()
        saved_model.save()
        old_modified_on = saved_model.modified_on

        pre_models = [unsaved_model, saved_model]
        post_models = bulk_upsert_models(pre_models, return_models=True)

        post_unsaved_model = [model for model in post_models if model.id == unsaved_model.id][0]
        post_saved_model = [model for model in post_models if model.id == saved_model.id][0]

        self.assertIsNotNone(post_unsaved_model.modified_on)
        self.assertIsNotNone(post_unsaved_model.created_on)
        self.assertIsNotNone(post_saved_model.created_on)
        self.assertLess(old_modified_on, post_saved_model.modified_on)



    def test_upsert_insert_only_fields(self):
        saved_model_1 = TestComplexModel(integer_field=1, string_field="a")
        saved_model_1.save()
        saved_model_1.string_field = "b"

        unsaved_model_2 = TestComplexModel(integer_field=2, string_field="c")

        post_models = bulk_upsert_models(
            [saved_model_1, unsaved_model_2],
            insert_only_field_names=["string_field"],
            return_models=True,
        )
        post_model_1 = [model for model in post_models if model.string_field == "a"][0]
        post_model_2 = [model for model in post_models if model.string_field == "c"][0]

        # string_field should not be "b" because field is insert only
        self.assertEqual(post_model_1.string_field, "a")
        self.assertEqual(post_model_2.string_field, "c")

    def test_upsert_pk_names(self):
        saved_model_1 = TestComplexModel(integer_field=1, string_field="a")
        saved_model_1.save()
        # Changing this field should trigger a new insert (since this is the primary key on insert)
        saved_model_1.string_field = "c"

        unsaved_model_2 = TestComplexModel(integer_field=2, string_field="b")

        post_models = bulk_upsert_models(
            [saved_model_1, unsaved_model_2],
            pk_field_names=["string_field"],
            return_models=True,
        )

        post_model_1 = [model for model in post_models if model.string_field == "c"][0]
        post_model_2 = [model for model in post_models if model.string_field == "b"][0]

        self.assertEqual(post_model_1.string_field, "c")
        self.assertEqual(post_model_2.string_field, "b")

        # There should be 3 since a new record would be inserted after pk changed
        self.assertEqual(TestComplexModel.objects.count(), 3)

    def test_upsert_no_updates_only_insert(self):
        """
        Calling bulk_upsert_changed_models where all columns are the primary key should
        insert exactly one record

        A scenario like this can happen when you have a table that has a unique
        constraint on all of its columns and you only want to insert (not update)
        a single record with a given set of values
        """
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model_1 = TestComplexModel(
            integer_field=1,
            string_field="a",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )
        unsaved_model_2 = TestComplexModel(
            integer_field=2,
            string_field="b",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )

        self.assertEqual(TestComplexModel.objects.count(), 0)

        # First time we should insert
        bulk_upsert_models(
            [unsaved_model_1, unsaved_model_2],
            pk_field_names=[
                "integer_field",
                "string_field",
                "datetime_field",
                "json_field",
                "test_foreign",
            ],
        )

        # Two records inserted
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # Try to insert again to make sure no duplicates
        bulk_upsert_models(
            [unsaved_model_1, unsaved_model_2],
            pk_field_names=[
                "integer_field",
                "string_field",
                "datetime_field",
                "json_field",
                "test_foreign",
            ],
        )

        # Still two objects
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # All values are as we expect - no exceptions here
        TestComplexModel.objects.get(
            integer_field=1,
            string_field="a",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )

        TestComplexModel.objects.get(
            integer_field=2,
            string_field="b",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )

    def test_upsert_no_updates_only_insert_return_models(self):
        """
        The insert-only bulk_upsert_models scenario with return_models set to True
        returns the correct models
        """
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model_1 = TestComplexModel(
            integer_field=1,
            string_field="a",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )
        unsaved_model_2 = TestComplexModel(
            integer_field=2,
            string_field="b",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )

        self.assertEqual(TestComplexModel.objects.count(), 0)

        # First time we should insert
        returned_models = bulk_upsert_models(
            [unsaved_model_1, unsaved_model_2],
            pk_field_names=[
                "integer_field",
                "string_field",
                "datetime_field",
                "json_field",
                "test_foreign",
            ],
            return_models=True,
        )

        # Two records inserted
        self.assertEqual(TestComplexModel.objects.count(), 2)

        saved_model_1 = TestComplexModel.objects.get(integer_field=1)
        saved_model_2 = TestComplexModel.objects.get(integer_field=2)

        # Returned models match
        self.assertCountEqual([saved_model_1, saved_model_2], returned_models)

        # Try to insert again to make sure no duplicates
        returned_models = bulk_upsert_models(
            [unsaved_model_1, unsaved_model_2],
            pk_field_names=[
                "integer_field",
                "string_field",
                "datetime_field",
                "json_field",
                "test_foreign",
            ],
            return_models=True,
        )

        # Still two objects
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # All values are as we expect - no exceptions here
        saved_model_1 = TestComplexModel.objects.get(
            integer_field=1,
            string_field="a",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )

        saved_model_2 = TestComplexModel.objects.get(
            integer_field=2,
            string_field="b",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            json_field={"a": "dict"},
            test_foreign=foreign,
        )

        # Returned models match
        self.assertCountEqual([saved_model_1, saved_model_2], returned_models)

    def test_simple_update_if_null(self):
        # Should update model1 because it's NULL
        model1 = TestComplexModel(integer_field=None)

        # Should NOT update model1 because it has a value
        model2 = TestComplexModel(integer_field=2)

        # Should NOT update model1 because it has a value
        model3 = TestComplexModel(integer_field=4)

        bulk_upsert_models(
            [model1, model2, model3], update_if_null_field_names=["integer_field"]
        )

        # Make sure all models were inserted correctly
        self.assertEqual(TestComplexModel.objects.count(), 3)

        # If any of these queries fail, the updates are incorrect above
        saved_model1 = TestComplexModel.objects.get(integer_field=None)
        saved_model2 = TestComplexModel.objects.get(integer_field=2)
        saved_model3 = TestComplexModel.objects.get(integer_field=4)

        saved_model1.integer_field = 1
        saved_model2.integer_field = 3
        saved_model3.integer_field = None

        # Update the models after the change
        bulk_upsert_models(
            [saved_model1, saved_model2, saved_model3],
            update_if_null_field_names=["integer_field"],
        )

        # Make sure all models were update in place
        self.assertEqual(TestComplexModel.objects.count(), 3)

        # If any of these queries fail, the updates are incorrect above
        TestComplexModel.objects.get(integer_field=1)

        # Should not update this row, since it has a value
        TestComplexModel.objects.get(integer_field=2)

        # Should update this row, since it's new value is NULL
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

        # Should update string_field and datetime_field
        model2 = TestComplexModel(
            integer_field=2, string_field="c", datetime_field=None
        )
        model2.save()
        model2.integer_field = 3
        model2.string_field = None
        model2.datetime_field = datetime(2020, 11, 1, 22, 20, 15, tzinfo=timezone.utc)

        # Should update all fields
        model3 = TestComplexModel(
            integer_field=4,
            string_field=None,
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model3.save()
        model3.integer_field = None
        model3.datetime_field = None
        model3.string_field = "d"

        bulk_upsert_models(
            [model1, model2, model3],
            update_if_null_field_names=["integer_field", "string_field"],
        )

        self.assertEqual(TestComplexModel.objects.count(), 3)

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
