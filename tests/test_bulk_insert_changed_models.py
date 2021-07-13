from datetime import datetime, timezone
from django.test import TestCase

from django_bulk_load import bulk_insert_changed_models
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
)


class E2ETestBulkInsertChangedModels(TestCase):
    def test_empty_insert(self):
        self.assertIsNone(
            bulk_insert_changed_models(
                [],
                pk_field_names=["integer_field"],
                compare_field_names=["string_field"],
            )
        )

    def test_single_new_model(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model = TestComplexModel(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        bulk_insert_changed_models(
            [unsaved_model],
            pk_field_names=["integer_field", "string_field"],
            compare_field_names=["json_field", "datetime_field", "test_foreign"],
        )

        saved_model = TestComplexModel.objects.get()
        self.assertIsNotNone(saved_model.id)
        for attr in ["integer_field", "string_field", "json_field", "test_foreign_id"]:
            self.assertEqual(getattr(saved_model, attr), getattr(unsaved_model, attr))

    def test_null_field_does_not_cause_update(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        saved_model1 = TestComplexModel(
            integer_field=1, json_field=None, string_field="hello"
        )
        saved_model1.save()

        # This should not trigger an update since it's in model_changed_field_names
        saved_model1.string_field = "world"
        bulk_insert_changed_models(
            [saved_model1],
            pk_field_names=["integer_field"],
            compare_field_names=["json_field"],
        )

        db_saved_model1 = TestComplexModel.objects.get()
        for attr in ["integer_field", "json_field", "test_foreign_id", "id"]:
            self.assertEqual(
                getattr(db_saved_model1, attr), getattr(saved_model1, attr)
            )

        # Make sure the string field was not updated
        self.assertEqual(db_saved_model1.string_field, "hello")

    def test_integer_field_change_not_pk_changed(self):
        model1 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        model1.save()
        model1.integer_field = 2
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["string_field"],
            compare_field_names=["integer_field"],
        )

        # Should insert a new row in DB because model changed
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # Get the original record. Make sure it's not changed
        saved_model1 = TestComplexModel.objects.order_by("id").first()
        self.assertEqual(saved_model1.integer_field, 123)
        self.assertEqual(saved_model1.string_field, "hello")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

        # Get the newest record inserted
        saved_model1 = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(saved_model1.integer_field, 2)
        self.assertEqual(saved_model1.string_field, "hello")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

    def test_integer_field_change_not_pk_unchanged(self):
        model1 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        model1.save()

        # Same model just not saved
        model2 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        bulk_insert_changed_models(
            [model2],
            pk_field_names=["string_field"],
            compare_field_names=["integer_field"],
        )

        # Should only have 1 record because nothing changed
        saved_model1 = TestComplexModel.objects.get()
        self.assertEqual(saved_model1.integer_field, 123)
        self.assertEqual(saved_model1.string_field, "hello")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

    def test_integer_field_change_is_pk(self):
        model1 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        model1.save()
        model1.integer_field = 2
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["string_field", "integer_field"],
            compare_field_names=["json_field"],
        )

        # Should insert a new row in DB because it no longer matches PK
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # Get the newest record inserted
        saved_model1 = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(saved_model1.integer_field, 2)
        self.assertEqual(saved_model1.string_field, "hello")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

    def test_string_field_change_not_pk_changed(self):
        model1 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        model1.save()
        model1.string_field = "world"
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["integer_field"],
            compare_field_names=["string_field"],
        )

        # Should insert a new row because string value changed
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # Get the newest record inserted
        saved_model1 = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(saved_model1.integer_field, 123)
        self.assertEqual(saved_model1.string_field, "world")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

    def test_string_field_change_not_pk_unchanged(self):
        model1 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        model1.save()

        # Same model just not saved
        model2 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        bulk_insert_changed_models(
            [model2],
            pk_field_names=["integer_field"],
            compare_field_names=["string_field"],
        )

        # Should only have 1 record because nothing changed
        saved_model1 = TestComplexModel.objects.get()
        self.assertEqual(saved_model1.integer_field, 123)
        self.assertEqual(saved_model1.string_field, "hello")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

    def test_string_field_change_is_pk(self):
        model1 = TestComplexModel(
            integer_field=123, string_field="hello", json_field=dict(fun="run")
        )
        model1.save()
        model1.string_field = "world"
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["integer_field", "string_field"],
            compare_field_names=["json_field"],
        )

        # Should insert a new row in DB because it no longer matches PK
        self.assertEqual(TestComplexModel.objects.count(), 2)

        # Get the newest record inserted
        saved_model1 = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(saved_model1.integer_field, 123)
        self.assertEqual(saved_model1.string_field, "world")
        self.assertEqual(saved_model1.json_field, dict(fun="run"))

    def test_datetime_field_not_pk_changed(self):
        model1 = TestComplexModel(
            integer_field=123,
            string_field="hello",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model1.save()

        # Changed the datetime
        model2 = TestComplexModel(
            integer_field=123,
            string_field="hello",
            datetime_field=datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc),
        )

        bulk_insert_changed_models(
            [model2],
            pk_field_names=["integer_field", "string_field"],
            compare_field_names=["datetime_field"],
        )

        # Should insert a new row because datetime value changed
        self.assertEqual(TestComplexModel.objects.count(), 2)

        saved_model = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(
            saved_model.datetime_field,
            datetime(2012, 12, 10, 22, 8, 9, tzinfo=timezone.utc),
        )

    def test_datetime_field_not_pk_unchanged(self):
        model1 = TestComplexModel(
            integer_field=123,
            string_field="hello",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model1.save()

        # Same model unsaved
        model2 = TestComplexModel(
            integer_field=123,
            string_field="hello",
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

        bulk_insert_changed_models(
            [model2],
            pk_field_names=["integer_field", "string_field"],
            compare_field_names=["datetime_field"],
        )

        # Should only have a single model since nothing changed
        saved_model = TestComplexModel.objects.get()
        self.assertEqual(
            saved_model.datetime_field,
            datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

    def test_json_field_change(self):
        model1 = TestComplexModel(integer_field=1, json_field=dict(a="b"))
        model1.save()
        model1.json_field = dict(c="d")
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["integer_field"],
            compare_field_names=["json_field"],
        )

        # Should insert a new row because json value changed
        self.assertEqual(TestComplexModel.objects.count(), 2)

        saved_model = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(saved_model.json_field, dict(c="d"))

    def test_multiple_new_models(self):
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
        bulk_insert_changed_models(
            unsaved_models,
            pk_field_names=["integer_field"],
            compare_field_names=[
                "string_field",
                "json_field",
                "datetime_field",
                "test_foreign",
            ],
        )

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

    def test_multiple_saved_model_changes(self):
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
        model1.string_field = "fun"
        model1.json_field = dict(test="val")
        model1.datetime_field = datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        model1.test_foreign = foreign2

        # 1 item should be saved now
        self.assertEqual(TestComplexModel.objects.count(), 1)

        bulk_insert_changed_models(
            [model1],
            pk_field_names=["integer_field"],
            compare_field_names=[
                "string_field",
                "json_field",
                "datetime_field",
                "test_foreign",
            ],
        )

        # Should have inserted a new record because values changed
        self.assertEqual(TestComplexModel.objects.count(), 2)

        old_model = TestComplexModel.objects.order_by("id").first()
        self.assertEqual(old_model.id, model1.id)
        self.assertEqual(old_model.integer_field, 123)
        self.assertEqual(old_model.string_field, "hello")
        self.assertEqual(old_model.json_field, dict(fun="run"))
        self.assertEqual(
            old_model.datetime_field,
            datetime(2020, 12, 1, 12, 2, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(old_model.test_foreign.id, foreign.id)

        new_model = TestComplexModel.objects.order_by("id").last()
        self.assertEqual(new_model.integer_field, 123)
        self.assertEqual(new_model.string_field, "fun")
        self.assertEqual(new_model.json_field, dict(test="val"))
        self.assertEqual(
            new_model.datetime_field, datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        )
        self.assertEqual(new_model.test_foreign.id, foreign2.id)

    def test_return_models(self):
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
        post_models = bulk_insert_changed_models(
            pre_models,
            pk_field_names=["string_field"],
            compare_field_names=["integer_field", "json_field", "test_foreign"],
            return_models=True,
        )

        # There were 2 already saved. 3 new models saved
        self.assertEqual(TestComplexModel.objects.count(), 5)

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

    def test_insert_field_not_in_compare_list(self):
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
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["string_field"],
            compare_field_names=["json_field", "test_foreign"],
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

    def test_insert_field_not_in_compare_list_with_changes(self):
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
        model1.json_field = dict(c="d")
        model1.datetime_field = datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc)
        bulk_insert_changed_models(
            [model1],
            pk_field_names=["string_field"],
            compare_field_names=["json_field", "test_foreign"],
        )

        self.assertEqual(TestComplexModel.objects.count(), 2)
        saved_model = TestComplexModel.objects.order_by("id").last()

        # Should change string_field, integer_field and datetime_field since string_field changed
        self.assertEqual(saved_model.string_field, "hello")
        self.assertEqual(saved_model.integer_field, 2)
        self.assertEqual(
            saved_model.datetime_field,
            datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(saved_model.json_field, dict(c="d"))
        self.assertEqual(saved_model.test_foreign_id, foreign1.id)

    def test_order_field_no_change(self):
        model1 = TestComplexModel(
            integer_field=3,
            string_field="hello",
            datetime_field=datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model1.save()
        model2 = TestComplexModel(
            integer_field=2,
            string_field="hello",
            datetime_field=datetime(2001, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model2.save()
        model3 = TestComplexModel(
            integer_field=1,
            string_field="hello",
            datetime_field=datetime(2002, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model3.save()

        # This should not create a new model, since it matches integer_field=3
        new_model = TestComplexModel(
            integer_field=3,
            string_field="hello",
            datetime_field=datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

        models = bulk_insert_changed_models(
            [new_model],
            pk_field_names=["string_field"],
            compare_field_names=["datetime_field"],
            order_field_name="integer_field",
            return_models=True,
        )

        self.assertEqual(len(models), 1)
        self.assertEqual(TestComplexModel.objects.count(), 3)

        # Check that we got back the row that matches
        self.assertEqual(models[0].id, model1.id)

    def test_order_field_with_change(self):
        model1 = TestComplexModel(
            integer_field=3,
            string_field="hello",
            datetime_field=datetime(1999, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model1.save()
        model2 = TestComplexModel(
            integer_field=2,
            string_field="hello",
            datetime_field=datetime(2001, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model2.save()
        model3 = TestComplexModel(
            integer_field=1,
            string_field="hello",
            datetime_field=datetime(2002, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        model3.save()

        # This should create a new model, since it matches string=hello and has
        # different datetime_field
        new_model = TestComplexModel(
            integer_field=4,
            string_field="hello",
            datetime_field=datetime(2020, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

        models = bulk_insert_changed_models(
            [new_model],
            pk_field_names=["string_field"],
            compare_field_names=["datetime_field"],
            order_field_name="integer_field",
            return_models=True,
        )

        self.assertEqual(len(models), 1)
        self.assertEqual(TestComplexModel.objects.count(), 4)

        # Make sure the model doesn't have the same ID
        saved_new_model = models[0]
        self.assertNotEqual(saved_new_model.id, model1.id)

        # Check all the fields are saved correctly
        self.assertEqual(
            saved_new_model.datetime_field,
            datetime(2020, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(saved_new_model.integer_field, 4)
        self.assertEqual(saved_new_model.string_field, "hello")
