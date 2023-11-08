from datetime import datetime, timezone

from django.test import TestCase
from django_bulk_load import bulk_insert_models
from django.db import IntegrityError
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
)


class E2ETestBulkInsertModelsTest(TestCase):
    def test_empty_upsert(self):
        self.assertEqual(bulk_insert_models([]), None)

    def test_single_insert_new(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model = TestComplexModel(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        bulk_insert_models([unsaved_model])

        saved_model = TestComplexModel.objects.get()
        self.assertIsNotNone(saved_model.id)
        for attr in ["integer_field", "string_field", "json_field", "test_foreign_id"]:
            self.assertEqual(getattr(saved_model, attr), getattr(unsaved_model, attr))

    def test_single_insert_new_with_pk(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model = TestComplexModel(
            id=10,
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        bulk_insert_models([unsaved_model])

        saved_model = TestComplexModel.objects.get()
        self.assertEqual(saved_model.id, 10)
        for attr in ["id", "integer_field", "string_field", "json_field", "test_foreign_id"]:
            self.assertEqual(getattr(saved_model, attr), getattr(unsaved_model, attr))

    def test_duplicate_insert_fails(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        saved_model = TestComplexModel.objects.create(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        with self.assertRaises(IntegrityError):
            bulk_insert_models([saved_model])

    def test_duplicate_insert_ignore_conflicts_success(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        saved_model = TestComplexModel.objects.create(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        self.assertEqual(TestComplexModel.objects.count(), 1)

        # This will throw an error without ignore_conflicts=True
        bulk_insert_models([saved_model], ignore_conflicts=True)

        # Check we still only have 1 record
        self.assertEqual(TestComplexModel.objects.count(), 1)

    def test_multiple_inserts(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model1 = TestComplexModel(
            integer_field=1,
            string_field="hello1",
            json_field=dict(fun="run1"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        unsaved_model2 = TestComplexModel(
            integer_field=2,
            string_field="hello2",
            json_field=dict(fun="run2"),
            datetime_field=datetime(2018, 2, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        unsaved_model3 = TestComplexModel(
            integer_field=3,
            string_field="hello3",
            json_field=dict(fun="run3"),
            datetime_field=datetime(2018, 3, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        unsaved_models = [unsaved_model1, unsaved_model2, unsaved_model3]
        bulk_insert_models(unsaved_models)

        unsaved_by_integer_field = {model.integer_field: model for model in unsaved_models}
        for saved_model in TestComplexModel.objects.all():
            self.assertIsNotNone(saved_model.id)
            for attr in ["integer_field", "string_field", "json_field", "test_foreign_id"]:
                self.assertEqual(getattr(saved_model, attr), getattr(unsaved_by_integer_field[saved_model.integer_field], attr))


    def test_return_models(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        unsaved_model1 = TestComplexModel(
            integer_field=1,
            string_field="hello1",
            json_field=dict(fun="run1"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        unsaved_model2 = TestComplexModel(
            integer_field=2,
            string_field="hello2",
            json_field=dict(fun="run2"),
            datetime_field=datetime(2018, 2, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )

        unsaved_models = [unsaved_model1, unsaved_model2]

        # This may return models in any order
        saved_models = bulk_insert_models(unsaved_models, return_models=True)
        unsaved_by_integer_field = {model.integer_field: model for model in unsaved_models}
        for saved_model in saved_models:
            self.assertIsNotNone(saved_model.id)
            for attr in ["integer_field", "string_field", "json_field", "test_foreign_id"]:
                self.assertEqual(getattr(saved_model, attr),
                                 getattr(unsaved_by_integer_field[saved_model.integer_field], attr))

    def test_errors_when_mix_of_pk_and_not(self):
        unsaved_model_with_pk = TestComplexModel(
            id=1,
            integer_field=1,
            string_field="hello1",
            json_field=dict(fun="run1"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
        )
        unsaved_model_without_pk = TestComplexModel(
            integer_field=2,
            string_field="hello2",
            json_field=dict(fun="run2"),
            datetime_field=datetime(2018, 2, 5, 3, 4, 5, tzinfo=timezone.utc),
        )

        with self.assertRaises(ValueError):
            bulk_insert_models([
                unsaved_model_with_pk,
                unsaved_model_without_pk
            ])

    def test_errors_when_uploading_binary(self):
        unsaved_model1 = TestComplexModel(
            binary_field=b"hello2",
        )
        unsaved_model2 = TestComplexModel(
            binary_field=b"hello2",
        )

        with self.assertRaises(ValueError):
            bulk_insert_models([
                unsaved_model1,
                unsaved_model2
            ])
