from datetime import datetime, timezone

from django.db import transaction, connection, connections
from django.test import TestCase
from django_bulk_load import bulk_select_model_dicts
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
)


class E2ETestBulkSelectModelDicts(TestCase):
    def test_empty_get(self):
        self.assertEqual(
            bulk_select_model_dicts(
                model_class=TestComplexModel,
                filter_field_names=[],
                filter_data=[],
                select_field_names=[],
            ),
            [],
        )

    def test_ignores_duplicates_in_input(self):
        saved_model = TestComplexModel(
            integer_field=123,
            string_field="hello",
        )
        saved_model.save()
        result_dicts = bulk_select_model_dicts(
            model_class=TestComplexModel,
            filter_field_names=["integer_field"],
            filter_data=[(123,), (123,)],
            select_field_names=["string_field", "integer_field"],
        )

        self.assertEqual(len(result_dicts), 1)

    def test_finds_duplicates_when_they_exist(self):
        TestComplexModel(
            integer_field=123,
            string_field="hello",
        ).save()
        secod_saved_model = TestComplexModel(
            integer_field=123,
            string_field="hello",
        ).save()
        result_dicts = bulk_select_model_dicts(
            model_class=TestComplexModel,
            filter_field_names=["integer_field"],
            filter_data=[(123,), (123,)],
            select_field_names=["string_field", "integer_field"],
        )

        self.assertEqual(len(result_dicts), 2)



    def test_single_select(self):
        foreign = TestForeignKeyModel()
        foreign.save()

        saved_model = TestComplexModel(
            integer_field=123,
            string_field="hello",
            json_field=dict(fun="run"),
            datetime_field=datetime(2018, 1, 5, 3, 4, 5, tzinfo=timezone.utc),
            test_foreign=foreign,
        )
        saved_model.save()
        result_dicts = bulk_select_model_dicts(
            model_class=TestComplexModel,
            filter_field_names=["integer_field"],
            filter_data=[(123,)],
            select_field_names=["string_field", "json_field", "test_foreign_id"],
        )

        self.assertEqual(len(result_dicts), 1)

        for attr in ["integer_field", "string_field", "json_field", "test_foreign_id"]:
            self.assertEqual(getattr(saved_model, attr), result_dicts[0][attr])

    def test_multi_select(self):
        saved_model1 = TestComplexModel(
            integer_field=1, json_field=None, string_field="hello1"
        )
        saved_model1.save()

        saved_model2 = TestComplexModel(
            integer_field=2, json_field=None, string_field="hello2"
        )
        saved_model2.save()

        saved_model3 = TestComplexModel(
            integer_field=3, json_field=None, string_field="hello3"
        )
        saved_model3.save()

        result_dicts = bulk_select_model_dicts(
            model_class=TestComplexModel,
            filter_field_names=["id"],
            filter_data=[(saved_model1.id,), (saved_model2.id,), (saved_model3.id,)],
            select_field_names=["integer_field", "string_field", "json_field"],
        )

        # Sort the results, so the order is the same
        result_dicts.sort(key=lambda result_dict: result_dict["id"])

        self.assertEqual(len(result_dicts), 3)
        for attr in ["integer_field", "string_field", "json_field"]:
            self.assertEqual(getattr(saved_model1, attr), result_dicts[0][attr])
            self.assertEqual(getattr(saved_model2, attr), result_dicts[1][attr])
            self.assertEqual(getattr(saved_model3, attr), result_dicts[2][attr])

    def test_multi_model_get_pk_fields(self):
        saved_model1 = TestComplexModel(
            integer_field=1, json_field=None, string_field="hello1"
        )
        saved_model1.save()

        saved_model2 = TestComplexModel(
            integer_field=2, json_field=None, string_field="hello2"
        )
        saved_model2.save()

        saved_model3 = TestComplexModel(
            integer_field=3, json_field=None, string_field="hello3"
        )
        saved_model3.save()

        result_dicts = bulk_select_model_dicts(
            model_class=TestComplexModel,
            filter_field_names=["integer_field", "string_field"],
            filter_data=[
                (saved_model1.integer_field, saved_model1.string_field),
                (saved_model2.integer_field, saved_model2.string_field),
                (saved_model3.integer_field, saved_model3.string_field),
            ],
            select_field_names=["json_field"],
        )

        # Sort the results, so the order is the same
        result_dicts.sort(key=lambda result_dict: result_dict["integer_field"])

        self.assertEqual(len(result_dicts), 3)
        for attr in ["integer_field", "string_field", "json_field"]:
            self.assertEqual(getattr(saved_model1, attr), result_dicts[0][attr])
            self.assertEqual(getattr(saved_model2, attr), result_dicts[1][attr])
            self.assertEqual(getattr(saved_model3, attr), result_dicts[2][attr])

    def test_multi_model_matches_multiple(self):
        saved_model1 = TestComplexModel(
            integer_field=1, json_field=dict(a="b"), string_field="hello1"
        )
        saved_model1.save()

        # Same as model above except different json_field
        saved_model2 = TestComplexModel(
            integer_field=1, json_field=None, string_field="hello1"
        )
        saved_model2.save()

        saved_model3 = TestComplexModel(
            integer_field=2, json_field=None, string_field="hello2"
        )
        saved_model3.save()

        result_dicts = bulk_select_model_dicts(
            model_class=TestComplexModel,
            filter_field_names=["integer_field", "string_field"],
            select_field_names=["json_field", "id"],
            filter_data=[(saved_model1.integer_field, saved_model1.string_field)],
        )

        # Sort the results, so the order is the same
        result_dicts.sort(key=lambda result_dict: result_dict["id"])

        self.assertEqual(len(result_dicts), 2)

        for attr in ["integer_field", "string_field", "json_field"]:
            self.assertEqual(getattr(saved_model1, attr), result_dicts[0][attr])
            self.assertEqual(getattr(saved_model2, attr), result_dicts[1][attr])

