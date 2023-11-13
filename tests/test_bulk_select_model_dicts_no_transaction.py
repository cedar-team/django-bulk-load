from datetime import datetime, timezone

from django.db import transaction, connection, connections
from django.test import TransactionTestCase
from django_bulk_load import bulk_select_model_dicts
from .test_project.models import (
    TestComplexModel,
    TestForeignKeyModel,
)


class E2ETestBulkSelectModelDictsNoTransaction(TransactionTestCase):

    def test_select_for_update(self):
        """
        This checks the select for update locks the correct rows
        :return:
        """
        saved_model1 = TestComplexModel(
            integer_field=1, json_field=dict(a="b"), string_field="hello1"
        )
        saved_model1.save()

        saved_model2 = TestComplexModel(
            integer_field=2, json_field=None, string_field="hello1"
        )
        saved_model2.save()

        # This model is not part of the query and shouldn't be locked
        saved_model3 = TestComplexModel(
            integer_field=3, json_field=None, string_field="hello1"
        )
        saved_model3.save()

        def get_unlocked_rows():
            # Create a separate connection to check for unlocked rows
            try:
                new_connection = connections.create_connection('default')
                # Should return 0 because both are locked
                with new_connection.cursor() as cursor:
                    # Should return 2 because nothing is locked yet
                    cursor.execute(f"select * from {TestComplexModel._meta.db_table} for update skip locked")
                    rows = cursor.fetchall()
                    return len(rows)
            finally:
                if new_connection:
                    new_connection.close()


        # Should be 3 unlocked rows
        self.assertEqual(get_unlocked_rows(), 3)

        with transaction.atomic():
            result_dicts = bulk_select_model_dicts(
                model_class=TestComplexModel,
                filter_field_names=["integer_field"],
                select_field_names=["json_field", "id"],
                filter_data=[(saved_model1.integer_field,), (saved_model2.integer_field,)],
                select_for_update=True
            )
            self.assertEqual(len(result_dicts), 2)

            # Should be 1 because the 2 rows are locked above
            self.assertEqual(get_unlocked_rows(), 1)

        # Should be 3 when transaction completes
        self.assertEqual(get_unlocked_rows(), 3)


