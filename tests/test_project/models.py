from django.contrib.postgres.fields import JSONField
from django.db import models


class TestForeignKeyModel(models.Model):
    pass


class TestComplexModel(models.Model):
    integer_field = models.IntegerField(null=True)
    string_field = models.TextField(null=True)
    datetime_field = models.DateTimeField(null=True)
    json_field = JSONField(null=True)
    test_foreign = models.ForeignKey(
        TestForeignKeyModel, on_delete=models.PROTECT, null=True
    )
