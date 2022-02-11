from django.db import models
from uuid import uuid4


class TestForeignKeyModel(models.Model):
    pass


class TestComplexModel(models.Model):
    integer_field = models.IntegerField(null=True)
    string_field = models.TextField(null=True)
    datetime_field = models.DateTimeField(null=True)
    json_field = models.JSONField(null=True)
    test_foreign = models.ForeignKey(
        TestForeignKeyModel, on_delete=models.PROTECT, null=True
    )



class TestUUIDModel(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    created_on = models.DateTimeField(auto_now_add=True)
    modified_on = models.DateTimeField(auto_now=True)