from django.db import models
from uuid import uuid4


class TestForeignKeyModel(models.Model):
    pass


class TestComplexModel(models.Model):
    integer_field = models.IntegerField(null=True, db_index=True)
    string_field = models.TextField(null=True, db_index=True)
    datetime_field = models.DateTimeField(null=True, db_index=True)
    json_field = models.JSONField(null=True)
    test_foreign = models.ForeignKey(
        TestForeignKeyModel, on_delete=models.PROTECT, null=True, db_index=True
    )
    binary_field = models.BinaryField(null=True)
    
    # Additional fields for heavy indexing scenario
    status = models.CharField(max_length=20, default='active', db_index=True)
    priority = models.IntegerField(default=1, db_index=True)
    category = models.CharField(max_length=50, null=True, db_index=True)
    score = models.FloatField(null=True, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    
    class Meta:
        indexes = [
            # Composite indexes for common query patterns
            models.Index(fields=['status', 'priority'], name='status_priority_idx'),
            models.Index(fields=['category', 'score'], name='category_score_idx'),
            models.Index(fields=['integer_field', 'string_field'], name='int_str_composite_idx'),
            models.Index(fields=['datetime_field', 'status'], name='datetime_status_idx'),
            models.Index(fields=['created_at', 'updated_at'], name='created_updated_idx'),
            
            # Partial indexes for filtered queries
            models.Index(fields=['integer_field'], condition=models.Q(is_active=True), name='active_integer_idx'),
            models.Index(fields=['score'], condition=models.Q(score__isnull=False), name='non_null_score_idx'),
            models.Index(fields=['datetime_field'], condition=models.Q(status='active'), name='active_datetime_idx'),
            
            # Covering index for performance
            models.Index(fields=['status', 'priority', 'category', 'score'], name='covering_idx'),
        ]
        
        constraints = [
            # Unique constraints to add more indexing overhead
            models.UniqueConstraint(fields=['integer_field', 'category'], name='unique_int_category'),
            models.UniqueConstraint(fields=['string_field', 'status'], condition=models.Q(is_active=True), name='unique_str_status_active'),
        ]



class TestUUIDModel(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    created_on = models.DateTimeField(auto_now_add=True)
    modified_on = models.DateTimeField(auto_now=True)