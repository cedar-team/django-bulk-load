# Generated manually for performance tests

from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='TestForeignKeyModel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='TestComplexModel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('integer_field', models.IntegerField(blank=True, db_index=True, null=True)),
                ('string_field', models.TextField(blank=True, db_index=True, null=True)),
                ('datetime_field', models.DateTimeField(blank=True, db_index=True, null=True)),
                ('json_field', models.JSONField(blank=True, null=True)),
                ('binary_field', models.BinaryField(blank=True, null=True)),
                ('status', models.CharField(db_index=True, default='active', max_length=20)),
                ('priority', models.IntegerField(db_index=True, default=1)),
                ('category', models.CharField(blank=True, db_index=True, max_length=50, null=True)),
                ('score', models.FloatField(blank=True, db_index=True, null=True)),
                ('is_active', models.BooleanField(db_index=True, default=True)),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('test_foreign', models.ForeignKey(blank=True, db_index=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='test_project.testforeignkeymodel')),
            ],
        ),
        migrations.CreateModel(
            name='TestUUIDModel',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True)),
                ('created_on', models.DateTimeField(auto_now_add=True)),
                ('modified_on', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(fields=['status', 'priority'], name='status_priority_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(fields=['category', 'score'], name='category_score_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(fields=['integer_field', 'string_field'], name='int_str_composite_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(fields=['datetime_field', 'status'], name='datetime_status_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(fields=['created_at', 'updated_at'], name='created_updated_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(condition=models.Q(is_active=True), fields=['integer_field'], name='active_integer_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(condition=models.Q(score__isnull=False), fields=['score'], name='non_null_score_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(condition=models.Q(status='active'), fields=['datetime_field'], name='active_datetime_idx'),
        ),
        migrations.AddIndex(
            model_name='testcomplexmodel',
            index=models.Index(fields=['status', 'priority', 'category', 'score'], name='covering_idx'),
        ),
        migrations.AddConstraint(
            model_name='testcomplexmodel',
            constraint=models.UniqueConstraint(fields=['integer_field', 'category'], name='unique_int_category'),
        ),
        migrations.AddConstraint(
            model_name='testcomplexmodel',
            constraint=models.UniqueConstraint(condition=models.Q(is_active=True), fields=['string_field', 'status'], name='unique_str_status_active'),
        ),
    ] 