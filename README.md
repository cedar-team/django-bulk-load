# Django Bulk Load
Load large sets of Django models into the DB using Postgres COPY command. This is a more performant alternative
to [bulk_create](https://docs.djangoproject.com/en/3.2/ref/models/querysets/#bulk-create) and 
[bulk_update](https://docs.djangoproject.com/en/3.2/ref/models/querysets/#bulk-update) in Django.

Note: Currently, this library only supports Postgres. Other databases may be added in the future

## Install
```shell
pip install django-bulk-load
```

## API
Just import and use the functions below. No need to change settings.py

### bulk_insert_models()
INSERT a batch of models. It makes use of Postgres COPY command to improve speed. If a row already exist, the entire
insert will fail.

```python
from django_bulk_load import bulk_insert_models

bulk_insert_models(
    models: Sequence[Model],
    ignore_conflicts: bool = False,
    return_models: bool = False,
)
```

### bulk_upsert_models()
UPSERT a batch of models. Replicates [UPSERTing](https://wiki.postgresql.org/wiki/UPSERT) for a large set of models. 
By default, it matches existing models using the model `pk`, but you can specify matching on other fields with
`pk_field_names`. See bulk_load.py for descriptions of other parameters.

```python
from django_bulk_load import bulk_upsert_models

bulk_upsert_models(
    models: Sequence[Model],
    pk_field_names: Sequence[str] = None,
    insert_only_field_names: Sequence[str] = None,
    model_changed_field_names: Sequence[str] = None,
    update_if_null_field_names: Sequence[str] = None,
    return_models: bool = False,
)
```

### bulk_update_models()
UPDATE a batch of models. If the model is not found in the database, it is ignored. See bulk_load.py for descriptions of other parameters.

```python
from django_bulk_load import bulk_update_models

bulk_update_models(
    models: Sequence[Model],
    update_field_names: Sequence[str] = None,
    pk_field_names: Sequence[str] = None,
    model_changed_field_names: Sequence[str] = None,
    update_if_null_field_names: Sequence[str] = None,
    return_models: bool = False,
)
```

### bulk_insert_changed_models()
INSERTs a new record in the database when a model field has changed in any of `compare_field_names`,
with respect to its latest state, where "latest" is defined by ordering the records
for a given primary key by sorting in descending order on the column passed in
`order_field_name`. Does not INSERT a new record if the latest record has not changed.

```python
from django_bulk_load import bulk_update_models
bulk_insert_changed_models(
    models: Sequence[Model],
    pk_field_names: Sequence[str],
    compare_field_names: Sequence[str],
    order_field_name=None,
    return_models=None,
)
```

### bulk_select_model_dicts()
Select/Get model dictionaries by filter_field_names. It returns dictionaries, not Django
models for performance reasons. This is useful when querying a very large set of models or multiple field IN clauses.
 
```python
bulk_select_model_dicts(
    model_class: Type[Model],
    filter_field_names: Iterable[str],
    select_field_names: Iterable[str],
    filter_data: Iterable[Sequence],
    skip_filter_transform=False,
)
```