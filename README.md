# Django Bulk Load
Load large batches of Django models into the DB using the Postgres COPY command. This library is a more performant 
alternative to [bulk_create](https://docs.djangoproject.com/en/3.2/ref/models/querysets/#bulk-create) and 
[bulk_update](https://docs.djangoproject.com/en/3.2/ref/models/querysets/#bulk-update) in Django.

Note: Currently, this library only supports Postgres. Other databases may be added in the future.

## Install
```shell
pip install django-bulk-load
```

## Benchmarks
### bulk_update_models vs [Django's bulk_update](https://docs.djangoproject.com/en/dev/ref/models/querysets/#bulk-update) vs [django-bulk-update](https://github.com/aykut/django-bulk-update)

#### Results
```shell
count: 1,000
bulk_update (Django):             0.45329761505126953
bulk_update (django-bulk-update): 0.1036691665649414
bulk_update_models:               0.04524850845336914

count: 10,000
bulk_update (Django):             6.0840747356414795
bulk_update (django-bulk-update): 2.433042049407959
bulk_update_models:               0.10899758338928223

count: 100,000
bulk_update (Django):             647.6648473739624
bulk_update (django-bulk-update): 619.0643970966339
bulk_update_modelsL               0.9625072479248047

count: 1,000,000
bulk_update (Django):             Does not complete
bulk_update (django-bulk-update): Does not complete
bulk_update_models:               14.923949003219604
```
See this thread for information on Django performance issues.
https://groups.google.com/g/django-updates/c/kAn992Fkk24

#### Code
```shell
models = [TestComplexModel(id=i, integer_field=i, string_field=str(i)) for i in range(count)]

def run_bulk_update_django():
  start = time()
  TestComplexModel.objects.bulk_update(models, fields=["integer_field", "string_field"])
  print(time() - start)
  
def run_bulk_update_library():
  start = time()
  TestComplexModel.objects.bulk_update(models, update_fields=["integer_field", "string_field"])
  print(time() - start)
  
def run_bulk_update_models():
  start = time()
  bulk_update_models(models)
  print(time() - start)
```


### bulk_insert_models vs [Django's bulk_create](https://docs.djangoproject.com/en/dev/ref/models/querysets/#bulk-create)
#### Results
```
count: 1,000
bulk_create:        0.048630714416503906
bulk_insert_models: 0.03132152557373047

count: 10,000
bulk_create:        0.45952868461608887
bulk_insert_models: 0.1908433437347412

count: 100,000
bulk_create:        4.875206708908081
bulk_insert_models: 1.764514684677124

count: 1,000,000
bulk_create:        59.16990399360657
bulk_insert_models: 18.651455640792847
```
#### Code
```shell
models = [TestComplexModel(integer_field=i, string_field=str(i)) for i in range(count)]

def run_bulk_create():
  start = time()
  TestComplexModel.objects.bulk_create(models)
  print(time() - start)
  
def run_bulk_insert_models():
  start = time()
  bulk_insert_models(models)
  print(time() - start)
```

## API
Just import and use the functions below. No need to change settings.py

### bulk_insert_models()
INSERT a batch of models. It makes use of the Postgres COPY command to improve speed. If a row already exist, the entire
insert will fail. See bulk_load.py for descriptions of all parameters.

```python
from django_bulk_load import bulk_insert_models

bulk_insert_models(
    models: Sequence[Model],
    ignore_conflicts: bool = False,
    return_models: bool = False,
)
```

### bulk_upsert_models()
UPSERT a batch of models. It replicates [UPSERTing](https://wiki.postgresql.org/wiki/UPSERT). 
By default, it matches existing models using the model `pk`, but you can specify matching on other fields with
`pk_field_names`. See bulk_load.py for descriptions of all parameters.

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
UPDATE a batch of models. By default, it matches existing models using the model `pk`, but you can specify matching on other fields with
`pk_field_names`. If the model is not found in the database, it is ignored. See bulk_load.py for descriptions of all parameters.

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
`order_field_name`. Does not INSERT a new record if the latest record has not changed. See bulk_load.py for descriptions of all parameters.

```python
from django_bulk_load import bulk_insert_changed_models

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
from django_bulk_load import bulk_select_model_dicts

bulk_select_model_dicts(
    model_class: Type[Model],
    filter_field_names: Iterable[str],
    select_field_names: Iterable[str],
    filter_data: Iterable[Sequence],
    skip_filter_transform=False,
)
```

## Contributing
We are not accepting pull requests from anyone outside Cedar employees at this time. 
All pull requests will be closed.

### Commit Syntax
All PRs must be a single commit and follow the following syntax 
https://github.com/angular/angular/blob/master/CONTRIBUTING.md#-commit-message-format

### Testing
You will need Docker installed and run the following command
```
./test.sh
```
