# CHANGELOG



## v1.4.3 (2023-11-17)

### Fix

* fix(release): fix version variables ([`148bb07`](https://github.com/cedar-team/django-bulk-load/commit/148bb07fc1d4865ffd5aa0ee3e3a33ee97146305))


## v1.4.2 (2023-11-17)

### Fix

* fix(release): update workflow.yml permissions ([`f1172c8`](https://github.com/cedar-team/django-bulk-load/commit/f1172c8f6fcb860d07e6921c488052bba1e15b04))


## v1.4.1 (2023-11-17)

### Fix

* fix(build): update release.yml ([`37be1f3`](https://github.com/cedar-team/django-bulk-load/commit/37be1f3e54a4d72d3cafbe31a2248b17ef1559bf))


## v1.4.0 (2023-11-15)

### Feature

* feat: add ability to specify custom where clause in bulk_update_models and bulk_upsert_models ([`0dc7a65`](https://github.com/cedar-team/django-bulk-load/commit/0dc7a65348f750792cf58da1bc996b569d45d650))


## v1.3.0 (2023-11-13)

### Feature

* feat: select_for_update in bulk_select_model_dicts

Remove extra space ([`d1c6caa`](https://github.com/cedar-team/django-bulk-load/commit/d1c6caa7a0a0714ce327caf075c31061af9b446b))

### Fix

* fix(django): add error when trying to insert binary data ([`9bd7b6d`](https://github.com/cedar-team/django-bulk-load/commit/9bd7b6d70240bc1e876026b6e1e91350593b5ece))


## v1.2.2 (2022-02-11)

### Fix

* fix(django): fix issue with uuid pk and auto_now_add ([`777c787`](https://github.com/cedar-team/django-bulk-load/commit/777c787e501ff6528b921c3f97bc9b1fb990f35e))


## v1.2.1 (2022-02-10)

### Fix

* fix(setup.py): update repo url ([`5fd77ed`](https://github.com/cedar-team/django-bulk-load/commit/5fd77ed5ed0f01b615d1a4ae0a8ebc965c3530fb))


## v1.2.0 (2022-02-10)

### Build

* build: update branch name for release ([`5921d3e`](https://github.com/cedar-team/django-bulk-load/commit/5921d3e64cc34106e186720f6bbd903ead71e1a7))

* build: update release workflow branch ([`a8baa2a`](https://github.com/cedar-team/django-bulk-load/commit/a8baa2a021a57ec194690b4cb6cb3aad4a519105))

### Documentation

* docs(PR): Add PR template ([`412830d`](https://github.com/cedar-team/django-bulk-load/commit/412830d3eb1842a13abde92645b0b81901050932))

* docs(README): Add commit syntax and testing ([`3085774`](https://github.com/cedar-team/django-bulk-load/commit/30857741475423bc664db740f789bb2ebfb47083))

### Feature

* feat: update dependency from psycopg2-binary to psycopg ([`9adfb78`](https://github.com/cedar-team/django-bulk-load/commit/9adfb788028c1cab1530ec2f412258f90e11c712))


## v1.1.0 (2021-07-15)

### Build

* build(release): add token to checkout step

add build token to checkout step to allow push permissions on github ([`fd76b0c`](https://github.com/cedar-team/django-bulk-load/commit/fd76b0c7947aae932c5462e3f79f3afb91a043af))

* build(release): change release to use custom token

add a new secret to the repo with admin priviledge, so it can bypass the status checks ([`b634960`](https://github.com/cedar-team/django-bulk-load/commit/b6349602824b04d06e0f69647e64732df277abe2))

* build(release): update release branch

Update the release branch name to main from master ([`4e2f2bc`](https://github.com/cedar-team/django-bulk-load/commit/4e2f2bc0569068106af678b03e0871c33b061631))

* build(release): Fix missing tool.semaantic_release

Fix issue with missing prefix in pyproject.toml ([`852222d`](https://github.com/cedar-team/django-bulk-load/commit/852222d7cd22b9c44ec8739f0a97343165397653))

* build(release): change code branch for more debugging

update the semantic release branch to show version ([`3cd8a92`](https://github.com/cedar-team/django-bulk-load/commit/3cd8a9261b3abcacfb51f6d4943e425084c57c90))

* build(release): fix invalid syntax to pyproject.toml

Remove change to follow the docs ([`25d884f`](https://github.com/cedar-team/django-bulk-load/commit/25d884f14b316482cfa8ef24ac36496ea068f44d))

* build(release): remove quotes from version variable

Update the version variable to remove quotes ([`36678cd`](https://github.com/cedar-team/django-bulk-load/commit/36678cd6f5f1c1fdefa1be1cf52c1798a4979dd6))

* build(release): change branch for release

Change build for release from master to main ([`d296a68`](https://github.com/cedar-team/django-bulk-load/commit/d296a6819c2f27f496b40cdd51d3f710ac30bbc1))

* build(github): add semantic release

Add Github action that automatically releases to pypi and github based on commit messages ([`e20a616`](https://github.com/cedar-team/django-bulk-load/commit/e20a6166d1b7023f61ad152d7157e7b4ad1fa028))

* build: remove tests from package

remove the tests folder from the pypi package ([`3c6df55`](https://github.com/cedar-team/django-bulk-load/commit/3c6df55d429dc760518fd4ffdd43d9c828183e9c))

* build: fix invalid github action syntax

Updates the Github action syntax to be valid ([`ae78829`](https://github.com/cedar-team/django-bulk-load/commit/ae788294f208e8bf4b9a61c37e93c7abbde64c9f))

* build: update github workflow steps

updates Github action workflow to run in a single job instead of 2 ([`9939af0`](https://github.com/cedar-team/django-bulk-load/commit/9939af059cfac050d07542d35709b35f9ba33837))

* build(github): Add commit syntax check

Adds commit syntax check for PRs ([`fe7176b`](https://github.com/cedar-team/django-bulk-load/commit/fe7176b7b6ab9e9e00e77eb7289431b1865d47ee))

* build: Add test github action

Add Github action to run the unit tests ([`0e731ce`](https://github.com/cedar-team/django-bulk-load/commit/0e731ce434bc1ed3095d55619da9487355e07f96))

### Documentation

* docs(readme): update readme grammar

update the readme grammar and add correct types in insert query ([`facf45c`](https://github.com/cedar-team/django-bulk-load/commit/facf45c0d89405020709d14196300dcab17422b6))

* docs: Fix wording

Fix wording in the readme ([`81ee1b7`](https://github.com/cedar-team/django-bulk-load/commit/81ee1b716569e62d7b58e099b8628f98a0d38444))

* docs: Fix benchmark format

Update the readme to make the benchmarks more readable ([`c9ca2a1`](https://github.com/cedar-team/django-bulk-load/commit/c9ca2a1e23e31832e5fd7e7f02bf23a294edaa3a))

* docs: update readme

Added the word count to benchmarks ([`a8313c6`](https://github.com/cedar-team/django-bulk-load/commit/a8313c6d0cea2fabe4b80039957d3f5d04ad8d74))

* docs: add benchmarks to readme

Added benchmark to the Readme to compare the library against Django and django-bulk-update ([`153c479`](https://github.com/cedar-team/django-bulk-load/commit/153c47908e245878c8995ce31641e2898eddda62))

* docs: update readme

Added better imports in the examples and changed sentences around bulk_load.py ([`ee6641e`](https://github.com/cedar-team/django-bulk-load/commit/ee6641e3f3fc44d7e9c6cce58136b7be557992c9))

### Feature

* feat: Initial commit of repo ([`aa97f9d`](https://github.com/cedar-team/django-bulk-load/commit/aa97f9dd42e169538c41b599efd4056f14ef8a43))

### Refactor

* refactor: Update docs and add tests

This renames the tests to be picked up by test.sh. It also adds a test for bulk_insert_models ([`3ef7cff`](https://github.com/cedar-team/django-bulk-load/commit/3ef7cffff3a04a0fd52419d8c26a77b8ca8df369))

* refactor: update license, pyproject.toml ([`5575da9`](https://github.com/cedar-team/django-bulk-load/commit/5575da97fd1441424dd8d976efb1ab44951de05e))
