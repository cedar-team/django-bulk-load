from uuid import uuid1

POSTGRES_MAX_TABLE_NAME_LEN_CHARS = 63
NULL_CHARACTER = "\\N"


def generate_table_name(source_table_name: str) -> str:
    table_name_template = "loading_{source_table_name}_" + uuid1().hex
    # postgres has a max table name length of 63 characters, so it's possible
    # the staging table name could exceed the max table length. when this happens,
    # use only the uuid portion of the staging table name to ensure that the
    # table name is unique.
    max_source_table_name_length = POSTGRES_MAX_TABLE_NAME_LEN_CHARS - len(
        table_name_template.replace("{source_table_name}", "")
    )
    truncated_source_table_name = source_table_name[: max_source_table_name_length - 1]
    return table_name_template.format(source_table_name=truncated_source_table_name)
