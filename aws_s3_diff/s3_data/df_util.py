def get_column_name_from_column_multi_index(column_multi_index: tuple[str, str]):
    if column_multi_index[1] in ("date", "hash", "size"):
        return f"{column_multi_index[1]}_in_{column_multi_index[0]}"
    return "_".join(column_multi_index)
