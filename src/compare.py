import sys

import pandas as pd
from pandas import DataFrame as Df


FilePathNamesToCompare = tuple[str, str]


def run():
    file_path_names = _get_s3_file_name_paths_from_user_input()
    print(f"Start comparing {' and '.join(file_path_names)}")
    s3_data_df = _get_df_combine_files(file_path_names)
    #_show_summary(s3_data_df, file_path_names)
    s3_analyzed_df = _get_df_analyze_s3_data(s3_data_df, file_path_names)
    print(s3_analyzed_df)


def _get_s3_file_name_paths_from_user_input() -> FilePathNamesToCompare:
    user_input = sys.argv
    try:
        return user_input[1], user_input[2]
    except IndexError:
        raise ValueError(
            "Usage: python compare.py {file_path_name_1} {file_path_name_2}"
            "\nExample: python compare.py exports/file-1.csv exports/file-2.csv"
        )


def _get_df_combine_files(file_path_names: FilePathNamesToCompare) -> Df:
    file_1_df = _get_df_from_file(file_path_names, 0)
    file_2_df = _get_df_from_file(file_path_names, 1)
    result = file_1_df.join(file_2_df, how='outer')
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_multindex(result))
    return result

def _get_column_names_multindex(column_names: list[str]) -> list[tuple[str, str]]:
    return [
        _get_tuple_column_names_multindex(column_name)
        for column_name in column_names
    ]

def _get_tuple_column_names_multindex(column_name: str) -> tuple[str, str]:
    column_name_clean = column_name.replace("_file","")
    indexes = column_name_clean.split("_")
    return indexes[1], indexes[0]


def _get_df_from_file(file_path_names: FilePathNamesToCompare, file_index: int) -> Df:
    return pd.read_csv(
        file_path_names[file_index],
        index_col="name",
        parse_dates=["date"],
    ).add_suffix(f'_file_{file_index}')


def _get_df_analyze_s3_data(df: Df, file_path_names: FilePathNamesToCompare) -> Df:
    condition_exists = (
        df.loc[:, ("0", "size")].notnull()
    ) & (
        df.loc[:, ("1", "size")].notnull()
    )
    # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
    df[[("analysis","exists_file_in_both_paths"),]] = False
    df.loc[condition_exists, [("analysis","exists_file_in_both_paths"),]] = True
    condition = (
        df.loc[:, ("analysis", "exists_file_in_both_paths")].eq(False)
    ) & (
        df.loc[:, ("0", "size")].notnull()
    )
    df.loc[condition, "unique_path_where_the_file_exists"] = file_path_names[0]
    condition = (
        df.loc[:, ("analysis", "exists_file_in_both_paths")].eq(False)
    ) & (
        df.loc[:, ("1", "size")].notnull()
    )
    df.loc[condition, "unique_path_where_the_file_exists"] = file_path_names[1]

    condition = (
        df.loc[:, ("analysis", "exists_file_in_both_paths")].eq(True)
    ) & (
        df.loc[:, ("0", "size")] == df.loc[:, ("0", "size")]
    )
    df.loc[condition, "has_file_same_size_in_both_paths"] = False
    df.loc[condition, "has_file_same_size_in_both_paths"] = True
    return df


def _show_summary(df: pd.DataFrame, file_path_names: FilePathNamesToCompare):
    # TODO work with the result of _get_df_analyze_s3_data
    print()
    print(f"Files in {file_path_names[0]} but not in {file_path_names[1]}")
    print(_get_str_summary_lost_files(_get_lost_files(df, 0)))
    print()
    print(f"Files in {file_path_names[1]} but not in {file_path_names[0]}")
    print(_get_str_summary_lost_files(_get_lost_files(df, 1)))
    print()
    print("Files with different sizes")
    print(_get_str_summary_sizes_files(_get_files_with_different_size(df)))
    print()
    _show_last_file(file_path_names, df, 0)
    print()
    _show_last_file(file_path_names, df, 1)

def _get_str_summary_lost_files(files: list[str]) -> str:
    if len(files) == 0:
        return "- No lost files"
    return _get_str_from_files(files)

def _get_str_from_files(files: list[str]) -> str:
    files_with_prefix = [f"- {file}" for file in files]
    return "\n".join(files_with_prefix)

def _get_lost_files(df: Df, file_index: int) -> list[str]:
    return df.loc[df[f"date_file_{file_index}"].isnull()].index.tolist()

def _get_str_summary_sizes_files(files: list[str]) -> str:
    if len(files) == 0:
        return "- All files have same size"
    return _get_str_from_files(files)

def _get_files_with_different_size(df: Df) -> list[str]:
    condition = (df["size_file_0"].notnull()) & (df["size_file_1"].notnull()) & (df["size_file_0"] != df["size_file_1"])
    return df.loc[condition].index.tolist()


def _show_last_file(file_path_names: FilePathNamesToCompare, df: Df, file_index: int):
    print("Last file in", file_path_names[file_index])
    column_name = f"date_file_{file_index}"
    condition = df[column_name] == df[column_name].max()
    row_file_df = df.loc[condition]
    file_name = row_file_df.index.values[0]
    date = row_file_df[column_name].values[0]
    print(f"{file_name} ({date})")


if __name__ == "__main__":
    run()
