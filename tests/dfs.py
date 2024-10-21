from pandas import DataFrame as Df

# TODO convert epoch to datetime
expected_result_compare_df = Df(
    {
        ("aws_account_1_pro", "date"): {
            ("cars", "europe-spain", "cars_20241014.csv"): 1728895741000,
            ("pets", "dogs-big_size", "dogs-20240914.xlsx"): 1726344693000,
            ("pets", "dogs-big_size", "dogs-20241015.xlsx"): 1728987798000,
            ("pets", "dogs-big_size", "dogs-20241019.xlsx"): 1729320608000,
            ("pets", "dogs-big_size", "dogs-20241020-test.xlsx"): None,
            ("pets", "dogs-big_size", "dogs-20241021.xlsx"): None,
            ("pets", "horses-europe", "horses-20210219.xlsx"): None,
        },
        ("aws_account_1_pro", "size"): {
            ("cars", "europe-spain", "cars_20241014.csv"): 33201,
            ("pets", "dogs-big_size", "dogs-20240914.xlsx"): 23543,
            ("pets", "dogs-big_size", "dogs-20241015.xlsx"): 123433,
            ("pets", "dogs-big_size", "dogs-20241019.xlsx"): 859636,
            ("pets", "dogs-big_size", "dogs-20241020-test.xlsx"): None,
            ("pets", "dogs-big_size", "dogs-20241021.xlsx"): None,
            ("pets", "horses-europe", "horses-20210219.xlsx"): None,
        },
        ("aws_account_2_release", "date"): {
            ("cars", "europe-spain", "cars_20241014.csv"): 1729377181000,
            ("pets", "dogs-big_size", "dogs-20240914.xlsx"): 1726351293000,
            ("pets", "dogs-big_size", "dogs-20241015.xlsx"): 1728991218000,
            ("pets", "dogs-big_size", "dogs-20241019.xlsx"): 1729324328000,
            ("pets", "dogs-big_size", "dogs-20241020-test.xlsx"): 1729452661000,
            ("pets", "dogs-big_size", "dogs-20241021.xlsx"): None,
            ("pets", "horses-europe", "horses-20210219.xlsx"): None,
        },
        ("aws_account_2_release", "size"): {
            ("cars", "europe-spain", "cars_20241014.csv"): 33201,
            ("pets", "dogs-big_size", "dogs-20240914.xlsx"): 23543,
            ("pets", "dogs-big_size", "dogs-20241015.xlsx"): 123433,
            ("pets", "dogs-big_size", "dogs-20241019.xlsx"): 33,
            ("pets", "dogs-big_size", "dogs-20241020-test.xlsx"): 332,
            ("pets", "dogs-big_size", "dogs-20241021.xlsx"): None,
            ("pets", "horses-europe", "horses-20210219.xlsx"): None,
        },
        ("aws_account_3_dev", "date"): {
            ("cars", "europe-spain", "cars_20241014.csv"): 1728895741000,
            ("pets", "dogs-big_size", "dogs-20240914.xlsx"): 1726351893000,
            ("pets", "dogs-big_size", "dogs-20241015.xlsx"): 1728995358000,
            ("pets", "dogs-big_size", "dogs-20241019.xlsx"): 1729331708000,
            ("pets", "dogs-big_size", "dogs-20241020-test.xlsx"): None,
            ("pets", "dogs-big_size", "dogs-20241021.xlsx"): 1729507808000,
            ("pets", "horses-europe", "horses-20210219.xlsx"): 1613776508000,
        },
        ("aws_account_3_dev", "size"): {
            ("cars", "europe-spain", "cars_20241014.csv"): 33201,
            ("pets", "dogs-big_size", "dogs-20240914.xlsx"): 23543,
            ("pets", "dogs-big_size", "dogs-20241015.xlsx"): 553433,
            ("pets", "dogs-big_size", "dogs-20241019.xlsx"): 859636,
            ("pets", "dogs-big_size", "dogs-20241020-test.xlsx"): None,
            ("pets", "dogs-big_size", "dogs-20241021.xlsx"): 196,
            ("pets", "horses-europe", "horses-20210219.xlsx"): 3963,
        },
    }
)
