# TODO move to the config folder
# TODO replace with json file
# TODO refactor create a class to read this config
# TODO testing: not use this file, create one in for the tests
config = {
    "is_file_copied": {
        "origin": "aws_account_1_pro",
        "targets": ["aws_account_2_release", "aws_account_3_dev"],
    },
    "can_file_exist": {
        "origin": "aws_account_1_pro",
        "target": "aws_account_2_release",
    },
}
