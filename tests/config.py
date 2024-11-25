from src.config import Config


def get_config_for_the_test() -> Config:
    aws_account = "aws_account_1_pro"
    return Config(aws_account)
