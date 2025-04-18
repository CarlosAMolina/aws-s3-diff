import pathlib
import sys

current_path = pathlib.Path(__file__).parent.absolute()
src_path = current_path.parent.joinpath("aws_s3_diff")
sys.path.append(str(src_path))
