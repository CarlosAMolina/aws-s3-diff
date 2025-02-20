# http://docs.getmoto.org/en/latest/docs/server_mode.html

from aws import S3
from aws import set_aws_credentials
from moto.server import ThreadedMotoServer

set_aws_credentials()
server = ThreadedMotoServer()
server.start()
ENDPOINT_URL = "http://localhost:5000"
print("MotoServer:", ENDPOINT_URL)
S3("pro", ENDPOINT_URL).create_objects()
print("You can now run aws commands in another terminal. Example: aws --endpoint-url http://localhost:5000 s3 ls")
input("Press enter to stop the MotoServer")
print("Stopping MotoServer")
server.stop()
