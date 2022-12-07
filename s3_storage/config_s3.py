import boto3
from constants import ACCESS_KEY, SECRET_KEY, S3_URL, S3_BUCKET


RESOURCE = boto3.resource(
    "s3",
    endpoint_url=S3_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

CLIENT = boto3.client(
    "s3",
    endpoint_url=S3_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

BUCKET = RESOURCE.Bucket(S3_BUCKET)
