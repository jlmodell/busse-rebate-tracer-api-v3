import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), ".env"))

assert os.environ["ATLAS_MONGODB_URI"], "ATLAS_MONGODB_URI not set"
assert os.environ["S3_ACCESS_KEY"], "S3_ACCESS_KEY not set"
assert os.environ["S3_SECRET_KEY"], "S3_SECRET_KEY not set"
assert os.environ["S3_URL"], "S3_URL not set"
assert os.environ["S3_BUCKET"], "S3_BUCKET not set"


MONGODB_URI = os.environ["ATLAS_MONGODB_URI"]
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_URL = os.environ["S3_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]

assert os.environ["DISCORD_URL"], "DISCORD_URL not set"
DISCORD_URL = os.environ["DISCORD_URL"]

assert os.environ["REDIS_URL"], "REDIS_URL not set"
assert os.environ["REDIS_PASS"], "REDIS_PASS not set"
REDIS_URL, REDIS_PORT = os.environ["REDIS_URL"].split(":")
REDIS_PASS = os.environ["REDIS_PASS"]
