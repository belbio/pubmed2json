import os

from dotenv import load_dotenv

load_dotenv()


NUMBER_OF_PROCESSORS = os.getenv("NUMBER_OF_PROCESSORS", default=10)

PUBMED_DATA_DIR = os.getenv("PUBMED_DATA_DIR")

ARANGO_URL = os.getenv("ARANGO_URL")

# Sqlite option
# XML_DB_FN = os.getenv("XML_DB_FN")
# JSON_DB_FN = os.getenv("JSON_DB_FN")

# S3 storage option
# AWS_S3_USER_ACCESS_KEY = os.getenv("AWS_S3_USER_ACCESS_KEY")
# AWS_S3_USER_SECRET_KEY = os.getenv("AWS_S3_USER_SECRET_KEY")
# S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
