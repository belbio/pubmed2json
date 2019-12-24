import os

from dotenv import load_dotenv

load_dotenv()


def set_bool(val: str, default: bool = False):

    if isinstance(val, bool):
        return val

    if val in ["1", "true", "TRUE", "True"]:
        return True
    elif val in ["0", "false", "FALSE", "False"]:
        return False
    else:
        return default


# Settings

NUMBER_OF_PROCESSORS = os.getenv("NUMBER_OF_PROCESSORS", default=10)

PUBMED_DATA_DIR = os.getenv("PUBMED_DATA_DIR")

ARANGO_URL = os.getenv("ARANGO_URL")
PUBMED_DB_NAME = os.getenv("PUBMED_DB_NAME", default="pubmed")
STORE_XML = set_bool(os.getenv("STORE_XML", default=False))
