import logging

import arango
import pm.settings as settings
import xxhash

log = logging.getLogger()

username = "root"
password = ""

client = arango.ArangoClient(hosts=settings.ARANGO_URL)

sys_db = client.db("_system", username=username, password=password)

# Create a new database
if not sys_db.has_database(settings.PUBMED_DB_NAME):
    sys_db.create_database(
        name=settings.PUBMED_DB_NAME,
        users=[{"username": username, "password": password, "active": True}],
    )

pubmed_db = client.db(settings.PUBMED_DB_NAME, username=username, password=password)


# xml collection
if not pubmed_db.has_collection("xml"):
    pubmed_db.create_collection("xml", index_bucket_count=64)

xml_coll = pubmed_db.collection("xml")

# json collection
if not pubmed_db.has_collection("json"):
    pubmed_db.create_collection("json", index_bucket_count=64)

json_coll = pubmed_db.collection("json")

# processed_files collection
if not pubmed_db.has_collection("processed_files"):
    pubmed_db.create_collection("processed_files", index_bucket_count=64)

files_coll = pubmed_db.collection("processed_files")


def add_xml(pmid: str, filename: str, xml_article_str: str):

    doc = {"_key": pmid, "filename": filename, "article": xml_article_str}
    try:
        xml_coll.insert(doc, overwrite=True, silent=True, return_old=False)
    except arango.DocumentInsertError as e:
        log.exception(f"Problem inserting Pubmed XML {pmid}  FN: {filename}")


def add_json(pmid: str, article: dict):

    pmid = pmid
    doc = {"_key": pmid, "article": article}
    try:
        json_coll.insert(doc, overwrite=True)
    except arango.DocumentInsertError as e:
        log.exception(f"Problem inserting Pubmed JSON {pmid}  FN: {article['pubmed_xml_fn']}")


def add_processed_filename(fn: str, article_cnt: int, duration: float):

    _key = xxhash.xxh64(fn).hexdigest()

    doc = {"_key": _key, "fn": fn, "article_cnt": article_cnt, "duration": duration}
    files_coll.insert(doc, overwrite=True)


def add_stats():
    pass


def get_processed_files():

    processed_files = []
    for doc in files_coll:
        processed_files.append(doc["fn"])

    return processed_files
