#!/usr/bin/env python
# -*-coding: utf-8 -*-

"""
Usage: $ {1: program}.py
"""

import datetime
import json
import os
import sqlite3

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_S3_USER_ACCESS_KEY = os.getenv("AWS_S3_USER_ACCESS_KEY")
AWS_S3_USER_SECRET_KEY = os.getenv("AWS_S3_USER_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

conn = sqlite3.connect("pubmed.db")
cursor = conn.cursor()

session = boto3.Session(
    aws_access_key_id=AWS_S3_USER_ACCESS_KEY,
    aws_secret_access_key=AWS_S3_USER_SECRET_KEY,
    region_name="us-east-2",
)

s3_client = session.client("s3")  # Low level client to S3

s3 = session.resource("s3")  # S3 object-oriented interface

# s3 = boto3.resource(
#     "s3",
#     aws_access_key_id=AWS_S3_USER_ACCESS_KEY,
#     aws_secret_access_key=AWS_S3_USER_SECRET_KEY,
#     region_name="us-east-2",
# )


def setup_db():

    # Create table
    cursor.execute("""CREATE TABLE pubmed (pmid integer, doc text)""")
    cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS pmid_idx ON pubmed (pmid)""")
    conn.commit()


def add_doc(pmid: int, doc: str):

    try:
        cursor.execute("INSERT INTO pubmed VALUES (?, ?)", (pmid, doc))
    except Exception as e:
        print("Error", str(e))


def get_docs():

    cnt = 0
    start_time = datetime.datetime.now()

    paginator = s3_client.get_paginator("list_objects_v2")
    for result in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix="pubmed"):
        # Download each file individually
        for key in result["Contents"]:
            if key["Key"].endswith(".json"):
                # print("Key", key)
                doc = s3.Object(S3_BUCKET_NAME, key["Key"]).get()["Body"].read().decode("utf-8")
                doc = json.loads(doc)
                # print("DumpVar:\n", json.dumps(doc, indent=4))
                # quit()
                # print("Adding pmid", doc["pmid"])
                add_doc(doc["pmid"], json.dumps(doc))
                cnt += 1
                if cnt % 100 == 0:
                    conn.commit()
                    now = datetime.datetime.now()
                    elapsed = f"{(now - start_time).total_seconds():.1f}"
                    start_time = now
                    print(f"Elapsed: {elapsed} sec  Cnt: {cnt}")


def main():
    # setup_db()

    cursor.execute("DELETE FROM pubmed")
    conn.commit()

    get_docs()


if __name__ == "__main__":
    main()
