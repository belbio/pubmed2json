#!/usr/bin/env python3.7
# -*-coding: utf-8 -*-

"""
Usage: $ {1: program}.py
"""

import datetime
import glob
import json
import os
import sqlite3

conn = sqlite3.connect("pubmed.db")
cursor = conn.cursor()

set_size = 1000


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

    for fn in glob.iglob("files/*.json"):
        with open(fn, "r") as f:
            doc = json.load(f)
            add_doc(doc["pmid"], json.dumps(doc))
            cnt += 1
            if cnt % set_size == 0:
                conn.commit()
                now = datetime.datetime.now()
                elapsed = f"{(now - start_time).total_seconds():.1f}"
                start_time = now
                print(
                    f"Elapsed: {elapsed} sec  Rate: {set_size/float(elapsed)} docs/sec  Cnt: {cnt}"
                )


def main():
    setup_db()

    cursor.execute("DELETE FROM pubmed")
    conn.commit()

    get_docs()


if __name__ == "__main__":
    main()
