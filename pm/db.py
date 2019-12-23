import bz2
import datetime
import glob
import json
import logging
import os
import sqlite3
import xml.etree.cElementTree as ET  # C implementation of ElementTree
from xml.etree.cElementTree import Element

import pm.convert
import pm.settings as settings

log = logging.getLogger()

xml_conn = sqlite3.connect(settings.XML_DB_FN)
xml_cursor = xml_conn.cursor()

json_conn = sqlite3.connect(settings.JSON_DB_FN)
json_cursor = json_conn.cursor()


def setup_databases():

    try:
        setup_xml_db()
    except Exception:
        pass

    try:
        setup_json_db()
    except Exception:
        pass


def setup_xml_db():
    """For XML SQLite database"""

    # Create tables
    xml_cursor.execute(
        """CREATE TABLE IF NOT EXISTS pubmed_xml (pmid INTEGER PRIMARY KEY, doc text)"""
    )
    # xml_cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS pmid_idx ON pubmed_xml (pmid)""")

    xml_cursor.execute(
        """CREATE TABLE IF NOT EXISTS pm_files_processed (filename text PRIMARY KEY)"""
    )
    # xml_cursor.execute(
    #     """CREATE UNIQUE INDEX IF NOT EXISTS pmfiles_idx ON pm_files_processed (filename)"""
    # )
    xml_conn.commit()


def setup_json_db():
    """For JSON SQLite database"""

    # Create tables
    json_cursor.execute(
        """CREATE TABLE IF NOT EXISTS pubmed_json (pmid INTEGER PRIMARY KEY, doc_bz2 blob)"""
    )
    json_cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS pmid_idx ON pubmed_json (pmid)""")
    json_conn.commit()


def reset_processed_files():
    sql = "DELETE FROM pm_files_processed"
    xml_cursor.execute(sql)
    xml_conn.commit()


def reset_databases():
    xml_cursor.execute("DROP TABLE IF EXISTS pubmed_xml ")
    xml_cursor.execute("DROP TABLE IF EXISTS pm_files_processed")
    json_cursor.execute("DROP TABLE IF EXISTS pubmed_json")

    setup_databases()


def get_processed_files():
    sql = "SELECT filename FROM pm_files_processed"
    xml_cursor.execute(sql)
    return xml_cursor.fetchall()


def compress_doc(doc: str) -> bytes:
    try:
        return bz2.compress(doc)
    except Exception as e:
        log.error(f"Cannot compress doc, error: {str(e)}")


def decompress_doc(doc_bz2: bytes) -> str:
    try:
        return bz2.decompress(doc_bz2)
    except Exception as e:
        log.error(f"Cannot decompress doc_bz2, error: {str(e)}")


def xml_tostring(doc: Element) -> str:
    try:
        # return ET.tostring(doc, "utf-8").decode("utf-8")  # convert from bytes to string
        return ET.tostring(doc, "utf-8")

    except Exception as e:
        log.error(f"Cannot convert xml to string, error: {str(e)}")


def xml_fromstring(doc: str) -> Element:
    try:
        return ET.fromstring(doc)
    except Exception as e:
        log.error(f"Cannot convert string to xml, error: {str(e)}")


def add_xml(pmid: int, doc: Element):

    pmid = int(pmid)

    try:
        doc_bz2 = compress_doc(xml_tostring(doc))
        xml_cursor.execute(
            "INSERT INTO pubmed_xml (pmid, doc_bz2) VALUES (:pmid, :doc_bz2)  ON CONFLICT (pmid) DO UPDATE SET doc_bz2=:doc_bz2",
            {"pmid": pmid, "doc_bz2": doc_bz2},
        )
    except Exception as e:
        print("Error", str(e))


def add_xml_fn(fn):

    article_gen = pm.xml.parse_baseline_file_as_generator(fn)
    xml_cursor.executemany(
        "INSERT INTO pubmed_xml (pmid, doc) VALUES (?, ?) ON CONFLICT (pmid) DO NOTHING ",
        article_gen,
    )

    add_xml_fn(fn)
    commit_xml()


# def add_xml_fn(fn: str):

#     try:
#         xml_cursor.execute(
#             "INSERT INTO pm_files_processed (fn) VALUES (:fn)", {"fn": fn},
#         )
#     except Exception as e:
#         pass


def add_json(pmid: int, doc: str):

    pmid = int(pmid)

    try:
        doc_bz2 = compress_doc(doc)
        json_cursor.execute(
            "INSERT INTO pubmed_xml (pmid, doc_bz2) VALUES (:pmid, :doc_bz2)  ON CONFLICT (pmid) DO UPDATE SET doc_bz2=:doc_bz2",
            {"pmid": pmid, "doc_bz2": doc_bz2},
        )
    except Exception as e:
        print("Error", str(e))


def get_xml_doc(pmid: int) -> Element:
    """Get xml doc and decompress it and convert back to XML Element object"""

    pmid = int(pmid)

    doc_bz2 = xml_cursor.execute("select doc_bz2 from pubmed_xml where pmid=?", (pmid,)).fetchone()
    doc_bz2 = doc_bz2[0]

    doc = xml_fromstring(decompress_doc(doc_bz2))

    return doc


def get_json_doc(pmid):
    """Get json doc and decompress it"""

    pmid = int(pmid)

    doc_bz2 = json_cursor.execute(
        "select doc_bz2 from pubmed_json where pmid=?", (pmid,)
    ).fetchone()
    doc_bz2 = doc_bz2[0]

    doc = decompress_doc(doc_bz2)
    return doc


def add_citation(pmid: int, xml_doc: Element):

    pmid = int(pmid)

    json_doc = pm.convert.xml_to_json(xml_doc)

    add_xml(pmid, xml_doc)
    add_json(pmid, json_doc)


def rm_pmid(pmid: int):
    xml_cursor.execute("DELETE FROM pubmed WHERE pmid=?", (pmid,))
    json_cursor.execute("DELETE FROM pubmed WHERE pmid=?", (pmid,))


def commit_xml():
    xml_conn.commit()


def commit_json():
    json_conn.commit()


def commit_both():
    commit_xml()
    commit_json()
