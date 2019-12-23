#!/usr/bin/env python
# -*-coding: utf-8 -*-

"""
Usage: $ {1: program}.py
"""

import logging

import pm.processing
import pm.settings as settings
import pm.xml

logging.basicConfig(filename="pubmed.log", level=logging.INFO)
log = logging.getLogger("pubmed")


# db.py should be finished
# TODO xml.py and convert.py (should be merged to xml.py)
# TODO processing.py (multi-processing code and queues)


def main():

    # pm.db.reset_databases()
    # # pm.db.setup_databases()

    # pm.xml.parse_pubmed_file("baseline/pubmed19n0971.xml.gz")
    # quit()

    # pm.xml.parse_baseline_file("baseline/pubmed19n0957.xml.gz")
    # pm.xml.parse_baseline_file("baseline/pubmed19n0941.xml.gz")

    # doc = pm.db.get_xml_doc("30525334")
    # print(pm.db.xml_tostring(doc))

    # quit()

    # pm.xml.parse_xml(f"{settings.PUBMED_DATA_DIR}/baseline/pubmed19n0972.xml.gz")
    # pm.db.xml_conn.commit()

    # pm.processing.load_baseline()
    pm.processing.load_updatefiles()


if __name__ == "__main__":
    main()
