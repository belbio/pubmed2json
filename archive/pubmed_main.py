#!/usr/bin/env python
# -*-coding: utf-8 -*-

"""
Usage: $ {1: program}.py

This program can be restarted and will take up where it left off. You can start
from scratch by deleting the file: processed_files.txt

This can also be used to update the medline - everytime you run it - it will check to see
if there are any new updatefiles and load those (or delete pubmed records listed as deleted)
"""

import datetime
import gzip
import io
import json
import logging
import multiprocessing
import os
import time
import urllib
import urllib.request
from multiprocessing import Process, Queue

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

import ftputil
import pubmed_convert as convert
from lxml import etree

logging.basicConfig(filename="pubmed2s3.log", level=logging.INFO)
log = logging.getLogger("pubmed2s3")

load_dotenv()

AWS_S3_USER_ACCESS_KEY = os.getenv("AWS_S3_USER_ACCESS_KEY")
AWS_S3_USER_SECRET_KEY = os.getenv("AWS_S3_USER_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
NUMBER_OF_PROCESSES = os.getenv("NUMBER_OF_PROCESSES", default=2)

FTP_HOST = "ftp.ncbi.nlm.nih.gov"

s3 = boto3.resource(
    "s3",
    aws_access_key_id=AWS_S3_USER_ACCESS_KEY,
    aws_secret_access_key=AWS_S3_USER_SECRET_KEY,
    region_name="us-east-2",
)


def load_task_queues(baseline_task_queue: Queue, update_task_queue: Queue):
    """Collect pubmed file names - skipping files that have already been processed

    file_type can be either: baseline or updatefiles
    """

    processed_files = {}
    try:
        with open("processed_files.txt", "r") as f:
            for fn in f:
                fn = fn.strip()
                processed_files[fn] = 1
    except Exception:
        pass

    file_list = []
    with ftputil.FTPHost(FTP_HOST, "anonymous", "") as ftp_host:
        # get list of files in repository
        ftp_host.use_list_a_option = False

        # Get baseline
        ftp_host.chdir(f"/pubmed/baseline")
        baseline_file_list = [
            f"/pubmed/baseline/{fn}"
            for fn in ftp_host.listdir(ftp_host.curdir)
            if fn.endswith("xml.gz")
        ]
        # Get updatefiles
        ftp_host.chdir(f"/pubmed/updatefiles")
        update_file_list = [
            f"/pubmed/updatefiles/{fn}"
            for fn in ftp_host.listdir(ftp_host.curdir)
            if fn.endswith("xml.gz")
        ]

    cnt = 0
    for fn in baseline_file_list:
        if fn not in processed_files:
            cnt += 1
            baseline_task_queue.put(fn)
    log.info(f"Queued {cnt} baseline files out of {len(baseline_file_list)}")

    cnt = 0
    for fn in update_file_list:
        if fn not in processed_files:
            cnt += 1
            update_task_queue.put(fn)
    log.info(f"Queued {cnt} updatefiles out of {len(update_file_list)}")


def finished_tasks(done_queue: Queue):
    """Write finished tasks to file"""

    checkpoint_seconds = 3600
    baseline_cnt, updatefile_cnt, article_cnt = 0, 0, 0
    last_article_cnt, last_updatefile_cnt = 0, 0
    start_time = datetime.datetime.now()
    last_time = datetime.datetime.now()
    with open("processed_files.txt", "a") as f:

        while True:
            time.sleep(5)
            while not done_queue.empty():
                d = done_queue.get()
                article_cnt = d["article_cnt"]
                fn = d["fn"]
                duration = d["duration_sec"]
                if "baseline" in fn:
                    file_type = "baseline"
                else:
                    file_type = "updatefile"

                f.write(f"{fn}\n")
                f.flush()
                # Log throughput
                articles_sec = article_cnt / duration
                if file_type == "baseline":
                    msg = f"Articles: {article_cnt} Articles/sec: {articles_sec}  Duration(sec): {duration} Estimated Total Articles/Sec: {articles_sec * int(NUMBER_OF_PROCESSES)} FN: {fn}"
                else:
                    msg = f"Articles: {article_cnt} Articles/sec: {articles_sec}  Duration(sec): {duration} FN: {fn}"

                log.info(msg)


def get_pubmed_file(fn):

    url = f"ftp://ftp.ncbi.nlm.nih.gov/{fn}"
    mysock = urllib.request.urlopen(url)
    memfile = io.BytesIO(mysock.read())
    return gzip.GzipFile(fileobj=memfile).read()


def save_to_s3(doc, fn):

    try:
        object = s3.Object(S3_BUCKET_NAME, f"pubmed/{doc['pmid']}.json")
        object.put(Body=json.dumps(doc))
        log.debug(f"Saving {doc['pmid']}")
    except Exception as e:
        log.error(f"Problem saving pubmed: {doc['pmid']} from fn: {fn} {str(e)}")


def delete_s3(pmid, fn):
    """Delete pubmed record from S3"""

    log.debug(f"Deleting {pmid}")

    try:
        s3.Object(S3_BUCKET_NAME, f"pubmed/{pmid}.json").delete()
    except ClientError as e:
        log.error(f"Problem deleting pubmed: {pmid} from fn: {fn} {str(e)}")


def parse_xml(content: str, fn: str):
    """Convert PubmedArticles to JSON docs - yields JSON doc"""

    try:
        root = etree.XML(content)
    except Exception as e:
        log.exception(f"Problem processing {fn}")
        return False

    article_cnt = 0

    for article in root.getchildren():
        pmid = article.xpath(".//PMID/text()")[0]
        log.debug(f"Processing PMID: {pmid}")

        article_cnt += 1

        # if article_cnt > 5:
        #     return article_cnt

        if article.tag == "PubmedArticle":
            doc = convert.parse_journal_article_record(article)
            save_to_s3(doc, fn)
        elif article.tag == "PubmedBookArticle":
            doc = convert.parse_book_record(article)
            save_to_s3(doc, fn)
        elif article.tag == "DeleteCitation":
            pmid = article.xpath(".//PMID/text()")[0]
            delete_s3(pmid, fn)
        else:
            pmid = article.xpath(".//PMID/text()")[0]
            print(f"Unprocessed article: {pmid} type: {article.tag}")

    return article_cnt


def pubmed_worker(task_queue, done_queue):
    """Process pubmed ftp file"""

    while not task_queue.empty():
        fn = task_queue.get()
        log.info(f"Starting to process {fn}")
        start_time = datetime.datetime.now()
        pm_file = get_pubmed_file(fn)
        article_cnt = parse_xml(pm_file, fn)
        if article_cnt:
            duration_sec = (datetime.datetime.now() - start_time).total_seconds()

            done_queue.put({"article_cnt": article_cnt, "fn": fn, "duration_sec": duration_sec})


def main():

    multiprocessing.set_start_method("spawn")

    # Create queues
    baseline_task_queue = Queue()
    update_task_queue = Queue()
    done_queue = Queue()

    # Load task_queue
    load_task_queues(baseline_task_queue, update_task_queue)

    procs = []

    # Store finished files in a state file
    finished_proc = Process(target=finished_tasks, args=(done_queue,))
    finished_proc.start()

    # Start pubmed processing
    for i in range(int(NUMBER_OF_PROCESSES)):
        log.info(f"Starting pubmed processor {i}")
        proc = Process(target=pubmed_worker, args=(baseline_task_queue, done_queue))
        procs.append(proc)
        proc.start()

    # complete the processes
    for proc in procs:
        proc.join()

    # Process updatefiles -- Needs to be done after baseline is completed and in order so this is not parallelized
    proc = Process(target=pubmed_worker, args=(update_task_queue, done_queue))
    proc.start()
    proc.join()

    finished_proc.join()


if __name__ == "__main__":
    main()
