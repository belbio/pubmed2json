import datetime
import glob
import logging
import multiprocessing
import time
from multiprocessing import Process, Queue

import pm.arangodb as db
import pm.settings as settings
import pm.xml
from pm.arangodb import files_coll, json_coll, pubmed_db, xml_coll

log = logging.getLogger()

# Create queues
baseline_queue = Queue()
done_queue = Queue()


def finished_tasks(done_queue: Queue):
    """Write finished tasks to file"""

    total_article_cnt = 0
    total_start_time = datetime.datetime.now()
    with open("processed_files.txt", "a") as f:
        while True:
            time.sleep(300)
            while not done_queue.empty():
                d = done_queue.get()
                article_cnt = d["article_cnt"]
                fn = d["fn"]
                duration = d["duration_sec"]

                f.write(f"{fn}\n")
                f.flush()
                # Log throughput
                total_duration = (datetime.datetime.now() - total_start_time).total_seconds()

                total_articles_sec = total_article_cnt / total_duration
                msg = f"Baseline: {article_cnt} Articles/sec: {article_cnt/duration}  Duration(sec): {duration} Estimated Total Articles/Sec: {total_articles_sec} FN: {fn}"
                f.write(f"{msg}\n")
                log.info(msg)


def load_baseline_queue(baseline_queue):

    processed_files = db.get_processed_files()

    files = sorted(glob.glob(f"{settings.PUBMED_DATA_DIR}/baseline/*.gz"))
    for fn in files:
        fn = fn.replace(f"{settings.PUBMED_DATA_DIR}/", "")
        if fn in processed_files:
            log.info(f"Already processed {fn}")
            continue

        baseline_queue.put(fn)


def pubmed_file_worker(task_queue, done_queue):
    """Process pubmed files"""

    while not task_queue.empty():
        fn = task_queue.get()
        log.info(f"Starting to process {fn}")
        article_cnt, duration_sec = pm.xml.parse_pubmed_file(fn)
        if article_cnt:
            done_queue.put({"article_cnt": article_cnt, "fn": fn, "duration_sec": duration_sec})


def load_baseline():

    # Load task_queue
    load_baseline_queue(baseline_queue)

    procs = []

    # Store finished files in a state file
    finished_proc = Process(target=finished_tasks, args=(done_queue,))
    finished_proc.start()

    # Start pubmed baseline processing
    for i in range(int(settings.NUMBER_OF_PROCESSORS)):
        log.info(f"Starting pubmed processor {i}")
        proc = Process(target=pubmed_file_worker, args=(baseline_queue, done_queue))
        procs.append(proc)
        proc.start()

    # Wait for pubmed baseline files to be finished
    for proc in procs:
        proc.join()

    log.info("Finished processing baseline files")

    # Wait until the done_queue is completed
    finished_proc.join()


def load_updatefiles():

    processed_files = db.get_processed_files()

    files = sorted(glob.glob(f"{settings.PUBMED_DATA_DIR}/updatefiles/*.gz"))

    log.info("Starting to process updatefiles")

    with open("processed_files.txt", "a") as f:
        for fn in files:
            if fn in processed_files:
                log.info("Skipping already processed file: {fn}")
                continue
            fn = fn.replace(f"{settings.PUBMED_DATA_DIR}/", "")
            article_cnt, duration_sec = pm.xml.parse_pubmed_file(fn)

            msg = f"UpdateFiles: {article_cnt} Articles/sec: {article_cnt/duration_sec}  Duration(sec): {duration_sec}  FN: {fn}"
            f.write(f"{msg}\n")
            log.info(msg)


# # Multiprocessing Example Code ################################################
# multiprocessing.set_start_method("spawn")


# def pubmed_worker(task_queue, done_queue):
#     """Process pubmed ftp file"""

#     while not task_queue.empty():
#         fn = task_queue.get()
#         log.info(f"Starting to process {fn}")
#         start_time = datetime.datetime.now()
#         pm_file = get_pubmed_file(fn)
#         article_cnt = parse_xml(pm_file, fn)
#         if article_cnt:
#             duration_sec = (datetime.datetime.now() - start_time).total_seconds()

#             done_queue.put({"article_cnt": article_cnt, "fn": fn, "duration_sec": duration_sec})


# def load_task_queues(baseline_task_queue: Queue, update_task_queue: Queue):
#     """Collect pubmed file names - skipping files that have already been processed

#     file_type can be either: baseline or updatefiles
#     """

#     processed_files = {}
#     try:
#         with open("processed_files.txt", "r") as f:
#             for fn in f:
#                 fn = fn.strip()
#                 processed_files[fn] = 1
#     except Exception:
#         pass

#     file_list = []
#     with ftputil.FTPHost(FTP_HOST, "anonymous", "") as ftp_host:
#         # get list of files in repository
#         ftp_host.use_list_a_option = False

#         # Get baseline
#         ftp_host.chdir(f"/pubmed/baseline")
#         baseline_file_list = [
#             f"/pubmed/baseline/{fn}"
#             for fn in ftp_host.listdir(ftp_host.curdir)
#             if fn.endswith("xml.gz")
#         ]
#         # Get updatefiles
#         ftp_host.chdir(f"/pubmed/updatefiles")
#         update_file_list = [
#             f"/pubmed/updatefiles/{fn}"
#             for fn in ftp_host.listdir(ftp_host.curdir)
#             if fn.endswith("xml.gz")
#         ]

#     cnt = 0
#     for fn in baseline_file_list:
#         if fn not in processed_files:
#             cnt += 1
#             baseline_task_queue.put(fn)
#     log.info(f"Queued {cnt} baseline files out of {len(baseline_file_list)}")

#     cnt = 0
#     for fn in update_file_list:
#         if fn not in processed_files:
#             cnt += 1
#             update_task_queue.put(fn)
#     log.info(f"Queued {cnt} updatefiles out of {len(update_file_list)}")


# def finished_tasks(done_queue: Queue):
#     """Write finished tasks to file"""

#     checkpoint_seconds = 3600
#     baseline_cnt, updatefile_cnt, article_cnt = 0, 0, 0
#     last_article_cnt, last_updatefile_cnt = 0, 0
#     start_time = datetime.datetime.now()
#     last_time = datetime.datetime.now()
#     with open("processed_files.txt", "a") as f:

#         while True:
#             time.sleep(5)
#             while not done_queue.empty():
#                 d = done_queue.get()
#                 article_cnt = d["article_cnt"]
#                 fn = d["fn"]
#                 duration = d["duration_sec"]
#                 if "baseline" in fn:
#                     file_type = "baseline"
#                 else:
#                     file_type = "updatefile"

#                 f.write(f"{fn}\n")
#                 f.flush()
#                 # Log throughput
#                 articles_sec = article_cnt / duration
#                 if file_type == "baseline":
#                     msg = f"Articles: {article_cnt} Articles/sec: {articles_sec}  Duration(sec): {duration} Estimated Total Articles/Sec: {articles_sec * int(NUMBER_OF_PROCESSES)} FN: {fn}"
#                 else:
#                     msg = f"Articles: {article_cnt} Articles/sec: {articles_sec}  Duration(sec): {duration} FN: {fn}"

#                 log.info(msg)
