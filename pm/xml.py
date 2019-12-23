import datetime
import gzip
import logging
import re
import time

import pm.arangodb as db
import pm.settings as settings
from lxml import etree as ET
from lxml.etree import Element
from pm.arangodb import files_coll, json_coll, pubmed_db, xml_coll

log = logging.getLogger()

# DTD for pubmed xml files
# http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd

# print etree.tostring(xml_root, pretty_print=True)


def first_true(iterable, default=False, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)


def process_deletions(deletions: Element):

    # Check out /sdata/pubmed/updatefiles/pubmed19n1302.xml.gz for this example
    # <DeleteCitation>
    #   <PMID Version="1">29413257</PMID>
    #   <PMID Version="1">29604678</PMID>
    #   <PMID Version="1">31283596</PMID>
    # </DeleteCitation>
    pass  # TODO


def parse_pubmed_file(filename: str) -> int:
    """Parse baseline and updatefiles"""

    path_fn = f"{settings.PUBMED_DATA_DIR}/{filename}"

    start_time = datetime.datetime.now()

    article_cnt = 0
    with gzip.open(path_fn, "rb") as f:
        context = ET.iterparse(f, events=("start", "end"))  # turn it into an iterator
        context = iter(context)
        for event, elem in context:
            if event == "end" and elem.tag in ["PubmedArticle", "PubmedBookArticle"]:
                article_cnt += 1

                # if article_cnt > 1000:
                #     quit()

                process_xml_record(elem, filename=filename)
                elem.clear()
            elif event == "end" and elem.tag == "DeleteCitation":
                process_deletions(elem)
            elif event == "end" and elem.tag in ["DeleteDocument", "BookDocument"]:
                log.warning(f"{filename} has the {elem.tag} tag and is not being processed")

    end_time = datetime.datetime.now()
    duration_sec = (end_time - start_time).total_seconds()
    db.add_processed_filename(filename, article_cnt, duration_sec)

    return article_cnt, duration_sec


def process_xml_record(record: Element, filename: str = ""):
    """Save to Pubmed XML sqlite database

    article: the <PubmedArticle> element
    """

    citation = record.find("MedlineCitation")
    pmid = citation.find("PMID").text

    # t0 = time.time()
    xml_record_str = ET.tostring(record, xml_declaration=True).decode("utf-8")
    # print("Time", (time.time() - t0) * 1000.0, "ms")

    record_dict = convert_record(pmid, record)
    # print(xml_record_str)
    import json

    # print("DumpVar:\n", json.dumps(record_dict, indent=4))

    # print(pmid, prettify(article))

    try:
        db.add_xml(pmid, xml_record_str)
        db.add_json(pmid, record_dict)
    except Exception as e:
        log.exception(f"Problem adding PMID: {pmid} from {filename} - error: {str(e)}")


def convert_record(pmid: str, root: Element) -> dict:
    """Convert pubmed article from xml to dict"""

    """Parse Pubmed Journal Article record"""

    doc = {
        "pmid": "",
        "title": "",
        "abstract": "",
        "authors": [],
        "pub_date": "",
        "journal_iso_title": "",
        "journal_title": "",
        "article_types": [],
        "doi": "",
        "compounds": [],
        "mesh": [],
    }

    doc["pmid"] = pmid

    # Get Title
    iterable = [
        root.xpath(".//Article/ArticleTitle/text()"),
        root.xpath(".//Article/BookTitle/text()"),
    ]
    title = first_true(iterable, default="")
    if not title:
        log.warning(f"Missing title for pmid: {pmid}")
    else:
        doc["title"] = title[0]

    # Get Abstract
    doc["abstract"] = get_abstract(root.find(".//MedlineCitation/Article"))

    # Get Publication types
    for pub_type in root.xpath(".//Article/PublicationTypeList/PublicationType"):
        doc["article_types"].append(pub_type.text)

    # Get Authors
    doc["authors"] = []
    for author in root.xpath(".//Author"):
        last_name = next(iter(author.xpath("LastName/text()")), "")
        first_name = next(iter(author.xpath("ForeName/text()")), "")
        initials = next(iter(author.xpath("Initials/text()")), "")
        if not first_name and initials:
            first_name = initials
        doc["authors"].append(f"{last_name}, {first_name}")

    # Get Pub Date
    doc["pub_date"] = process_pub_date(root)

    # Get metadata
    doc["journal_title"] = next(iter(root.xpath(".//Journal/Title/text()")), "")
    doc["journal_iso_title"] = next(iter(root.xpath(".//Journal/ISOAbbreviation/text()")), "")
    doc["doi"] = next(iter(root.xpath('.//ArticleId[@IdType="doi"]/text()')), None)

    # Get compound list
    doc["compounds"] = []
    for chem in root.xpath(".//ChemicalList/Chemical/NameOfSubstance"):
        doc["compounds"].append({"id": f"MESH:{chem.get('UI')}", "name": chem.text})

    compounds = [cmpd["id"] for cmpd in doc["compounds"]]

    # Get MESH list - minus anything in the compound list
    doc["mesh"] = []
    for mesh in root.xpath(".//MeshHeading/DescriptorName"):
        mesh_id = mesh.get("UI")
        if mesh_id in compounds:
            continue
        doc["mesh"].append({"id": f"MESH:{mesh_id}", "name": mesh.text})

    return doc


def node_text(node):
    """Needed for things like abstracts which have internal tags (see PMID:27822475)"""

    if node.text:
        result = node.text
    else:
        result = ""
    for child in node:
        if child.tail is not None:
            result += child.tail
    return result


def get_abstract(root):

    # TODO https:.//stackoverflow.com/questions/4770191/lxml-etree-element-text-doesnt-return-the-entire-text-from-an-element
    # atext = next(iter(root.xpath(".//Abstract/AbstractText/text()")), "")

    abstract = ""
    for abstracttext in root.xpath(".//Abstract/AbstractText"):
        abstext = node_text(abstracttext)

        label = abstracttext.get("Label", None)
        if label:
            abstract += f"{label}: {abstext}\n"
        else:
            abstract += f"{abstext}\n"

    return abstract.rstrip()


def process_pub_date(root):
    """Create pub_date from what Pubmed provides in Journal PubDate entry
    """

    year = next(iter(root.xpath(".//Journal/JournalIssue/PubDate/Year/text()")), None)
    mon = next(iter(root.xpath(".//Journal/JournalIssue/PubDate/Month/text()")), "Jan")
    day = next(iter(root.xpath(".//Journal/JournalIssue/PubDate/Day/text()")), "01")
    medline_date = next(
        iter(root.xpath(".//Journal/JournalIssue/PubDate/MedlineDate/text()")), None
    )

    if not year:
        year = 1900

    if medline_date:

        match = re.search(r"\d{4,4}", medline_date)
        if match:
            year = match.group(0)

        if int(year) < 1900:
            year = 1900

        if year and re.match("[a-zA-Z]+", mon):
            try:
                pub_date = datetime.datetime.strptime(f"{year}-{mon}-{day}", "%Y-%b-%d").strftime(
                    "%Y-%m-%d"
                )
            except Exception as e:
                pub_date = "1900-01-01"
                pmid = root.xpath(".//MedlineCitation/PMID/text()")
                log.error(f"Problem converting {year} {mon} {day} to pubdate for PMID:{pmid}")

        elif year:
            pub_date = f"{year}-{mon}-{day}"

    else:
        pub_date = None
        if year and re.match("[a-zA-Z]+", mon):
            try:
                pub_date = datetime.datetime.strptime(f"{year}-{mon}-{day}", "%Y-%b-%d").strftime(
                    "%Y-%m-%d"
                )
            except Exception as e:
                pub_date = "1900-01-01"
                pmid = root.xpath(".//MedlineCitation/PMID/text()")
                log.error(f"Problem converting {year} {mon} {day} to pubdate for PMID:{pmid}")

        elif year:
            pub_date = f"{year}-{mon}-{day}"

    return pub_date


# Examples ####################################################################

# def parse_baseline_file_as_generator(filename: str) -> int:
#     """Parse baseline file"""

#     start_time = datetime.datetime.now()
#     file_path = f"{settings.PUBMED_DATA_DIR}/{filename}"

#     article_cnt = 0
#     flag = False
#     pmid = None

#     with gzip.open(file_path, "rb") as f:
#         for line in f:
#             line = line.decode("utf-8")

#             # Start
#             if "<PubmedArticle>" in line:
#                 flag = True
#                 article = ""
#                 pmid = None
#                 batch_db = sysdb.begin_batch_execution(return_result=False)

#             if not pmid and "<PMID V" in line:
#                 # print("Line", line)
#                 matches = re.match(r'\s*\<PMID Version="\d+"\>(\d+)\<\/PMID', line)
#                 pmid_line = line
#                 pmid = matches.group(1)

#             if flag:
#                 article += line

#             if "</PubmedArticle>" in line:
#                 flag = False
#                 article_cnt += 1
#                 if pmid:
#                     print("PMID", pmid, "Article Count", article_cnt)
#                     yield (pmid, article)
#                 else:
#                     log.error(f"Missing pmid for {pmid_line} in file: {filename}")

#     duration = (datetime.datetime.now() - start_time).total_seconds()
#     log.info(f"Loaded {filename} Article_cnt: {article_cnt}  duration: {duration}")

#     return article_cnt


# def parse_baseline_file_as_text(filename: str) -> int:
#     """Parse baseline file"""

#     start_time = datetime.datetime.now()
#     file_path = f"{settings.PUBMED_DATA_DIR}/{filename}"

#     article_cnt = 0
#     flag = False
#     article_set = []
#     counter = 0

#     with gzip.open(file_path, "rb") as f:
#         for line in f:
#             line = line.decode("utf-8")

#             # Start
#             if "<PubmedArticle>" in line:
#                 flag = True
#                 article = ""
#                 pmid = None
#                 batch_db = sysdb.begin_batch_execution(return_result=False)

#             if "<PMID V" in line:
#                 # print("Line", line)
#                 matches = re.match(r'\s*\<PMID Version="\d+"\>(\d+)\<\/PMID', line)
#                 pmid_line = line
#                 pmid = matches.group(1)

#             if flag:
#                 article += line

#             if "</PubmedArticle>" in line:
#                 counter += 1
#                 flag = False
#                 if pmid:
#                     batch_db.collection("pubmed_xml").insert({"_key": pmid, "article": article})
#                 else:
#                     log.error(f"Missing pmid for {pmid_line} in file: {filename}")

#                 if counter % 100 == 0:
#                     batch_db.commit()

#     batch_db.commit()
#     sysdb.collection("pubmed_files").insert({"fn": filename})

#     duration = (datetime.datetime.now() - start_time).total_seconds()
#     log.info(f"Loaded {filename} Article_cnt: {article_cnt}  duration: {duration}")

#     return article_cnt


# def parse_baseline_file_as_xml(filename: str) -> int:
#     """Parse baseline file"""

#     start_time = datetime.datetime.now()
#     file_path = f"{settings.PUBMED_DATA_DIR}/{filename}"

#     article_cnt = 0
#     with gzip.open(file_path, "rb") as f:
#         context = ET.iterparse(f, events=("start", "end"))  # turn it into an iterator
#         context = iter(context)
#         for event, elem in context:
#             if event == "end" and elem.tag == "PubmedArticle":
#                 article_cnt += 1
#                 save_xml_article(elem)
#                 elem.clear()

#     pm.db.add_xml_fn(filename)
#     pm.db.commit_xml()

#     duration = (datetime.datetime.now() - start_time).total_seconds()
#     log.info(f"Loaded {filename} Article_cnt: {article_cnt}  duration: {duration}")

#     return article_cnt


# def parse_baseline_file_as_lxml(filename: str) -> int:
#     """Parse baseline file"""

#     start_time = datetime.datetime.now()
#     file_path = f"{settings.PUBMED_DATA_DIR}/{filename}"

#     article_cnt = 0
#     with gzip.open(file_path, "rb") as f:
#         context = ET.iterparse(f, events=("start", "end"))  # turn it into an iterator
#         context = iter(context)
#         for event, elem in context:
#             if event == "end" and elem.tag == "PubmedArticle":
#                 article_cnt += 1
#                 citation = elem.find("MedlineCitation")
#                 pmid = citation.find("PMID").text
#                 elem_str = ET.tostring(elem, encoding="utf-8")
#                 print("PMID", pmid, "Count", article_cnt)
#                 yield (pmid, elem_str)
#                 elem.clear()

#     duration = (datetime.datetime.now() - start_time).total_seconds()
#     log.info(f"Loaded {filename} Article_cnt: {article_cnt}  duration: {duration}")

#     return article_cnt

# class Pubmed_paper:
#     """ Used to temporarily store a pubmed paper outside es """

#     def __init__(self):
#         self.pmid = 0  # every paper has a created_date
#         self.created_datetime = datetime.datetime.today()
#         self.title = ""
#         self.abstract = ""

#     def __repr__(self):
#         return "<Pubmed_paper %r>" % (self.pm_id)


# def extract_data(citation):
#     paper = Pubmed_paper()

#     citation = citation.find("MedlineCitation")

#     paper.pmid = citation.find("PMID").text
#     paper.title = citation.find("Article/ArticleTitle").text

#     Abstract = citation.find("Article/Abstract")
#     if Abstract is not None:
#         # Here we discard information about objectives, design,
#         # results and conclusion etc.
#         for text in Abstract.findall("AbstractText"):
#             if text.text:
#                 if text.get("Label"):
#                     paper.abstract += "<b>" + text.get("Label") + "</b>: "
#                 paper.abstract += text.text + "<br>"

#     DateCreated = citation.find("DateCreated")
#     paper.created_datetime = datetime.datetime(
#         int(DateCreated.find("Year").text),
#         int(DateCreated.find("Month").text),
#         int(DateCreated.find("Day").text),
#     )

#     return paper


# def fill_pubmed_papers_table(
#     list_of_files,
# ):  # Loop over all files, extract the information and index in bulk
#     for i, f in enumerate(list_of_files):
#         print("Read file %d filename = %s" % (i, f))
#         time0 = time.time()
#         time1 = time.time()
#         inF = gzip.open(
#             f, "rb"
#         )  # we have to iterate through the subtrees, ET.parse() would result # in memory issues
#         context = ET.iterparse(inF, events=("start", "end"))  # turn it into an iterator
#         context = iter(context)

#         # get the root element
#         event, root = context.next()
#         print("Preparing the file: %0.4fsec" % ((time.time() - time1)))
#         time1 = time.time()

#         documents = []
#         time1 = time.time()
#         for event, elem in context:
#             if event == "end" and elem.tag == "PubmedArticle":
#                 doc, source = extract_data(elem)
#                 documents.append(doc)
#                 documents.append(source)
#                 elem.clear()
#         root.clear()
#         print("Extracting the file information: %0.4fsec" % ((time.time() - time1)))
#         time1 = time.time()

#         # res = es.bulk(index=index_name, body=documents, request_timeout=300)
#         print("Indexing data: %0.4fsec" % ((time.time() - time1)))
#         print("Total time spend on this file: %0.4fsec\n" % ((time.time() - time0)))
#         # os.remove(f)  # we directly remove all processed files

#     return
