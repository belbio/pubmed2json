import copy
import datetime
import json
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any, List, Mapping

from pm.arangodb import files_coll, json_coll, pubmed_db, xml_coll
from lxml import etree

log = logging.getLogger(__name__)

# TODO - problems converting date for PMIDs: 30479086, 15517475


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


def process_pub_date(pmid, year, mon, day, medline_date):
    """Create pub_date from what Pubmed provides in Journal PubDate entry
    """

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
                log.error(f"Problem converting {year} {mon} {day} to pubdate for PMID:{pmid}")

        elif year:
            pub_date = f"{year}-{mon}-{day}"

    return pub_date


def parse_book_record(root) -> dict:
    """Parse Pubmed Book entry"""

    doc = {
        "abstract": "",
        "pmid": "",
        "title": "",
        "authors": [],
        "pub_date": "",
        "journal_iso_title": "",
        "journal_title": "",
        "doi": "",
        "compounds": [],
        "mesh": [],
    }

    doc["pmid"] = root.xpath(".//PMID/text()")[0]

    doc["title"] = next(iter(root.xpath(".//BookTitle/text()")))

    doc["authors"] = []
    for author in root.xpath(".//Author"):
        last_name = next(iter(author.xpath("LastName/text()")), "")
        first_name = next(iter(author.xpath("ForeName/text()")), "")
        initials = next(iter(author.xpath("Initials/text()")), "")
        if not first_name and initials:
            first_name = initials
        doc["authors"].append(f"{last_name}, {first_name}")

    pub_year = next(iter(root.xpath(".//Book/PubDate/Year/text()")), None)
    pub_mon = next(iter(root.xpath(".//Book/PubDate/Month/text()")), "Jan")
    pub_day = next(iter(root.xpath(".//Book/PubDate/Day/text()")), "01")
    medline_date = next(
        iter(root.xpath(".//Journal/JournalIssue/PubDate/MedlineDate/text()")), None
    )

    pub_date = process_pub_date(pub_year, pub_mon, pub_day, medline_date)

    doc["pub_date"] = pub_date

    for abstracttext in root.xpath(".//Abstract/AbstractText"):
        abstext = node_text(abstracttext)

        label = abstracttext.get("Label", None)
        if label:
            doc["abstract"] += f"{label}: {abstext}\n"
        else:
            doc["abstract"] += f"{abstext}\n"

    doc["abstract"] = doc["abstract"].rstrip()

    return doc


def parse_journal_article_record(root) -> dict:
    """Parse Pubmed Journal Article record"""

    # print("Root", root)
    # pmid = root.find("PMID").text
    # print("PMID", pmid)
    # quit()

    doc = {
        "abstract": "",
        "pmid": "",
        "title": "",
        "authors": [],
        "pub_date": "",
        "journal_iso_title": "",
        "journal_title": "",
        "doi": "",
        "compounds": [],
        "mesh": [],
    }

    doc["pmid"] = root.xpath(".//PMID/text()")[0]

    doc["title"] = next(iter(root.xpath(".//ArticleTitle/text()")), "")

    # TODO https:.//stackoverflow.com/questions/4770191/lxml-etree-element-text-doesnt-return-the-entire-text-from-an-element
    atext = next(iter(root.xpath(".//Abstract/AbstractText/text()")), "")

    for abstracttext in root.xpath(".//Abstract/AbstractText"):
        abstext = node_text(abstracttext)

        label = abstracttext.get("Label", None)
        if label:
            doc["abstract"] += f"{label}: {abstext}\n"
        else:
            doc["abstract"] += f"{abstext}\n"

    doc["abstract"] = doc["abstract"].rstrip()

    doc["authors"] = []
    for author in root.xpath(".//Author"):
        last_name = next(iter(author.xpath("LastName/text()")), "")
        first_name = next(iter(author.xpath("ForeName/text()")), "")
        initials = next(iter(author.xpath("Initials/text()")), "")
        if not first_name and initials:
            first_name = initials
        doc["authors"].append(f"{last_name}, {first_name}")

    pub_year = next(iter(root.xpath(".//Journal/JournalIssue/PubDate/Year/text()")), None)
    pub_mon = next(iter(root.xpath(".//Journal/JournalIssue/PubDate/Month/text()")), "Jan")
    pub_day = next(iter(root.xpath(".//Journal/JournalIssue/PubDate/Day/text()")), "01")
    medline_date = next(
        iter(root.xpath(".//Journal/JournalIssue/PubDate/MedlineDate/text()")), None
    )

    pub_date = process_pub_date(doc["pmid"], pub_year, pub_mon, pub_day, medline_date)

    doc["pub_date"] = pub_date
    doc["journal_title"] = next(iter(root.xpath(".//Journal/Title/text()")), "")
    doc["joural_iso_title"] = next(iter(root.xpath(".//Journal/ISOAbbreviation/text()")), "")
    doc["doi"] = next(iter(root.xpath('.//ArticleId[@IdType="doi"]/text()')), None)

    doc["compounds"] = []
    for chem in root.xpath(".//ChemicalList/Chemical/NameOfSubstance"):
        chem_id = chem.get("UI")
        doc["compounds"].append({"id": f"MESH:{chem_id}", "name": chem.text})

    compounds = [cmpd["id"] for cmpd in doc["compounds"]]
    doc["mesh"] = []
    for mesh in root.xpath(".//MeshHeading/DescriptorName"):
        mesh_id = f"MESH:{mesh.get('UI')}"
        if mesh_id in compounds:
            continue
        doc["mesh"].append({"id": mesh_id, "name": mesh.text})

    return doc


def get_pubmed(pmid: str) -> Mapping[str, Any]:
    """Get pubmed xml for pmid and convert to JSON

    Remove MESH terms if they are duplicated in the compound term set

    ArticleDate vs PubDate gets complicated: https:.//www.nlm.nih.gov/bsd/licensee/elements_descriptions.html see <ArticleDate> and <PubDate>
    Only getting pub_year at this point from the <PubDate> element.

    Args:
        pmid: pubmed id number as a string

    Returns:
        pubmed json
    """

    doc = {
        "abstract": "",
        "pmid": pmid,
        "title": "",
        "authors": [],
        "pub_date": "",
        "joural_iso_title": "",
        "journal_title": "",
        "doi": "",
        "compounds": [],
        "mesh": [],
    }

    try:
        pubmed_url = PUBMED_TMPL.replace("PMID", str(pmid))
        r = get_url(pubmed_url)
        content = r.content
        log.debug(f"Getting Pubmed URL {pubmed_url}")
        root = etree.fromstring(content)

    except Exception as e:
        log.error(
            f"Bad Pubmed request, status: {r.status_code} error: {e}",
            url=f'{PUBMED_TMPL.replace("PMID", pmid)}',
        )
        return {"doc": {}, "message": f"Cannot get PMID: {pubmed_url}"}

    doc["pmid"] = root.xpath(".//PMID/text()")[0]
    print("PMID", doc["pmid"])

    if doc["pmid"] != pmid:
        log.error("Requested PMID doesn't match record PMID", url=pubmed_url)

    if root.find("PubmedArticle") is not None:
        doc = parse_journal_article_record(doc, root)
    elif root.find("PubmedBookArticle") is not None:
        doc = parse_book_record(doc, root)

    return doc


def xml_to_json(xml_doc) -> str:
    """Convert Pubmed XML to JSON"""

    json_doc = ""
    return json.dumps(json_doc)
