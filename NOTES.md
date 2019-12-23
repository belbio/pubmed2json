# Pubmed processing notes

## Commands

    wget -m ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline
    wget -m ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles

## Misc

-   Have code to convert pubmed from xml to json and store on S3 on dev:~ubuntu/pubmed_to_s3/
-   Have code to sync S3 files to Dev serve and then create sqlite3 db from them on dev:/pubmed
-   Have code in ~/belbio/pubmed_to_s3

## Blog article - nice XML Pubmed processing

http://mystatisticsblog.blogspot.com/2017/09/working-with-biggest-scientific.html

Elasticsearch and pubmed: Step 2 parsing the data into Elasticsearch
This is the second blog post discussing my project with the PubMed dataset. In the last post, I explained how to get a local copy of all the PubMed data. Here I will describe how I parse the data into my elasticsearch database.

The installation guidelines for an elasticsearch server are given here:

1. First download the latest version, in my case 5.5.2

    curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.2.tar.gz

2. Untar the file you just downloaded

    tar -xvf elasticsearch-5.5.2.tar.gz

3. Enter into the folder which was created by the previous command

    cd elasticsearch-5.5.2/bin

4. Now you can start the elasticsearch server with ./elasticsearch. In general, it is a good idea to set the swap and heap size using the corresponding environment variable. The recommendation is to use half of your available memory, which in my case is 4Gb, but I run other stuff on my machine and therefore use only 1Gb

    ES_JAVA_OPTS="-Xms1g -Xmx1g" ./elasticsearch

5. Now we need a python interface for elasticsearch. Of course, it is possible to directly interact with the elasticsearch server using curl (e.g. pycurl), but there are some interfaces which allow you to get away from the rather messy elasticsearch syntax. To be honest none of the available python packages totally convinced me, but the current standard seems to be the one which is also called elasticsearch (see e.g. here for more details)

    pip install elasticsearch

With this we can contact the elasticsearch server within our python program by including the following lines
from elasticsearch import Elasticsearch

    es = Elasticsearch(hosts=['localhost:9200'])

Now we have to create an elasticsearch index (in other database systems this is called a table). This index describes the structure in which our data will be stored. I decided to store the data in 5 shards, which means that the data will be divided into 5 index files, each of which can be stored on a different machine. For now, I am not using any replicas (copies of shards). Replicas increase redundancy which allows retrieving data even if one or multiple servers are down. The decision how many shards to use is quite important since it cannot easily be changed in the future, while replicas can be added at any time.

Below I posted the function which creates the index. The settings option is used to set the number of shards and replicas together with some customised filters which I will describe in a second. The mapping describes the data vector and how it will be stored. Here I will keep things simple and only store the title, abstract and creation_date of the papers, even though the .gz files we downloaded contain much more information.

    def create_pubmed_paper_index():
        settings = { # changing the number of shards after the fact is not # possible max Gb per shard should be 30Gb, replicas can # be produced anytime # https://qbox.io/blog/optimizing-elasticsearch-how-many-shards-per-index
        "number_of_shards" : 5,
        "number_of_replicas": 0
        }
        mappings = {
            "pubmed-paper": {
                "properties" : {
                "title": { "type": "string", "analyzer": "standard"},
                "abstract": { "type": "string", "analyzer": "standard"},
                "created_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                    }
                }
            }
        }
        es.indices.delete(index=index_name, ignore=[400, 404])
        es.indices.create(
            index=index_name,
            body={ 'settings': settings,
                'mappings': mappings
            },
            request_timeout=30
        )

        return

Elasticsearch is a non SQL database, meaning it does not follow the SQL syntax or functionality. If you are used to SQL databases, this will require some re-thinking. For example there is no way to join indices easily, which is a fundamental principle of SQL. This limits what you can do with elasticsearch significantly.

However, joins are very slow, and elasticsearch is all about speed. If you think about it, there is almost always a way around joins, all what you have to do is to store the data in the way you want to retrieve it. This can sometimes be ugly and requires a lot of memory to store redundant data, but without having to perform joins at runtime, it can be very fast.

Another way how elasticsearch saves time, is by processing the document directly when indexed. I used the standard analyzer, which lower cases and tokenizes all words. So for example the sentence

    "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."

would be stored as:

    [ the, 2, quick, brown, foxes, jumped, over, the, lazy, dog's, bone ]

Again, this will speed up the required processing steps at runtime.

The function above creates the elasticsearch index. Now we have to read the data into this index. For that we write a small function, which unzips the files and builds an Element tree using xml.etree.cElementTree. Note that building such a tree can quickly lead to memory issues, since our .gz files are several Gb in size. So you should stay away from the often used parse() function which would eventually load the entire file into memory. Below I use the iterparse() function, which allows us to discard the elements after we have written them to the database.

    import xml.etree.cElementTree as ET # C implementation of ElementTree

    def fill_pubmed_papers_table(list_of_files): # Loop over all files, extract the information and index in bulk
        for i, f in enumerate(list_of_files):
            print("Read file %d filename = %s" % (i, f))
            time0 = time.time()
            time1 = time.time()
            inF = gzip.open(f, 'rb') # we have to iterate through the subtrees, ET.parse() would result # in memory issues
            context = ET.iterparse(inF, events=("start", "end")) # turn it into an iterator
            context = iter(context)

            # get the root element
            event, root = context.next()
            print("Preparing the file: %0.4fsec" % ((time.time() - time1)))
            time1 = time.time()

            documents = []
            time1 = time.time()
            for event, elem in context:
                if event == "end" and elem.tag == "PubmedArticle":
                    doc, source = extract_data(elem)
                    documents.append(doc)
                    documents.append(source)
                    elem.clear()
            root.clear()
            print("Extracting the file information: %0.4fsec" %
                ((time.time() - time1)))
            time1 = time.time()

            res = es.bulk(index=index_name, body=documents, request_timeout=300)
            print("Indexing data: %0.4fsec" % ((time.time() - time1)))
            print("Total time spend on this file: %0.4fsec\n" %
                ((time.time() - time0)))
            os.remove(f) # we directly remove all processed files

        return

This function is looking for elements with the tag PubmedArticle, which we pass on to the extract_data() function. In that function, we extract the information we need. To write such a function we need to know the internal structure of the pubmed xml files. To get an idea how that structure might look like, you could print out one element using

    def prettify(elem):
        from bs4 import BeautifulSoup # just for prettify
        '''Return a pretty-printed XML string for the Element.'''

        return BeautifulSoup(ET.tostring(elem, 'utf-8'), "xml").prettify()

I am using BeautifulSoup to produce a readable output since the equivalent functionality in cElementTree doesn't look as nice.

Without going into any more detail, here is the function which can extract the relevant information and store it in a class element

    def extract_data(citation):
        new_pubmed_paper = Pubmed_paper()

        citation = citation.find('MedlineCitation')

        new_pubmed_paper.pm_id = citation.find('PMID').text
        new_pubmed_paper.title = citation.find('Article/ArticleTitle').text

        Abstract = citation.find('Article/Abstract')
        if Abstract is not None:
            # Here we discart information about objectives, design,
            # results and conclusion etc.
            for text in Abstract.findall('AbstractText'):
                if text.text:
                    if text.get('Label'):
                        new_pubmed_paper.abstract += '<b>' + text.get('Label') + '</b>: '
                    new_pubmed_paper.abstract += text.text + '<br>'

        DateCreated = citation.find('DateCreated')
        new_pubmed_paper.created_datetime = datetime.datetime(
            int(DateCreated.find('Year').text),
            int(DateCreated.find('Month').text),
            int(DateCreated.find('Day').text)
        )
        doc, source = get_es_docs(new_pubmed_paper)
        del new_pubmed_paper
        return doc, source

where the class Pubmed_paper() is defined as

    class Pubmed_paper():
        ''' Used to temporarily store a pubmed paper outside es '''
        def **init**(self):
        self.pm_id = 0 # every paper has a created_date
        self.created_datetime = datetime.datetime.today()
        self.title = ""
        self.abstract = ""

        def __repr__(self):
            return '<Pubmed_paper %r>' % (self.pm_id)

        and the function which writes the doc and source dictionaries is

    def get_es_docs(paper):
        source = {
            'title': paper.title,
            'created_date': paper.created_datetime.date(),
            'abstract': paper.abstract
        }
        doc = {
            "index": {
                "\_index": index_name,
                "\_type": type_name,
                "\_id": paper.pm_id
            }
        }
        return doc, source

To read all the files into the database will take a few hours.

This post was a bit code heavy, but now that we have written the entire dataset into elasticsearch, we can easily access it. In the next post we will start a small project using this large dataset and making use of the fast elasticsearch database. You can access the code used in this post at GitHub. Let me know if you have any comments/questions below.
cheers
Florian
