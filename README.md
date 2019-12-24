# Pubmed to JSON

Convert pubmed XML to json and store in ArangoDB

This processes the Pubmed XML files and stores the string version of each Pubmed Article record as XML and a converted JSON format in two separate ArangoDB collections (xml, json). This is to make re-processing specific Pubmed XML into JSON easier. Longer term, I'll probably drop the XML storage.

With 10 processes running on a 32core, 96Gb RAM XEON Ubuntu server: I get about 1800 docs per second loaded. That would probably go up to 3000+ if not storing the XML for each pubmed record. 

## TODO

- Figure out how to run updatefiles using multi-processing as well - currently run one at a time in order to make sure newer updates overwrite older updates. Can probably add a check for updatefiles records to be able to handle that while loading multiple updatefiles files at a time.
- Add more documentation

## Setup

* Install poetry
* Install Arangodb
* Copy sample.env to .env and update the env vars
* `poetry install`
* Setup download of pubmed xml files to (I use lftp to mirror the files locally)
* Run main.py to start processing baseline (using multi-processing) andd then updatefiles one at a time
