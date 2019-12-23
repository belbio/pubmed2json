# Pubmed to JSON

Convert pubmed XML to json and store in ArangoDB

## Setup

* Install poetry
* Install Arangodb
* Copy sample.env to .env and update the env vars
* `poetry install`
* Setup download of pubmed xml files to (I use lftp to mirror the files locally)
* Run main.py to start processing baseline (using multi-processing) andd then updatefiles one at a time
