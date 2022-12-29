# Opta Sports Realtime Data Ingestion

### This code can be used to ingest xml files produced by opta sports.  In particular the follow types of files are supported

* `squads`
* `f24`
* `results`

## Process


This code was developed using the following components on aws
* S3
* SQS
* Lambda
* Postgres Aurora


Once a file gets written to a particular path in s3 a message to an `sns` queue is fired with the metadata of the written file.  The `sns` queue is drained by lambda.  The lambda code which is fired is dependant upon the file name.

Lambda parses and cleans data in the files and writes the contents to Postgres.  Next a call to a store procedure moves the data from the landing tables to a 3NF data model to simplify querying.  The stored procedures are responsible for identifying new and resent rows and ensuring the referential integrity.  All the DDL to create the objects in under the `sql/` path of this repo

The ERD of the database design

![opta-sports-er](https://user-images.githubusercontent.com/11559064/209897404-5f20d554-dcc7-464c-9c2a-022711bc3376.png)
