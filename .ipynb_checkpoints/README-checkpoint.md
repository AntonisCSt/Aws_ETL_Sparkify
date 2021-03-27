# Hi!

# Welcome to my Data modeling and Postgresql ETL mini-project!

## In this project we are creating a relational star-shape database for sparkify

## What is spakrify?

#### """Sparkify is a start-up that wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app."""

#### So you will ask....
#### Ok Antonis, how do I create the database?

#### Well its easy!
##### 0) Create a redshift cluster and add to dwh.cfg the appropriate fields for connecting it to our S3 bucket
##### 1) Create the tables by running create_tables.py
##### 2) Create the starch schema database by running etl.py

 
### Code Description:

create_tables.py drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

etl.py loads the json data from the S3 bucket, stages it to two tables (event and songs tables) then creates the star schema database

sql_queries.py contains all your sql queries, and is imported into the last three files above.
README.md provides discussion on your project.


## Check out the ETL sketch of the project
![alt text](ETL.png)





## Check out the Star schema result of our loaded database (after staging as part of the DWH)
![alt text](Schema_sql.png)



Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 


## OPTIONAL: Question for the reviewer
> **Question: How can I make the staging of the events table go faster?**