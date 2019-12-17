## DATA LAKE WITH APACHE SPARK AND S3
#### INTRODUCTION
The purpose of this project is to create a Data lake for the analysis of user activity of a music streaming app called sparkify.
Currently, they don't have an easy way to query their data, which are JSON logs saved in AWS s3 bucket.
This project fetches the data from s3 using spark, processes the data and saves them back into s3 in parquet file format.

#### DATA SCHEMA/STRUCTURE
The *star schema* is used for structuring the files with `songplays` as the **fact table** and `users, songs, artists and time` as **dimension tables**
This makes the querying of the data much easier for analysis.

### HOW TO RUN THIS PROJECT

#### Required Python Packages

 - *configparser*
 - *pyspark*

> NOTE: Consider using a virtual envronment.

#### FILES

 - `etl.py` contains code to run the entire ETL process
 - `exploratory_analysis.ipynb`  This is the notebook to test out the individual operations for the ETL process

#### STARTUP
Make sure you have all the required packages installed on your machine. Creat a config file `dl.cfg` file to enter your configuration details as shown below:

```
[AWS]
AWS_ACCESS_KEY_ID=<your_access_key>
AWS_SECRET_ACCESS_KEY=<your_secret_key>
```

From the project directory run the following command:

    $ python3 etl.py


