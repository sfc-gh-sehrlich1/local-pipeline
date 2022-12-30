import os
from snowflake.snowpark import Session
import cfg as cfg
from snowflake.snowpark.functions import udf, col


##### Establish the Connection
connection_parameters = cfg.connection_parameters
session = Session.builder.configs(connection_parameters).create()

# Initialize variables
stage_name = "@json_file_stage"
table_name = ""

### Take the files from local folder and load them to a stage
try:
    put_result = session.file.put("/uploadablefiles/*", stage_name)
except:
    print("There was an error placing your files to stage")


## Copy files into the table of your choice
if put_result[0].status == 'UPLOADED':
    dfraw = session.read.json(f"@json_file_stage/{put_result[0].target}")
    try:
        dfraw.copy_into_table(table_name)
        print("Copy Into successful")
    except:
        print("there was an error: ")