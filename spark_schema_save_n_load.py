##### READ SPARK DATAFRAME
df = spark.read.option("header", "true").option("inferSchema", "true").csv(fname)
# store the schema from the CSV w/ the header in the first file, and infer the types for the columns
df_schema = df.schema

##### SAVE JSON SCHEMA INTO S3 / BLOB STORAGE
# save the schema to load from the streaming job, which we will load during the next job 
dbutils.fs.rm("/home/mwc/airline_schema.json", True)

with open("/dbfs/home/mwc/airline_schema.json", "w") as f:
  f.write(df.schema.json())

##### LOAD JSON SCHEMA BACK TO DATAFRAME SCHEMA OBJECT
import json
from pyspark.sql.functions import *
from pyspark.sql.types import * 

schema = '/dbfs/home/mwc/airline_schema.json' 

with open(schema, 'r') as content_file:
  schema_json = content_file.read()

new_schema = StructType.fromJson(json.loads(schema_json))
