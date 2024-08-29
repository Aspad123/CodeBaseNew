# Databricks notebook source
# Findout the corrupt records in a csv file if any column contains corrupt records

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, isnan
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('carat', DoubleType(), True),
    StructField('cut', StringType(), True),
    StructField('color', StringType(), True),
    StructField('clarity', StringType(), True),
    StructField('depth', DoubleType(), True),
    StructField('table', DoubleType(), True),
    StructField('price', IntegerType(), True),
    StructField('x', DoubleType(), True),
    StructField('y', DoubleType(), True),
    StructField('z', DoubleType(), True),
    StructField('_corrupt_record', StringType(), True)
    ])

diamonds_with_wrong_schema = spark.read.format('csv').option('header',True).schema(schema).load('s3://aspad-22082024-test/2024_05_08T04_08_53Z/daimonds.csv')

bad_record_df = diamonds_with_wrong_schema.filter(
    (col('id').isNull()) |
    (col('carat').isNull()) |
    (col('carat') < 0) |
    (col('cut').isNull()) |
    (col('color').isNull()) |
    (col('clarity').isNull()) |
    (col('depth').isNull()) |
    (col('table').isNull()) |
    (col('price').isNull()) |
    (isnan(col('price'))) |
    (col('x').isNull()) |
    (col('y').isNull()) |
    (col('z').isNull()))


good_record_df = diamonds_with_wrong_schema.subtract(bad_record_df)

display(bad_record_df)

display(good_record_df)

# COMMAND ----------

ret_df = good_record_df.select(col('id').alias('customer_id'),col('carat').alias('customer_carat'))
#display(ret_df)
print(good_record_df.columns)

# COMMAND ----------

# Modify the original file and add a column '_corrupt_records' in that file with corrupt record details in a csv file 


# COMMAND ----------

# Create a table claims and create a view claims_view from the table claims

# COMMAND ----------

# Create a new column "alcoholic_status" that indicates if a person is alcoholic or non-alcoholic using json file
#Create a new column "diabetes_category" that indicates if a person has diabetes or not
# Create a new column "hypertension_severe_category" that indicates if a person has severe hypertension or not
# Select the required columns from transform_df3
