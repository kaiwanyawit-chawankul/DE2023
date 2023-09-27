# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Silver Tier
# MAGIC
# MAGIC In the last exercise, we took our data wrote it to the Parquet format, ready for us to pick up in the Silver Tier. In this exercise, we'll take our first step towards curation and cleanup by:
# MAGIC * Unpacking strings containing json to JSON
# MAGIC * Flattening our data (unpack nested structures and bring to top level)
# MAGIC
# MAGIC We'll do this for:
# MAGIC * StartTransaction Request
# MAGIC * StartTransaction Response
# MAGIC * StopTransaction Request
# MAGIC * MeterValues Request

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

dataset_name = "batch_processing_earthquake_silver"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, dataset_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the Bronze layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Bronze layer but for simplicity and consistent results [of this exercise], we're going to read in a Bronze output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!.

# COMMAND ----------

filepath = "dbfs:/FileStore/napat.ar/batch_earthquake_data_ingest/output/part-00000-tid-3799133021480717355-20bbbc36-11f8-4ba9-892f-d25ee3c13a45-1-1-c000.snappy.parquet" #helpers.download_to_local_dir(url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(filepath)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Filter

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

def only_in_range_filter(input_df: DataFrame):
    return input_df \
        .filter((year(input_df.time_stamp) >= 1995) & (year(input_df.time_stamp) <= 2023))

def make_timestamp(input_df: DataFrame):
    return input_df \
        .withColumn("time_stamp",  to_timestamp(col("date_time"), "dd-MM-yyyy HH:mm")) \
        .drop("date_time")

display(df.transform(make_timestamp).transform(only_in_range_filter))

# COMMAND ----------

from pyspark.sql.functions import col, isnull, split, element_at

def remove_non_location(input_df: DataFrame):
    return input_df \
        .filter(~isnull(col("location")) & ~isnull(col("continent")) & ~isnull(col("country")))

def make_country(input_df: DataFrame):
    return input_df \
        .withColumn("country", element_at(split(col("location"), ", "), -1))

def selected_asia_continent(input_df: DataFrame):
    return input_df \
        .filter(col("continent") == "Asia")

display(df.transform(remove_non_location).transform(make_country).transform(selected_asia_continent))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Request to Parquet
# MAGIC In this exercise, write the StartTransaction Request data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

############ SOLUTION ##############

def write_asia_earthquake_request(input_df: DataFrame):
    output_directory = f"{out_dir}/AsiaEarthQuake"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)
    

write_asia_earthquake_request(
    df \
        .transform(remove_non_location) \
        .transform(make_country) \
        .transform(selected_asia_continent) \
        .transform(make_timestamp) \
        .transform(only_in_range_filter))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/AsiaEarthQuake")))