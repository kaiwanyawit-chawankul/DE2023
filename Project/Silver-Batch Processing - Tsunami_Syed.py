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

dataset_name = "batch_processing_tsunami_silver"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, dataset_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

filepath = "dbfs://{working_directory}/batch_tsunami_data_ingest/output/part-00000-tid-7457436861026864621-d5198723-111d-4d9d-9f72-567f3ec1fdef-18-1-c000.snappy.parquet" 

# COMMAND ----------

bronze_input_location = working_directory.replace("batch_processing_tsunami_silver", filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the Bronze layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Bronze layer but for simplicity and consistent results [of this exercise], we're going to read in a Bronze output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!.

# COMMAND ----------

# filepath = "dbfs:/FileStore/Data_Nerds/Data_Store/bronze/imdb_top_1000.csv"

# COMMAND ----------

filepath = "dbfs://{working_directory}/batch_tsunami_data_ingest/output/part-00000-tid-7457436861026864621-d5198723-111d-4d9d-9f72-567f3ec1fdef-18-1-c000.snappy.parquet" 

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(f"{bronze_input_location}/output")
    return df
    
df = read_parquet(filepath)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Filter

# COMMAND ----------

# from pyspark.sql.functions import make_timestamp
import pyspark.sql.functions as F

def only_in_range_filter(input_df: DataFrame):
    return input_df \
        .filter((input_df.Year >= 1995) & (input_df.Year <= 2023))

def make_timestamp(input_df: DataFrame):
    return input_df \
        .withColumn("Month", F.lpad(F.col("Mo"), 2, "0")) \
        .withColumn("Day", F.lpad(F.col("Dy"), 2, "0")) \
        .withColumn("Hour", F.coalesce(F.lpad(F.col("Hr"), 2, "0"), F.lit("00"))) \
        .withColumn("Min", F.coalesce(F.lpad(F.col("Mn"), 2, "0"), F.lit("00"))) \
        .withColumn("time_stamp",  F.to_timestamp(F.concat_ws(" ", F.concat_ws("-", F.col("Year"), F.col("Month"), F.col("Day")), F.concat_ws(":", F.col("Hour"), F.col("Min"))), "yyyy-MM-dd HH:mm")) \
        .drop(*["Year", "Mo", "Dy", "Hr", "Mn", "Sec", "Month", "Day", "Hour", "Min"])

display(df.transform(only_in_range_filter).transform(make_timestamp))

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

def write_tsunami_request(input_df: DataFrame):
    output_directory = f"{out_dir}/Tsunami"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)
    

write_tsunami_request(df.transform(only_in_range_filter).transform(make_timestamp))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/Tsunami")))