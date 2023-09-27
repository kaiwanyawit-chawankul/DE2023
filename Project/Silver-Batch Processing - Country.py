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

dataset_name = "batch_processing_country_silver"

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

filepath = "dbfs:/FileStore/kaiwanyawit.c/batch_contry_data_ingest/output/part-00000-tid-7273449015298497755-7ee26e80-64d0-4b3c-968c-21b8e1e1c976-10-1-c000.snappy.parquet" #helpers.download_to_local_dir(url)

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

def asia_country_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    selected_continent = "Asia"
    ###
    return input_df.filter(input_df.Continent == selected_continent)

display(df.transform(asia_country_filter))

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

def write_asia_country_request(input_df: DataFrame):
    output_directory = f"{out_dir}/AsiaCountry"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)
    

write_asia_country_request(df.transform(asia_country_filter))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/AsiaCountry")))