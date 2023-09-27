# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Gold
# MAGIC
# MAGIC Remember our domain question, **What is the final charge time and final charge dispense for every completed transaction**? It was the exercise which required several joins and window queries. :)  We're here to do it again (the lightweight version) but with the help of the work we did in the Silver Tier. 
# MAGIC
# MAGIC Steps:
# MAGIC * Match StartTransaction Requests and Responses
# MAGIC * Join Stop Transaction Requests and StartTransaction Responses, matching on transaction_id (left join)
# MAGIC * Find the matching StartTransaction Requests (left join)
# MAGIC * Calculate the total_time (withColumn, cast, maths)
# MAGIC * Calculate total_energy (withColumn, cast)
# MAGIC * Calculate total_parking_time (explode, filter, window, groupBy)
# MAGIC * Join and Shape (left join, select) 
# MAGIC * Write to Parquet
# MAGIC
# MAGIC **NOTE:** You've already done these exercises before. We absolutely recommend bringing over your answers from that exercise to speed things along (with some minor tweaks), because you already know how to do all of that already! Of course, you're welcome to freshly rewrite your answers to test yourself!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

exercise_name = "batch_processing_gold"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Silver Layer
# MAGIC Let's read the parquet files that we created in the Silver layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Silver layer but for simplicity and consistent results [of this exercise], we're going to read in a Silver output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!

# COMMAND ----------

asia_country_filepath = "dbfs:/FileStore/kaiwanyawit.c/batch_processing_country_silver/output/AsiaCountry/part-00000-tid-5881000497512877271-6eff35f5-c457-4e8f-979b-0c64be746a3a-14-1-c000.snappy.parquet"
asia_earthquake_filepath = "dbfs:/FileStore/napat.ar/batch_processing_earthquake_silver/output/AsiaEarthQuake/part-00000-tid-1437792538800422111-64676c4d-606f-46bd-96e7-b35e4f267ffe-8-1-c000.snappy.parquet"
tsunami_filepath = "dbfs:/FileStore/kaiwanyawit.c/batch_processing_tsunami_silver/output/Tsunami/part-00000-tid-6832050707734169279-82c5d9f0-1b85-4c24-a883-8e85eee57fbe-8-1-c000.snappy.parquet"

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, initcap, regexp_replace, lower

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
asia_country_df = read_parquet(asia_country_filepath)
asia_earthquake_df = read_parquet(asia_earthquake_filepath)
tsunami_df = read_parquet(tsunami_filepath)

# TODO: Move to silver
def capitalize_country(input_df: DataFrame):
    return input_df \
        .withColumn("country", lower(col("country"))) \
        .withColumn("country", regexp_replace("country", "nan", " ")) \
        .withColumn("country", initcap(col("country")))

tsunami_df = tsunami_df.withColumnRenamed("Country", "country").transform(capitalize_country)
asia_country_only = asia_country_df.select("Country").distinct()

display(asia_country_df)
display(asia_country_only)
display(asia_earthquake_df)
display(tsunami_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Match StartTransaction Requests and Responses
# MAGIC In this exercise, match StartTransaction Requests and Responses using a [left join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) on `message_id`.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, year

def distinct_country(input_df: DataFrame):
    return input_df.select("country").distinct()

display(asia_earthquake_df.transform(distinct_country))
display(tsunami_df.transform(distinct_country))

def match_dataFrame_with_contry_in_asia_with_asia_earthquake(input_df: DataFrame) -> DataFrame:
    join_type = "inner"
    
    return input_df \
        .join(asia_country_only, input_df.country == asia_country_only.Country, join_type) \
        .select(
            asia_country_only.Country.alias("Country"),
            input_df.title.alias("Title"), 
            input_df.magnitude.alias("Magnitude"),
            input_df.depth.alias("Depth"),
            input_df.tsunami.alias("Tsunami"),
            input_df.time_stamp.alias("Timestamp")
        )

def match_dataFrame_with_contry_in_asia_with_tsunami(input_df: DataFrame) -> DataFrame:
    join_type = "inner"
    
    return input_df \
        .join(asia_country_only, input_df.country == asia_country_only.Country, join_type) \
        .select(
            asia_country_only.Country.alias("Country"), 
            input_df.TsunamiNanCauseNanCode.alias("TsunamiCauseCode"),
            input_df.Deaths,
            input_df.Missing,
            input_df.Injuries,
            input_df.HousesNanDestroyed.alias("HousesDestroyed"),
            input_df.HousesNanDamaged.alias("HousesDamaged"),
            input_df.time_stamp.alias("Timestamp")
        )

display(asia_earthquake_df.transform(match_dataFrame_with_contry_in_asia_with_asia_earthquake))
display(tsunami_df.transform(match_dataFrame_with_contry_in_asia_with_tsunami))

# COMMAND ----------

def match_country_in_asia(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    join_type = "inner"
    
    return input_df \
        .join(join_df, input_df.country == join_df.Country, join_type)
    asia_earthquake_df

display(asia_earthquake_df.transform(match_country_in_asia, asia_country_df))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To answer
# MAGIC
# MAGIC What is the relationship between earthquakes and tsunamis?
# MAGIC
# MAGIC
# MAGIC What are the top 3 countries that were hit by tsunamis? Frequency, impact (damage)
# MAGIC
# MAGIC
# MAGIC What is the trend of tsunami impact on the top 3 countries over the time periods.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

asia_country_hit_by_tsunami_df = tsunami_df.transform(match_dataFrame_with_contry_in_asia_with_tsunami)
display(asia_country_hit_by_tsunami_df)

# asia_country_hit_by_tsunami_df.printSchema()


def hit_summary(input_df: DataFrame) -> DataFrame:
    join_type = "inner"
    
    return input_df \
        .groupBy(col("Country")).count().orderBy(col("count").desc())



hit_summary_df = asia_country_hit_by_tsunami_df.transform(hit_summary)
top_3_country = hit_summary_df.limit(3)

display(hit_summary_df)
display(top_3_country)

def top_3_data_filter(input_df: DataFrame) -> DataFrame:
    join_type = "inner"
    
    return input_df \
        .join(top_3_country, input_df.Country == top_3_country.Country, join_type) \
        .select(
            top_3_country.Country.alias("Country"),
            input_df.Timestamp.alias("Timestamp"), 
            # input_df.magnitude.alias("Country"),
            input_df.Deaths.alias("Deaths"),
            input_df.Missing.alias("Missing"),
            input_df.Injuries.alias("Injuries"),
            input_df.HousesDestroyed.alias("HousesDestroyed"),
            input_df.HousesDamaged.alias("HousesDamaged")
        )

display(asia_country_hit_by_tsunami_df.transform(top_3_data_filter))

def filter_by_Japan(input_df: DataFrame):
    return input_df.filter(input_df.Country == "Japan")

# spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin",False)
# display(asia_country_hit_by_tsunami_df.transform(top_3_data_filter).transform(filter_by_Japan))




# COMMAND ----------

########## SOLUTION ##########
def match_start_transaction_requests_with_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = "inner"
    ###
    return input_df.\
        join(join_df, input_df.message_id == join_df.message_id, join_type).\
        select(
            input_df.charge_point_id.alias("charge_point_id"), 
            input_df.transaction_id.alias("transaction_id"), 
            join_df.meter_start.alias("meter_start"), 
            join_df.timestamp.alias("start_timestamp")
        )
    start_transaction_response_df
display(start_transaction_response_df.transform(match_start_transaction_requests_with_responses, start_transaction_request_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Stop Transaction Requests and StartTransaction Responses
# MAGIC In this exercise, [left join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) Stop Transaction Requests and the newly joined StartTransaction Request/Response DataFrame (from the previous exercise), matching on transaction_id (left join).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  ```

# COMMAND ----------

def join_with_start_transaction_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = None
    ###
    return input_df. \
    join(join_df, input_df.transaction_id == join_df.transaction_id, join_type). \
    select(
        join_df.charge_point_id, 
        join_df.transaction_id, 
        join_df.meter_start, 
        input_df.meter_stop.alias("meter_stop"), 
        join_df.start_timestamp, 
        input_df.timestamp.alias("stop_timestamp")
    )


    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    )
)

# COMMAND ----------

############## SOLUTION ##############
def join_with_start_transaction_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = "left"
    ###
    return input_df. \
    join(join_df, input_df.transaction_id == join_df.transaction_id, join_type). \
    select(
        join_df.charge_point_id, 
        join_df.transaction_id, 
        join_df.meter_start, 
        input_df.meter_stop.alias("meter_stop"), 
        join_df.start_timestamp, 
        input_df.timestamp.alias("stop_timestamp")
    )

    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate the total_time
# MAGIC Using Pyspark functions [withColumn](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html) and [cast](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast) and little creative maths, calculate the total charging time (stop_timestamp - start_timestamp) in hours (two decimal places).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- stop_timestamp: string (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_time(input_df: DataFrame) -> DataFrame:
    seconds_in_one_hour = 3600
    ### YOUR CODE HERE
    stop_timestamp_column_name: str = None
    start_timestamp_column_name: str = None
    ###
    return input_df. \
        withColumn("total_time", col(stop_timestamp_column_name).cast("long")/seconds_in_one_hour - col(start_timestamp_column_name).cast("long")/seconds_in_one_hour). \
        withColumn("total_time", round(col("total_time").cast(DoubleType()),2))
    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time)
)



# COMMAND ----------

############# SOLUTION ###############
from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_time(input_df: DataFrame) -> DataFrame:
    seconds_in_one_hour = 3600
    ### YOUR CODE HERE
    stop_timestamp_column_name: str = "stop_timestamp"
    start_timestamp_column_name: str = "start_timestamp"
    ###
    return input_df. \
        withColumn("total_time", col(stop_timestamp_column_name).cast("long")/seconds_in_one_hour - col(start_timestamp_column_name).cast("long")/seconds_in_one_hour). \
        withColumn("total_time", round(col("total_time").cast(DoubleType()),2))
    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time)
)



# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_time_hours_e2e

test_calculate_total_time_hours_e2e(
    stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time), display
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_energy
# MAGIC Calculate total_energy (withColumn, cast)
# MAGIC Using [withColumn](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn) and [cast](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast), calculate the total energy by subtracting `meter_stop` from `meter_start`, converting that value from Wh (Watt-hours) to kWh (kilo-Watt-hours), and rounding to the nearest 2 decimal points.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  ```
# MAGIC
# MAGIC  **Hint:** Wh -> kWh = divide by 1000

# COMMAND ----------

from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    meter_stop_column_name: str = None
    meter_start_column_name: str = None
    ###
    return input_df \
        .withColumn("total_energy", (col(meter_stop_column_name) - col(meter_start_column_name))/1000) \
        .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))
    


display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy)
)

# COMMAND ----------

############ SOLUTION ############

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    meter_stop_column_name: str = "meter_stop"
    meter_start_column_name: str = "meter_start"
    ###
    return input_df \
        .withColumn("total_energy", (col(meter_stop_column_name) - col(meter_start_column_name))/1000) \
        .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))
    

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy)
)

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_energy_e2e

test_calculate_total_energy_e2e(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy), display)

# COMMAND ----------

from pyspark.sql.functions import when, sum, abs, first, last, lag
from pyspark.sql.window import Window

def calculate_total_parking_time(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    transaction_id_column_name: str = None
    ###

    window_by_transaction = Window.partitionBy(transaction_id_column_name).orderBy(col("timestamp").asc())
    window_by_transaction_group = Window.partitionBy([transaction_id_column_name, "charging_group"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return input_df.\
        withColumn("charging", when(col("value") > 0,1).otherwise(0)).\
        withColumn("boundary", abs(col("charging")-lag(col("charging"), 1, 0).over(window_by_transaction))).\
        withColumn("charging_group", sum("boundary").over(window_by_transaction)).\
        select(col(transaction_id_column_name), "timestamp", "value", "charging", "boundary", "charging_group").\
        withColumn("first", first('timestamp').over(window_by_transaction_group).alias("first_id")).\
        withColumn("last", last('timestamp').over(window_by_transaction_group).alias("last_id")).\
        filter(col("charging") == 0).\
        groupBy(transaction_id_column_name, "charging_group").agg(
            first((col("last").cast("long") - col("first").cast("long"))).alias("group_duration")
        ).\
        groupBy(transaction_id_column_name).agg(
            round((sum(col("group_duration"))/3600).cast(DoubleType()), 2).alias("total_parking_time")
        )

display(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time)
)

# COMMAND ----------

############## SOLUTION ###############
from pyspark.sql.functions import when, sum, abs, first, last, lag
from pyspark.sql.window import Window

def calculate_total_parking_time(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    transaction_id_column_name: str = "transaction_id"
    ###

    window_by_transaction = Window.partitionBy(transaction_id_column_name).orderBy(col("timestamp").asc())
    window_by_transaction_group = Window.partitionBy([transaction_id_column_name, "charging_group"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return input_df.\
        withColumn("charging", when(col("value") > 0,1).otherwise(0)).\
        withColumn("boundary", abs(col("charging")-lag(col("charging"), 1, 0).over(window_by_transaction))).\
        withColumn("charging_group", sum("boundary").over(window_by_transaction)).\
        select(col(transaction_id_column_name), "timestamp", "value", "charging", "boundary", "charging_group").\
        withColumn("first", first('timestamp').over(window_by_transaction_group).alias("first_id")).\
        withColumn("last", last('timestamp').over(window_by_transaction_group).alias("last_id")).\
        filter(col("charging") == 0).\
        groupBy(transaction_id_column_name, "charging_group").agg(
            first((col("last").cast("long") - col("first").cast("long"))).alias("group_duration")
        ).\
        groupBy(transaction_id_column_name).agg(
            round((sum(col("group_duration"))/3600).cast(DoubleType()), 2).alias("total_parking_time")
        )

display(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join and Shape
# MAGIC
# MAGIC Join and Shape (left join, select)
# MAGIC
# MAGIC Now that we have the `total_parking_time`, we can join that with our Target Dataframe (where we stored our Stop/Start Transaction data).
# MAGIC
# MAGIC Recall that our newly transformed DataFrame has the following schema:
# MAGIC ```
# MAGIC root
# MAGIC |-- transaction_id: integer (nullable = true)
# MAGIC |-- total_parking_time: double (nullable = true)
# MAGIC ```
# MAGIC  
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

def join_and_shape(input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = None
    ###
    return input_df.\
        join(joined_df, on=input_df.transaction_id == joined_df.transaction_id, how=join_type).\
        select(
            input_df.charge_point_id, 
            input_df.transaction_id, 
            input_df.meter_start, 
            input_df.meter_stop, 
            input_df.start_timestamp, 
            input_df.stop_timestamp, 
            input_df.total_time, 
            input_df.total_energy, 
            joined_df.total_parking_time
        )

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     )
)

# COMMAND ----------

########### SOLUTION ############
def join_and_shape(input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = "left"
    ###
    return input_df.\
        join(joined_df, on=input_df.transaction_id == joined_df.transaction_id, how=join_type).\
        select(
            input_df.charge_point_id, 
            input_df.transaction_id, 
            input_df.meter_start, 
            input_df.meter_stop, 
            input_df.start_timestamp, 
            input_df.stop_timestamp, 
            input_df.total_time, 
            input_df.total_energy, 
            joined_df.total_parking_time
        )

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write to Parquet
# MAGIC In this exercise, write the DataFrame `f"{out_dir}/cdr"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------

def write_to_parquet(input_df: DataFrame):
    output_directory = f"{out_dir}/cdr"
    ### YOUR CODE HERE
    mode_type: str  = None
    ###
    input_df.\
        write.\
        mode(mode_type).\
        parquet(output_directory)

write_to_parquet(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     ))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/cdr")))

# COMMAND ----------

############ SOLUTION ##############

def write_to_parquet(input_df: DataFrame):
    output_directory = f"{out_dir}/cdr"
    ### YOUR CODE HERE
    mode_type: str  = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_type).\
        parquet(output_directory)

write_to_parquet(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     ))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/cdr")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congratulations on finishing the Gold Tier exercise! Compared to a previous exercise where we did some of these exact exercises, you might have noticed that this time around, it was significantly easier to comprehend and complete because we didn't need to perform as many repetitive transformations to get to the interesting business logic.
# MAGIC
# MAGIC * What might you do with this data now that you've transformed it?