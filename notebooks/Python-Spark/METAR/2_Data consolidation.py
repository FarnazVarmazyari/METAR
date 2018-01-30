# Databricks notebook source
# MAGIC %md # `METAR - Data consolidation`

# COMMAND ----------

# MAGIC %md As discussed in the `1_Data Download` file, all the METAR data files that were downloaded can be accessed at the `/dbfs/mnt/datalab-datasets/metar/` location. A total of 2636 files were downloaded.

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/datalab-datasets/metar | wc

# COMMAND ----------

# MAGIC %md 
# MAGIC The file names are of the format: `LOCATION_from timestamp_to timestamp.txt` 
# MAGIC 
# MAGIC For instance, the last file name indicates that:
# MAGIC 
# MAGIC  - the data in the file belongs to **ZANESVILLE, OHIO** represented by **ZZV**
# MAGIC  - the data was collected **from August 01, 2012 to September 01, 2012**

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/datalab-datasets/metar | tail -n 5

# COMMAND ----------

# MAGIC %md In the content of the file, notice that:
# MAGIC 
# MAGIC - Besides the comments that start with a `#`, the other data seems to be like a typical comma separated file. 
# MAGIC - All the missing data is specified as "`M`" instead of a `null`, therefore while reading the data we have to make sure that we consider and convert all the `M` to `null` values. 

# COMMAND ----------

# MAGIC %sh head /dbfs/mnt/datalab-datasets/metar/0A9_201208010000_201209010000.txt

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC The following piece of code gathers the data in all the files and populates it in a data frame named `df`:

# COMMAND ----------

data_dir  = "/mnt/datalab-datasets/metar/"
df = sqlContext.read.format("com.databricks.spark.csv")
                    .options(header='true', inferSchema='true', comment="#", mode='DROPMALFORMED', nullValue="M")
                    .load(data_dir+'*.txt')

type(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC The variables in our data set and their corresponding datatypes are:

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Following is a one line description of all the variables in our data set. We will discuss each of these variables in detail in our next file named `3_Understanding the data`:
# MAGIC 
# MAGIC ###Variable Description
# MAGIC 
# MAGIC  - **`station`**: Three or four character site identifier
# MAGIC  - **`valid`**: Timestamp of the observation
# MAGIC  - **`tmpf`**: Air temperature in Fahrenheit, typically @ 2 meters
# MAGIC  - **`dwpf`**: Dew point temperature in Fahrenheit, typically @ 2 meters
# MAGIC  - **`relh`**: Relative humidity in %
# MAGIC  - **`drct`**: Wind Direction in degrees from north
# MAGIC  - **`sknt`**: Wind Speed in knots
# MAGIC  - **`p01i`**: One hour precipitation for the period from the observation time to the time of the previous hourly precipitation reset. This varies slightly by site. Values are in inches. This value may or may not contain frozen precipitation melted by some device on the sensor or estimated by some other means. Unfortunately, we do not know of an authoritative database denoting which station has which sensor.
# MAGIC  - **`alti`**:Pressure altimeter in inches
# MAGIC  - **`mslp`**:Sea Level Pressure in millibar
# MAGIC  - **`vsby`**:Visibility in miles
# MAGIC  - **`gust`**:Wind Gust in knots
# MAGIC  - **`skyc1`**:Sky Level 1 Coverage
# MAGIC  - **`skyc2`**:Sky Level 2 Coverage
# MAGIC  - **`skyc3`**:Sky Level 3 Coverage
# MAGIC  - **`skyc4`**:Sky Level 4 Coverage
# MAGIC  - **`skyl1`**:Sky Level 1 Altitude in feet
# MAGIC  - **`skyl2`**:Sky Level 2 Altitude in feet
# MAGIC  - **`skyl3`**:Sky Level 3 Altitude in feet
# MAGIC  - **`skyl4`**:Sky Level 4 Altitude in feet
# MAGIC  - **`presentwx`**:Present Weather Codes (space seperated)
# MAGIC  - **`metar`**:unprocessed reported observation in METAR format
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Total number of records in our data frame:

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change the data type of the `valid` column since it is time stamp and not a string:

# COMMAND ----------

df = df.withColumn('valid', df["valid"]
       .cast('timestamp'))
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Strip the space that existed in front of all column names:

# COMMAND ----------

for name in df.schema.names:
  df= df.withColumnRenamed(name, name.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC Check if our data has any missing values:

# COMMAND ----------

from pyspark.sql.functions import col,sum
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC The following columns have no `null` values: 
# MAGIC 
# MAGIC - `station`
# MAGIC - `valid`
# MAGIC - `lon`
# MAGIC - `lat`
# MAGIC - `metar`
# MAGIC 
# MAGIC The next file named `3_Understanding the Data` talks about the 

# COMMAND ----------

# MAGIC %md **-------------------------------------------------------------------------------------------------------------------------------*The End*---------------------------------------------------------------------------------------------------------------------------------**