# Databricks notebook source
# MAGIC %md
# MAGIC # METAR - Understanding the Data
# MAGIC   
# MAGIC  
# MAGIC In this file, we will:
# MAGIC - Introduce each variable
# MAGIC - Check its descriptive statistics
# MAGIC - Check the percentage of missing values
# MAGIC - Perform data validation

# COMMAND ----------

# MAGIC %md
# MAGIC Load the files and consolidating them in a dataframe object named `df` as mentioned in the `2_Data Consolidation` file:

# COMMAND ----------

data_dir  = "/mnt/datalab-datasets/metar/"
df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferSchema='true', comment="#", mode='DROPMALFORMED', nullValue="M").load(data_dir+'*.txt')
    
for name in df.schema.names:
  df= df.withColumnRenamed(name, name.strip())
  

# COMMAND ----------

# MAGIC %md 
# MAGIC To run SQL queries, dataframe should be in form of a SQL table.
# MAGIC 
# MAGIC The `registerTempTable()` method allows us to use the dataframe temporarily as a table. 

# COMMAND ----------

df.registerTempTable("table")

# COMMAND ----------

# MAGIC %md The lifetime of this table is tied to the SQLContext that was used to create the DataFrame.

# COMMAND ----------

# MAGIC %md # Exploratory analysis

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Wind
# MAGIC 
# MAGIC ### Introduction
# MAGIC The data set has three columns that represent wind related information:
# MAGIC 
# MAGIC  - `drct`: Wind Direction in degrees
# MAGIC  - `sknt`: Wind Speed
# MAGIC  - `gust`: Wind Gust

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Ideally, wind velocity in METAR is either expressed in Knots (KT) or Meters per second (MPS).
# MAGIC 
# MAGIC - The first 3 digits of this number will always represent the direction that the wind is blowing from.
# MAGIC   - 0 degrees represents north
# MAGIC   - 90 degrees represents east
# MAGIC   - 180 degrees represents south
# MAGIC   - 270 degrees represents west
# MAGIC - The last 2 numbers represent the velocity of the wind (wind velocity is either measured in Meters per second (MPS) or Knots (KT)
# MAGIC 
# MAGIC Consider the following examples to better understand how to interpret metar readings:
# MAGIC 
# MAGIC Example 1: `METAR UHPP 060600Z 34003MPS 9999 SHRA OVC009CB 03/02 Q1014 NOSIG` 
# MAGIC 
# MAGIC Interpretation: Winds at UHPP are blowing from the north west direction with a speed of 3 Meters per second
# MAGIC 
# MAGIC Example 2: `KPIT 201955Z 22015KT 3/4SM R28R/2600FT TSRA OVC01OCB 18/16 A2992 RMK SLPO13 T01760158` 
# MAGIC 
# MAGIC Interpretation:  Winds at KPIT are blowing from the south west direction with a speed of 15 knots per second
# MAGIC 
# MAGIC More examples:
# MAGIC 
# MAGIC `22015G30`
# MAGIC 
# MAGIC - When the code has a "G" in the middle, it means the winds are gusting.
# MAGIC - Interpretation:  Winds are blowing from the south west direction with a velocity that ranges between 15 and 30 knots. 
# MAGIC 
# MAGIC `00000KT`
# MAGIC 
# MAGIC  - If we see all zeroes, there is no wind.
# MAGIC 
# MAGIC `20015KT 180V260`
# MAGIC 
# MAGIC - The normal winds reported in the beginning and then 2 numbers separated by a "V" means the winds are variable. 
# MAGIC - Interpretation:  Winds are blowing from the south west direction with a speed of 15 knots. The direction of the wind is variable from 180 to 260 degrees. 
# MAGIC 
# MAGIC `VRB`
# MAGIC 
# MAGIC - This code represent winds that are less than 6 knots, and variable in direction.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Missing values
# MAGIC 
# MAGIC #### Q: What percentage of observations have missing wind information?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE drct is null AND sknt is null AND gust is null

# COMMAND ----------

# MAGIC %md There are 30,262 records that do not have any of the wind related fields populated. This means, only 1% of observations have no values related to wind. The missing values either should be imputed or should be deleted before any modeling efforts.

# COMMAND ----------

# MAGIC %md  
# MAGIC 
# MAGIC ### Descriptive statistics

# COMMAND ----------

df.describe('drct', 'sknt', 'gust').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Important points to note:
# MAGIC 
# MAGIC `drct`:
# MAGIC 
# MAGIC - Since the direction is measured in degrees, we know that it can take a maximum value of 360. However, an important point is that the 0 degrees and 360 degrees is the same thing, so ideally we should treat both these values as one. 
# MAGIC 
# MAGIC `sknt`:
# MAGIC 
# MAGIC - The maximum speed that is recorded is 1279 knots (1471.85 miles per hour). This is potentially an outlier and will be handled in upcoming data preprocessing steps.
# MAGIC 
# MAGIC `gust`:
# MAGIC 
# MAGIC - The maximum gust speed that is recorded is 220 knots = 253.17 miles per hour.
# MAGIC 
# MAGIC 
# MAGIC The maximum values are too high which indicates that there are outliers in the data.

# COMMAND ----------

# MAGIC %md ### Data validation
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #### Q: How many observations have information regarding wind in the METAR but their corresponding wind related columns are not populated?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE ( METAR like "%KT%" OR METAR like "%MPS%" OR METAR like "%VRB%" OR METAR like "%G%" ) and drct is null AND sknt is null AND gust is null

# COMMAND ----------

# MAGIC %md
# MAGIC Of the 30,262 observations that have no wind data, there are 5,545 records wherein wrong wind information was captured in the METAR and thus the 3 wind related columns were not populated.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Q: What do the METAR readings look like when none of the wind variables are populated?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT drct, sknt, gust, metar
# MAGIC FROM table
# MAGIC WHERE ( METAR like "%KT%" OR METAR like "%MPS%" OR METAR like "%VRB%" OR METAR like "%G%" ) and drct is null AND sknt is null AND gust is null

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 8.88000009KT is not the accurate format for a wind reading.
# MAGIC 
# MAGIC In 18% of the cases that have missing values in wind related columns, the wind readings weren't captured properly. This problem will be addressed in handling of missing values.

# COMMAND ----------

# MAGIC %md #### Q. How many observations have wind direction equal to 360?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE drct == 360

# COMMAND ----------

# MAGIC %md 55,494 observations have wind direction of 360. The value in these observations should be changed to 0 in preprocessing steps.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As per Wikipedia, the highest surface wind speed in the United States ever officially recorded is 372 km/h at the Mount Washington Observatory. Wind speeds within certain atmospheric phenomena (such as tornadoes) may greatly exceed these values but have never been accurately measured.
# MAGIC 
# MAGIC #### Q. How many observations in our data have speed equal to or greater than 200 knots?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE sknt >=200

# COMMAND ----------

# MAGIC %md These 21 observations could be potentially outliers.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q. What do the METAR in observations that have wind speed equal to or greater than 200 knots look like? 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT Distinct sknt, drct, gust, presentwx, p01i, metar
# MAGIC FROM table
# MAGIC WHERE sknt >=200

# COMMAND ----------

# MAGIC %md
# MAGIC In most of the above cases above, the METAR reading was not properly parsed, however, there are a few cases that seem to have faulty METAR reading. For instance, a metar reading of 100905KT definitely seems wrong.

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Q: Are there cases where `sknt` and `gust` are recorded but `drct` is not recorded and the direction of the wind is not variable?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) 
# MAGIC FROM table
# MAGIC WHERE drct is null and (sknt is not null or gust is not null) and (metar not like '%VRB%')

# COMMAND ----------

# MAGIC %md
# MAGIC It is great to know that there are no such records. We have a direction for all records that have the right METAR codes as long as winds are not variable in direction. On the other hand, `drct` will never be populated when the WIND code begins with VRB - since that means that the direction of the wind is variable and a particular degree cannot be associated with it.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q: Is there a value associated with `gust` when `sknt` is `null`?

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE gust is not null and (sknt is null)

# COMMAND ----------

# MAGIC %md The result of this query shows that gust is only associated with values that have the wind speed associated with them. This result confirms the integrity of the data with respect to wind gust.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### write a para Q: METAR readings are recorded in different units, are all the values in the `sknt` in the knot unit? 
# MAGIC We need to make sure that we are comparing apples to apples!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT drct, sknt, gust, metar
# MAGIC FROM table
# MAGIC WHERE (sknt is not null) and (metar not like '%KT%')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC All the values are being stored in Knots which supports the integrity of the data. 
# MAGIC 
# MAGIC 
# MAGIC The `MPS` code in the metar column has been stripped off and the values stored in the `sknt` column are all converted from MPS to **Knots**. For example, the first row of the data set:
# MAGIC 
# MAGIC If we look at the metar column:
# MAGIC - 70 degrees is the direction of the wind
# MAGIC - 3MPS is the speed of the wind.
# MAGIC 
# MAGIC However, while storing the values in the `sknt` column, we convert the MPS to knots **(3m/s= 5.831533knots)**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Visibility
# MAGIC 
# MAGIC ## Introduction
# MAGIC The visibility portion of the METAR is coded in one of these ways: 
# MAGIC 
# MAGIC - Statute miles
# MAGIC - Meters
# MAGIC - Runway Visual Range
# MAGIC 
# MAGIC Example: 
# MAGIC 
# MAGIC KPIT 151124Z 28016G20KT **2 3/4SM R28R/2600FT** TSRA OVC01OCB 18/16 A2992 RMK SLPO13 T01760158 
# MAGIC 
# MAGIC - "2 3/4SM" represents 2 and 3 quarters miles visibility
# MAGIC - Another visibility range that can be a METAR reading is the Runway Visual Range which is the actual distance that one can see looking down the runway. In the example, R28R/2600FT signifies that the runway visual range for runway 28 Right is 2600 feet.
# MAGIC 
# MAGIC 
# MAGIC Another Example, a metar reading of **R36L/2,400 feet (731.5 m)** would mean that the runway visual range for runway 36 left is 2,400 feet.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Missing values
# MAGIC 
# MAGIC #### Q: What percentage of the data does not have any visibility value associated with it?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE vsby is null

# COMMAND ----------

# MAGIC %md 60,034 records don't have visibility data which essentially means that only **0.2%** of visibility data is missing.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data validation
# MAGIC #### Q: Are all the visibility values recorded in the same unit?
# MAGIC 
# MAGIC As mentioned above, the metar reading for visibility can be recorded in either miles, statute miles or meters. Consider the records that do not have the visibility recorded in SM (statute miles):

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT vsby, metar
# MAGIC FROM table
# MAGIC WHERE vsby is not null and metar not like '%SM%'

# COMMAND ----------

# MAGIC %md Considering the following example:
# MAGIC 
# MAGIC *0.99	PASY 020252Z 21007KT 1600 BR OVC002 08/07 A2996 RMK AO2*
# MAGIC 
# MAGIC In the above case, the visibility is recorded in meters in the METAR reading, while in the table, it is converted to SM (Statute Miles). 1,600 meters in the METAR is equal to 0.99 statute miles in the `vsby` column.
# MAGIC 
# MAGIC This confirms that the visibility information is recorded in terms of singular unit which is statute miles.

# COMMAND ----------

# MAGIC %md ### Descriptive statistics:

# COMMAND ----------

df.describe('vsby').show()

# COMMAND ----------

# MAGIC %md The maximum visibility recorded is 130 SM which is roughly equal to 1,906 football fields!
# MAGIC Once again, this indicates that data has outliers.

# COMMAND ----------

# MAGIC %md # Time

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Introduction
# MAGIC 
# MAGIC `valid` is one of the fields that does not have a missing value. 
# MAGIC 
# MAGIC To better understand the time representation in METAR consider the following example:
# MAGIC 
# MAGIC **041600Z** indicates the time of the observation. It is the day of the month (04) followed by the time of day (1600 Zulu time, which equals 4:00 pm Greenwich Mean Time or 6:00 pm local time).

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Descriptive statistics:

# COMMAND ----------

df.describe('valid').show()

# COMMAND ----------

# MAGIC %md The data as mentioned before, was collected from August 01, 2012 to September 01, 2012.

# COMMAND ----------

# MAGIC %md #Longitude & Latitude

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Descriptive statistics:

# COMMAND ----------

df.describe('lon','lat').show()

# COMMAND ----------

# MAGIC %md We can validate that all longitude and latitude values are within the range.
# MAGIC 
# MAGIC 
# MAGIC Latitude measurements range from 0° to (+/–)90°. Longitude measures how far east or west of the prime meridian a place is located. The prime meridian runs through Greenwich, England. Longitude measurements range from 0° to (+/–)180°.

# COMMAND ----------

# MAGIC %md # Temperature, Dew Point and Humidity
# MAGIC 
# MAGIC ### Introduction
# MAGIC  Next we will look at:
# MAGIC 
# MAGIC - `tmpf`: Air Temperature in Fahrenheit, typically @ 2 meters
# MAGIC - `dwpf`: Dew Point Temperature in Fahrenheit, typically @ 2 meters
# MAGIC - The above two readings are recorded in Fahrenheit
# MAGIC - `relh`: Relative Humidity in %

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Missing data:
# MAGIC #### Q: What percentage of data is missing in the `tmpf`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE tmpf is null

# COMMAND ----------

# MAGIC %md The result shows that only 0.84% of the data is null. 

# COMMAND ----------

# MAGIC %md #### Q: What percentage of data is missing in the `dwpf`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE dwpf is null

# COMMAND ----------

# MAGIC %md 1.21% of `dwpf` data is missing.

# COMMAND ----------

# MAGIC %md #### Q: What percentage of data is missing in `relh`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE relh is null

# COMMAND ----------

# MAGIC %md 1.21% of `relh` data is missing.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Descriptive statistics of the `tmpf`, `dwpf` and `relh` variables: 

# COMMAND ----------

df.describe('tmpf','dwpf','relh').show()

# COMMAND ----------

# MAGIC %md The minimum and maximum values are a little extreme which suggests the presence of outliers.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data validation
# MAGIC #### Q: What is the location of the 10 highest recorded temperatures in our data?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT station, valid, metar, tmpf
# MAGIC FROM table
# MAGIC ORDER BY tmpf desc limit 10

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC `PAGN` is located in Angoon, AK, United States. 7 out of the 10 highest recorded temperatures are from Alaska. This suggests that these observations are erroneous or outliers. After defining outliers for this variable we can get rid of them in the data preprocessing step.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Q: What is the location of 10 lowest recorded temperature in our data?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT station, valid, tmpf, metar
# MAGIC FROM table
# MAGIC WHERE tmpf is not null
# MAGIC ORDER BY tmpf asc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC The lowest temperature is in Fernandina Beach, FL in August. Most of these readings were recorded on 21st August 2012 in Florida which indicates that the data has incorrect values in this variable. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC #### Q. How many observations have negative temperature values?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE tmpf < 0

# COMMAND ----------

# MAGIC %md
# MAGIC These 14 observations need to be addressed in data preprocessing step, as they are inaccurate.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Q: What is the location of the 10 lowest recorded dew point temperature in our data?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT station, valid, tmpf, dwpf, metar
# MAGIC FROM table
# MAGIC WHERE dwpf is not null
# MAGIC ORDER BY dwpf asc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Most of these observations belongs to Sparta, IL. The values indicates that further outlier analysis is necessary. As per records, the lowest dew point ever recorded was in Las Vegas and it was -22 F.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q: How many observations have dry air indication (negative dew point)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE dwpf < 0

# COMMAND ----------

# MAGIC %md
# MAGIC It is important to define outliers for `dwpf` carefully so we do not lose the information related to dry air condition in the process of elimination of outliers.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q: How many data points have relative humidity or `relh` higher than 100%?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE relh> 100

# COMMAND ----------

# MAGIC %md Since relative humidity is relative to "saturation" above a flat surface, it is possible to have humidity exceeding 100%. However, because of the ubiquitous presence of condensation nuclei (e.g., dust, salt, etc.), relative humidity in the Earth's atmosphere typically does not exceed 100% at the surface or 102% within clouds.
# MAGIC 
# MAGIC Keeping the above point in mind, `relh` higher than 110% should most likely be considered as outlier.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Q: How many observations have relative humidity higher than 110?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE relh> 110 

# COMMAND ----------

# MAGIC %md 
# MAGIC  
# MAGIC These 53 observations should be considered as outliers.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Sky Level Coverage 
# MAGIC 
# MAGIC ### Introduction
# MAGIC There are 4 variables `skyc1`, `skyc2`, `skyc3` and `skyc4` each of which has a corresponding sky coverage altitude which are stored in `skyl1`, `skyl2`, `skyl3` and `skyl4`.
# MAGIC 
# MAGIC For `skyl1`, `skyl2`, `skyl3` and `skyl4`:
# MAGIC 
# MAGIC - The numbers represent the clouds' height in feet (simply add 2 zeroes to get the height)
# MAGIC 
# MAGIC For `skyc1`, `skyc2`, `skyc3` and `skyc4`:
# MAGIC 
# MAGIC - The cloud cover will either be 
# MAGIC      - FEW (1/8 TO 2/8 cloud coverage), 
# MAGIC      - SCT (SCATTERED, 3/8 TO 4/8 cloud coverage, 
# MAGIC      - BKN (5/8-7/8 coverage), or 
# MAGIC      - OVC (OVERCAST, 8/8 Coverage)
# MAGIC       
# MAGIC - We will often have more than 1 designator (i.e. SCT035 BKN090 OVC140)
# MAGIC - An indefinite ceiling caused by fog, rain, snow, etc., will have a designator as **VV (Vertical Visibility)**
# MAGIC 
# MAGIC   *VV is the how high you can see vertically into the indefinite ceiling.*
# MAGIC   
# MAGIC - Notation for significant clouds will be found at the end of the reading (i.e. SCT035TCU), the different significant formations are:
# MAGIC     
# MAGIC      - TCU (Towering Cumulus)
# MAGIC      - CB, (Cumulonimbus, a shower/thunderstorm)
# MAGIC      - ACC (Altocumulus Castellanus)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q: What do the `skyl` variables represent?

# COMMAND ----------

df.select("skyl1", "skyl2", "skyl3", "skyl4").distinct().show()

# COMMAND ----------

# MAGIC %md As the level increases their corresponding altitude also increases. It is safe to assume that skyl1, skyl2, skyl3 and skyl4 represent rising altitude levels in the sky.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q: What do the `skyc` variables represent?

# COMMAND ----------

df.select("skyc1", "skyc2", "skyc3", "skyc4").distinct().show()

# COMMAND ----------

# MAGIC %md These variables represent the condition of the sky at that particular altitude level. For instance, `skyc1` represents the condition of the sky at `skyl1`.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Missing values:
# MAGIC #### Q: What percent of values are missing in the `skyc1`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyc1 is null

# COMMAND ----------

# MAGIC %md 
# MAGIC 2.71% of `skyc1` data is `null`.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyc2`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyc2 is null

# COMMAND ----------

# MAGIC %md 
# MAGIC 75.81% of `skyc2` data is null.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyc3`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyc3 is null

# COMMAND ----------

# MAGIC %md 84.24% of `skyc3` data is null.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyc4`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyc4 is null

# COMMAND ----------

# MAGIC %md 91.86% of `skyc4` data is null.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyl1`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyl1 is null

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 54.85% of `skyl1` data is null.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyl2`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyl2 is null

# COMMAND ----------

# MAGIC %md 75.19% of `skyl2` data is null.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyl3`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyl3 is null

# COMMAND ----------

# MAGIC %md 
# MAGIC 84.24% of `skyl3` data is null.

# COMMAND ----------

# MAGIC %md #### Q: What percent of values are missing in the `skyl4`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE skyl4 is null

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 91.86% of `skyl4` data is null.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data validation
# MAGIC #### Q: If there is a `skyc` value available, do we always have its corresponding `skyl` value?

# COMMAND ----------

sqlContext.sql('''\
SELECT skyc4, skyl4 FROM table
Where skyc4 is not null and skyl4 is null

UNION

SELECT skyc3, skyl3 FROM table
Where skyc3 is not null and skyl3 is null

UNION

SELECT skyc2, skyl2 FROM table
Where skyc2 is not null and skyl2 is null

UNION

SELECT skyc1, skyl1 FROM table
Where skyc1 is not null and skyl1 is null
''').show()


# COMMAND ----------

# MAGIC %md Only 5 records have a value for the sky condition when their corresponding sky altitude level is null. In preprocessing file further analysis is necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q: Are there any cases where a lower sky level is null but a higher sky level is not null?

# COMMAND ----------

sqlContext.sql('''\
SELECT * FROM table
Where skyc1 is null and skyc4 is not null

UNION

SELECT * FROM table
Where skyc1 is null and skyc3 is not null

UNION

SELECT * FROM table
Where skyc1 is null and skyc2 is not null

UNION

SELECT * FROM table
Where skyc1 is null and skyc4 is not null
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This means that all the sky levels are in order and therefore, if `skyl3` is null, `skyl4` will not have a value. This also confirms our assumption that sky level 1,2,3 and 4 are measured in the increasing order of their height.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q: What are the possible values that a `skyc` variable can have?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT skyc1
# MAGIC FROM table
# MAGIC WHERE skyc1 is not null  

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC As expected the distinct cloud condition types are:
# MAGIC 
# MAGIC - VV
# MAGIC - OVC
# MAGIC - FEW
# MAGIC - BKN
# MAGIC - SCT
# MAGIC - CLR

# COMMAND ----------

# MAGIC %md ####Q: Are there any cases with exceptional sky level conditions? For instance, skyl3 > skyl2 et cetra?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT skyc1, skyl1, skyl2, skyl3, skyl4, metar  
# MAGIC FROM table
# MAGIC WHERE (((skyl1 > skyl2) AND skyl2 != 0) 
# MAGIC       OR ((skyl2 > skyl3) AND skyl3 != 0)
# MAGIC       OR ((skyl3 > skyl4) AND skyl4 != 0)) 

# COMMAND ----------

# MAGIC %md Yes, for instance, in the first record we see that `skyl3` has a value greater than `skyl2` which theoretically should not be the case. 

# COMMAND ----------

# MAGIC %md ####Q: How many observations do not follow our assumption that sky levels are incremental (sky level 1 should be less that sky level 2)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) 
# MAGIC FROM table
# MAGIC WHERE (((skyl1 > skyl2) AND skyl2 != 0) 
# MAGIC       OR ((skyl2 > skyl3) AND skyl3 != 0)
# MAGIC       OR ((skyl3 > skyl4) AND skyl4 != 0))

# COMMAND ----------

# MAGIC %md There are 29 records where a lower sky level variable has a higher value. These observations will be eliminated in data preprocessing step.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Present Weather Condition and Obscuration
# MAGIC 
# MAGIC ### Introduction
# MAGIC 
# MAGIC |  _INTENSITY OR PROXIMITY 1_         |_DESCRIPTOR 2_        | _PRECIPITATION 3_      |_OBSCURATION 4_      | _OTHER 5_ |
# MAGIC |:-----------------------------------:|:-----------|:-----------|:-----------|:-----------|
# MAGIC |  light (-)  |  MI Shallow   |  DZ Drizzle  |  BR Mist |  PO Well-Developed Dust/Sand Whirls  
# MAGIC |  moderate ( ) |  PR Partial  |  RA Rain  |  FG Fog |  SQ Squalls 
# MAGIC |  heavy (+)  |  BC Patches  |  SN Snow |  FU Smoke |  SS Sandstorm  
# MAGIC |  VC In the Vicinity  | DR Low Drifting |  SG Snow Grains |  VA Volcanic Ash |  SS Dust storm  
# MAGIC |           |  BL Blowing  |  IC Ice Crystals |  DU Widespread Dust |  FC Funnel Cloud Tornado Waterspout 
# MAGIC |            |  SH Shower(s) |  PL Ice Pellets |  SA Sand  |                         
# MAGIC |             | TS Thunderstorm  |  GR Hail |  HZ Haze  |            
# MAGIC |              |  FZ Freezing  |  GS Small Hail and/or Snow Pellets |  PY Spray   |           
# MAGIC |              |  BC Patches  |  UP Unknown Precipitation  |              |              
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC _Notes:_
# MAGIC 1. The weather groups are usually constructed by considering columns 1 to 5 in sequence, i.e. intensity, followed by description, followed by weather phenomena, e.g. heavy rain shower(s) is coded as +SHRA
# MAGIC 
# MAGIC 2. Tornados and waterspouts should be coded as +FC.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Missing values
# MAGIC #### Q: What percent of values are missing in `presentwx`?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE presentwx is null

# COMMAND ----------

# MAGIC %md More than 88% of the observations are missing in `presentwx`.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data validation
# MAGIC #### Q: How many records have a `presentwx` value that suggests rain when the sky condition is clear?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE ((presentwx LIKE "%RA%") AND ((skyc1 == "CLR") OR (skyc2 == "CLR") OR (skyc3 == "CLR") OR (skyc4 == "CLR")))

# COMMAND ----------

# MAGIC %md 
# MAGIC We have condition wherein the `presentwx` column suggests that it rained but the sky condition is actually recorded as clear. This is interesting because we would not expect the sky to be clear during rain or thunderstorm.

# COMMAND ----------

# MAGIC %md #### Q: How many distinct weather conditions are recorded in the data set?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(Distinct presentwx)
# MAGIC FROM table
# MAGIC WHERE presentwx is not null

# COMMAND ----------

# MAGIC %md 
# MAGIC There are 255 distinct values in the `presentwx` column of our data. This suggests that `presentwx` cannot be encoded easily as the target for classification models.

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Q: Are there cases in which `presentwx` records more than one type of precipitation occuring at the same time?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT presentwx
# MAGIC FROM table
# MAGIC WHERE presentwx like "%RA%" AND presentwx like "%SN%"

# COMMAND ----------

# MAGIC %md 
# MAGIC `presentwx` values can consist of more than one type precipitation and intensity at a given observation, This makes the preparation for modeling more complicated as well.

# COMMAND ----------

# MAGIC %md ####Significant Cloud Types
# MAGIC 
# MAGIC Significant Clouds such as TCU (Towering Cumulus), CB, (Cumulonimbus, or a shower/thunderstorm), or ACC (Altocumulus Castellanus) should theoretically increase the chance of a thunderstorm.

# COMMAND ----------

# MAGIC %md #### Q: Does having a significant cloud type in the `metar` reading influence the value of any of the `sky` variables or the `presentwx` variable?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT skyc1, skyc2, skyc3, skyc4, presentwx, metar
# MAGIC FROM table
# MAGIC WHERE  ((metar like "% CB %") OR (metar like "%TCU%") OR (metar like "%ACC%")) AND (presentwx is not null)

# COMMAND ----------

# MAGIC %md There is an apparent relation between the presence of significant cloud types and `presentwx`. `presentwx` in this situation is mostly populated with codes related to rain and thunderstorm even though it is possible that major cloud types only cause obscurations such as mist. The information related to significant cloud type is recorded at the end of standard METAR reading. This suggests there are more information in METAR that can be parsed and used in predictive modeling.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC #Pressure altimeter 
# MAGIC 
# MAGIC ### Introduction
# MAGIC In the metar reading, `A` stands for Altimeter. A reading of 3016 indicates a value of 30.16 inches of mercury for the pressure.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Missing values
# MAGIC #### Q: What percentage of `alti` values are missing?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE alti is null

# COMMAND ----------

# MAGIC %md Only 0.58% of data is missing in`alti`

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Descriptive statistics of `alti`:

# COMMAND ----------

df.describe('alti').show()

# COMMAND ----------

# MAGIC %md The range of lowest to highest Barometric Pressure Records in the US is between 26.34 and 31.85:
# MAGIC 
# MAGIC  - Alaska has the highest pressure reading of 1078.6 mb (31.85”) on January 31, 1989 at Northway during one of the state’s greatest cold waves.
# MAGIC 
# MAGIC 
# MAGIC  - The lowest pressure ever measured anywhere in the United States (either as a result of a tropical or extra-tropical storm) was a reading of 892 mb (26.34”) at Matecumbe Key, Florida during the Great Labor Day Hurricane of September 2, 1935, the most intense hurricane ever to strike the United States.
# MAGIC 
# MAGIC This suggests that we have data points that that should probably be removed from our study.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data validation
# MAGIC #### Q: How many data points have values that are not in line with the national record?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE  alti < 25 or alti > 32 

# COMMAND ----------

# MAGIC %md 
# MAGIC These 11 records should be removed from our data set.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Sea Level Pressure 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Introduction
# MAGIC In METAR, the following would represent some information about sea level pressure:
# MAGIC 
# MAGIC Sea-Level Pressure (SLP###)
# MAGIC 
# MAGIC The Pressure altimeter in inches or `alti` and Sea Level Pressure in millibar `mslp` are somewhat related.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Missing values
# MAGIC #### Q: What percentage of `mslp` data is missing?

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE  mslp is null

# COMMAND ----------

# MAGIC %md 
# MAGIC There are 2,309,517 records that do not have value in `mslp`. Since our total number of records is 3,144,870, we have **73.44%** data points that has no values related to sea level pressure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descriptive statistics of `mslp`:

# COMMAND ----------

df.describe('mslp').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The highest sea level pressure ever reported in United States is 1056.9 millibars and over the world the highest sea level pressure ever reported is 1085.7. The maximum sea level pressure reported in our data is higher than the above two values. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data validation
# MAGIC ####Q: How many records have a sea level pressure higher than the national record:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE mslp > 1056.9

# COMMAND ----------

# MAGIC %md A total of 46 records have sea level pressures higher than the national record. These values are probably recorded by error.

# COMMAND ----------

# MAGIC %md 
# MAGIC #Precipitation Amount

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Introduction
# MAGIC 
# MAGIC `p01i` Records the precipitation amount in inches. This value may or may not contain frozen precipitation melted by some device on the sensor or estimated by some other means and unfortunately, we do not know of an authoritative database denoting which station used which sensor to record the values.
# MAGIC 
# MAGIC In METAR, the precipitation amount related readings are represented as:
# MAGIC 
# MAGIC |          |     |
# MAGIC |:--------------------------------------:|:-----------|
# MAGIC |  Snow Increasing Rapidly         |(SNINCR_amount this hour/total)       |
# MAGIC |    Hourly Precipitation Amount (P####): |Indicates the amount of liquid-equivalent precipitation accumulated during the last hour in hundredths.      |
# MAGIC |3- and 6-Hour Precipitation Amount (6####):| 3 or 6 hour precipitation amount. Follows RMK with 5 digits starting with 6. The last 4 digits are the inches of rain in hundredths. If used for the observation nearest to 00:00, 06:00, 12:00, or 18:00 UTC, it represents a 6-hour precipitation amount. If used in the observation nearest to 03:00, 09:00, 15:00 or 21:00 UTC, it represents a 3-hour precipitation amount.    |
# MAGIC |  24-Hour Precipitation Amount (7####)   | | 
# MAGIC |  Snow Depth on Ground (4/###):     |Total snow depth in inches. Follows RMK starting with 4/ and followed by 3 digit number that equals snow depth in inches. | 
# MAGIC |     Water Equivalent of Snow on Ground (9####):     |Snowfall in the last 6-hours. Follows RMK with 6 digits starting with 931. The last 3 digits are the total snowfall in inches and tenths. Liquid water equivalent of the snow (SWE). Follows RMK with 6 digits starting with 933. The last 3 digits are the total inches in tenths.|

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Missing values
# MAGIC #### Q: What percentage of `p01i` data is missing?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE p01i is null 

# COMMAND ----------

# MAGIC %md
# MAGIC There are 790 records that do not have value in `p01i`. Since our total number of records is 3,144,870, we only have 0.025% data that has no values related to precipitation amount.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data validation
# MAGIC #### Q: What are distinct weather conditions that have been observed when the precipitation amount is missing ?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT Distinct presentwx
# MAGIC FROM table
# MAGIC WHERE p01i is null and presentwx is not null

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC When the precipitation amount is null the weather conditions that have been reported are BR (mist) and HZ (haze). This suggests that null values actually show that there is no precipitation.

# COMMAND ----------

# MAGIC %md #### Q: How many observations don't have the `p01i` column is populated when there has been some sort of precipitation?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE (metar RLIKE ' P[0-9]{4} ' OR metar RLIKE ' 6[0-9]{4} ' OR metar LIKE ' 7[0-9]{4} ' OR metar RLIKE ' 9[0-9]{4} ') and p01i is null

# COMMAND ----------

# MAGIC %md
# MAGIC 12 of the 790 data points which have missing values in the `p01i`, do have information in metar. This suggests that there are some parsing errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q: Is it possible that `presentwx` is not reported but `p01i` is greater than 0?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT presentwx, p01i, metar
# MAGIC FROM table
# MAGIC WHERE (p01i is not null) AND (presentwx is null) AND (p01i > 0)

# COMMAND ----------

# MAGIC %md 
# MAGIC The present weather condition in these observations does not have any value, however, there is a value recorded for the precipitation. This suggests that if we were to describe the weather condition as wet or dry, we could consider it as wet in these cases, this can help us to retrieve more information for modeling purposes.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q: How many observations do not have `presentwx` value but `p01i` column has values greater than 0? 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM table
# MAGIC WHERE (p01i is not null) AND (presentwx is null) AND (p01i != 0)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 26,892 records have precipitation amount that is greater than 0 when `presentwx` is `null`.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Station
# MAGIC 
# MAGIC 
# MAGIC ### Introduction
# MAGIC The 3 or 4 letter code in the beginning of the METAR reading is used to identify the station where the METAR reading was recorded. There is no missing value in this column.

# COMMAND ----------

# MAGIC %md #### Q: What is the number of the distinct stations in our data? 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (DISTINCT station)
# MAGIC FROM table

# COMMAND ----------

# MAGIC %md We have data from 2128 different locations.

# COMMAND ----------

# MAGIC %md ------------------------------------------------------------------------------------------------------------------------------------------*The End*------------------------------------------------------------------------- --------------------------------------------------------