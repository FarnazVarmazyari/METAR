# Databricks notebook source
# MAGIC %md
# MAGIC # `METAR - Understanding the Data`

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###Our data set has a total of 24 variables:
# MAGIC 
# MAGIC | _ Name _      |_Description _      |_ Percentage of data missing _      |
# MAGIC |:-----------:|:-----------|:-----------:|
# MAGIC | `station` |A three or four character code to identify sites |0|
# MAGIC | `valid` |Timestamp at which the observation was taken |0|
# MAGIC | `tmpf` |Air temperature in Fahrenheit, typically at 2 meters| 0.84|
# MAGIC | `dwpf` |Dew point temperature in Fahrenheit, typically at 2 meters|1.21|
# MAGIC | `relh` |Relative humidity in %|1.21|
# MAGIC | `drct` |Wind direction in degrees from north|2.67|
# MAGIC | `sknt` |Wind speed in knots|.89|
# MAGIC | `p01i` |Amount of precipitation in inches|2.31|
# MAGIC | `alti` |Pressure altimeter in inches|.58|
# MAGIC |  `mslp`|Sea level pressure in millibar|67.69|
# MAGIC | `vsby` |Visibility in statute miles|1.75|
# MAGIC |  `gust`|Wind gust in knots|82.72|
# MAGIC |  `skyc1`|Sky level 1 coverage|2.71|
# MAGIC | `skyc2` |Sky level 2 coverage|75.81 |
# MAGIC | `skyc3` |Sky level 3 coverage| 84.24 |
# MAGIC | `skyc4` |Sky level 4 coverage|  91.86|
# MAGIC | `skyl1` |Sky level 1 altitude in feet|54.85|
# MAGIC | `skyl2` |Sky level 2 altitude in feet|75.19 |
# MAGIC | `skyl3` |Sky level 3 altitude in feet|84.24|
# MAGIC | `skyl4` |Sky level 4 altitude in feet|84.24|
# MAGIC | `presentwx` |Present weather condition|81.14|
# MAGIC | `metar` |Unprocessed METAR observation|0|
# MAGIC | `lon` |Longitude of the location|0|
# MAGIC | `lat` |Latitude of the location|0|

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary of handling missing values:
# MAGIC 
# MAGIC 
# MAGIC | _ No `null` values_      |_ Imputed with the last available non null value after grouping by station and ordering by the timestamp at which the reading was taken (After imputation all the remaining null values were dropped)_      |_ Columns dropped_      |_`null` dropped_|
# MAGIC |:-----------:|:-----------:|:-----------:|:-----------:|
# MAGIC | `station` |`tmpf`|`presentwx`|`drct`|
# MAGIC | `valid` |`dwpf`|`metar`|`sknt`|
# MAGIC | `lon` | `relh`| `p01i` ||
# MAGIC | `lat`| `vsby` |`gust`||
# MAGIC || `alti`|`mslp`|||

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Summary of handling potential outliers:
# MAGIC There are obseravations in our data set have a value that is either lower or higher than the national record and therefore, these values will be removed from our study since they are potentially outliers:
# MAGIC 
# MAGIC 
# MAGIC | Variable name   | Min Value   | Max Value  |
# MAGIC |:-----------:|:-----------:|:-----------:|
# MAGIC | `tmpf` |0|-|
# MAGIC |`dwpf`  |-23|96|
# MAGIC |`sknt`  |-| 220|
# MAGIC | `alti`| 25 |32|
# MAGIC |`vsby`|-|40|
# MAGIC |`relh`|-|110%|
# MAGIC 
# MAGIC Extra Notes:
# MAGIC 
# MAGIC - 14 `tmpf` observations were removed from the data set
# MAGIC - As per records, the lowest dew point ever recorded was in Las Vegas and it was -22 F
# MAGIC - 21 `sknt` observations were removed from the data set
# MAGIC - 11 `alti` observations were removed from the data set. Alaska has the highest recorded pressure reading of 31.85" -  January 31, 1989 at Northway during one of the state’s greatest cold waves, Matecumbe Key, Florida has the lowest recorded pressure of 26.34" - September 2, 1935 (The Great Labor Day Hurricane)
# MAGIC - 53 `relh` observations were removed from the data set

# COMMAND ----------

# MAGIC %md ### Points to note:

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC ##### `valid`:
# MAGIC 
# MAGIC   - The data set consists of METAR readings recorded from August 01, 2012 to September 01, 2012

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC ##### `drct` represents the direction that the wind is blowing from
# MAGIC  - 0 degrees represents north
# MAGIC  - 90 degrees represents east
# MAGIC  - 180 degrees represents south
# MAGIC  - 270 degrees represents west
# MAGIC 
# MAGIC 55,494 observations in `drct` have the value of 360. These values were changed to 0 since 0 and 360 degrees essentially represent the same thing.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##### Sky variables
# MAGIC `skyl1`, `skyl2`, `skyl3` and `skyl4` represent the height at which the sky condition was recorded and the corresponding sky condition is represented by `skyc1`, `skyc2`, `skyc3` and `skyc4`
# MAGIC 
# MAGIC - The sky condition (`skyc1`, `skyc2`, `skyc3` and `skyc4`) can have the following values: 
# MAGIC   - FEW (1/8 TO 2/8 cloud coverage), 
# MAGIC   - SCT (SCATTERED, 3/8 TO 4/8 cloud coverage, 
# MAGIC   - BKN (5/8-7/8 coverage), or 
# MAGIC   - OVC (OVERCAST, 8/8 Coverage)
# MAGIC   - An indefinite ceiling caused by fog, rain, snow, etc., will have a designator as **VV (Vertical Visibility)**
# MAGIC   - CLR
# MAGIC      
# MAGIC - Every `skyl` value should have a corresponding `skyc` value
# MAGIC   - 5 observations have a value for the sky condition but do not have a corresponding sky altitude level and these observations will be eliminated from the study
# MAGIC - `skyl1`, `skyl2`, `skyl3` and `skyl4` should have values such that `skyl1 < skyl2 <skyl3 < skyl4` 
# MAGIC   - 29 observations do not follow the assumed trend of `skyl1 < skyl2 <skyl3 < skyl4` and these observations will be eliminated from the study
# MAGIC   

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##### Present Weather Condition `presentwx`:
# MAGIC 
# MAGIC 
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
# MAGIC - Weather groups are usually constructed by considering columns 1 to 5 in sequence, i.e. intensity, followed by description, followed by weather phenomena
# MAGIC   - e.g. heavy rain shower(s) is coded as +SHRA
# MAGIC   - Tornados and waterspouts are coded as +FC
# MAGIC 
# MAGIC - `presentwx` is recorded as BR (mist) or HZ (haze) when the `p0li` is `null`
# MAGIC - It is possible to observe more than one intensity, obscuration and precipitation at a time 
# MAGIC   - (Example: -RASN BR represents rain and snow at the same time)
# MAGIC - There are 255 distinct values in the `presentwx` column

# COMMAND ----------

# MAGIC %md ------------------------------------------------------------------------------------------------------------------------------------------*The End*------------------------------------------------------------------------- --------------------------------------------------------