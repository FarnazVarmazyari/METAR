# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # `METAR - Data pre-processing`

# COMMAND ----------

# MAGIC %md Import all the required libraries:

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.functions import col,sum


# COMMAND ----------

# MAGIC %md Read all the data and store it in a dataframe:

# COMMAND ----------

data_dir  = "/mnt/datalab-datasets/metar/"
df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferSchema='true', comment="#", mode='DROPMALFORMED', nullValue="M").load(data_dir+'*.txt')
for name in df.schema.names:
  df= df.withColumnRenamed(name, name.strip())
  
df = df.withColumn('valid', df["valid"].cast('timestamp'))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Objective of our study
# MAGIC #### Problem statement: To make predictions about the weather condition and determine if there will be any sort of precipitation or not
# MAGIC 
# MAGIC Target variable(`precipitation`) is a combination of two variables(`presentwx` and `p0li`)
# MAGIC - If `presentwx` column contains any of the following codes that correspond to some sort of precipitation type, assume that precipitation occurs and populate the target column with a `1` 
# MAGIC 
# MAGIC 
# MAGIC | _PRECIPITATION _   |
# MAGIC |:-----------|
# MAGIC |- DZ Drizzle |
# MAGIC |- RA Rain |
# MAGIC  | - SN Snow | 
# MAGIC  | - SG Snow Grains |
# MAGIC | - IC Ice Crystals |
# MAGIC | - PL Ice Pellets | 
# MAGIC | - GR Hail | 
# MAGIC | - GS Small Hail and/or Snow Pellets | 
# MAGIC | - UP Unknown Precipitation
# MAGIC 
# MAGIC 
# MAGIC - If the `p01i` column has a value greater than zero, it indicates that the amount of precipitation recorded is greather than zero and therefore, populate the target column with a `1`

# COMMAND ----------

# MAGIC %md 
# MAGIC The target variable (`precipitation`) will be a binary variable: 
# MAGIC 
# MAGIC - 1 indicating precipitation occurred 
# MAGIC - 0 indicating precipitation did not occur
# MAGIC 
# MAGIC 
# MAGIC The following function populates the `precipitation` column based on the values in the `presentwx` column:

# COMMAND ----------

def isPrecp(x):
  if x is None:
    return 3
  conditions = ['DZ', 'RA', 'SN', 'SG', 'IC', 'PL', 'GR', 'GS', 'UP']
  if any(condition in x for condition in conditions):
      return 1
  return 0

isPrecp_udf = udf(isPrecp, IntegerType())
df_target_presetwx = df.withColumn("precipitation0", isPrecp_udf(df.presentwx))

# COMMAND ----------

# MAGIC %md 
# MAGIC The following function populates the `precipitation` column based on the values in the `p01i` column:

# COMMAND ----------

def isPrecp(x):
  if x is None: 
    return 3
  
  if x > 0:
      return 1
  return 0

isPrecp_udf = udf(isPrecp, IntegerType())
df_target_precipitationAmount = df_target_presetwx.withColumn("precipitation1", isPrecp_udf(df.p01i))

df_target = df_target_precipitationAmount.withColumn('precipitation', df_target_precipitationAmount.precipitation0 + df_target_precipitationAmount.precipitation1)

targetDf = df_target.withColumn("precipitation", when(df_target["precipitation"] == 3, 0).when(df_target["precipitation"] == 6, 3).when(df_target["precipitation"] == 2, 1).when(df_target["precipitation"] == 4, 1).otherwise(df_target["precipitation"]))

final_df_ = targetDf.withColumn("precipitation", when(targetDf["precipitation"] == 3, None).otherwise(targetDf["precipitation"]))
final_df = final_df_.na.drop(subset=['precipitation'])
final_df.groupby('precipitation').count().show()

# COMMAND ----------

# MAGIC %md 
# MAGIC To build the final data set, address the following issues with respect to all our explanatory variables:
# MAGIC  - missing values
# MAGIC  - duplicate values
# MAGIC  - outliers

# COMMAND ----------

# MAGIC %md Before investigating all the explanatory variables, remove all the variables that aren't needed: 
# MAGIC  - First, remove the variables that were created while preparing our target variable (i.e. `precipitation0`,`precipitation1`)
# MAGIC  - Second, remove the variables that the target variable is based on (i.e. `presentwx` and the `p0li`)
# MAGIC  - Third, remove the `metar` variable since that just contains the unprocessed METAR readings
# MAGIC  - Fourth, remove the `mslp` variable since 73.44% of its values are missing and thus, including this variable will limit our data set and/or lead to misleading results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Missing Values
# MAGIC 
# MAGIC The following four variables do not have any missing values and thus can be included as is:
# MAGIC  - `station`
# MAGIC  - `valid`
# MAGIC  - `lon`
# MAGIC  - `lat`
# MAGIC 
# MAGIC For the following quantitiave variables, impute the missing values with the last non-null value after grouping the data by `station` and ordering it by the time at which the METAR reading was taken:
# MAGIC - `vsby`
# MAGIC - `relh`
# MAGIC - `dwpf`
# MAGIC - `tmpf`
# MAGIC - `alti`
# MAGIC 
# MAGIC *Note: It is essential to group the readings up by `station` and order them in the ascending order of `valid`(the timestamp at which the reading was taken) before imputation to maintain the integrity of the data *

# COMMAND ----------

miss =['vsby', 'relh', 'dwpf', 'tmpf', 'alti']

for var in miss:
    final_df1 = final_df.withColumn(var, func.last(var, True).over(Window.partitionBy('station').orderBy('valid').rowsBetween(-sys.maxsize, 0)))
    
final = final_df1.drop('presentwx','precipitation0','precipitation1','metar','p01i','mslp')
final.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in final.columns)).show()

# COMMAND ----------

# MAGIC %md The `drct` variable records the direction of the wind, and 0 degrees and 360 degrees represent the same thing, keep just one of these two values and replace all the 360s by 0

# COMMAND ----------

final_df3 = final.withColumn("drct", when(final["drct"] == 360, 0).otherwise(final["drct"]))

final_df4=final_df3.fillna(0, subset=['gust'])


final_df4.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in final_df4.columns)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Even after imputing the `vsby`, `relh`, `dwpf`, `tmpf` and `alti` variables, there are still some `null` values. 
# MAGIC - This is due to the fact that while imputing these columns, the last non-null value was considered, but what if the first value after grouping the records by station is null? In that case, we will not be able to successfully impute all the values. Therefore, as expected, there are certain values that have not been imputed.
# MAGIC - Delete all these records from the data set

# COMMAND ----------

final_df6 = final_df4.na.drop(subset=['drct','tmpf','dwpf','relh','alti','vsby'])
final_df6.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in final_df6.columns)).show()
final_df6.registerTempTable("table")

# COMMAND ----------

# MAGIC %md #### Handling the Sky related variables

# COMMAND ----------

# MAGIC %md We know that all the sky level variables are incremental i.e. for a particular record `skyl1` should have a value less than `skyl2` and so on and so forth. 
# MAGIC 
# MAGIC Check if we have any data that violates the above rule:

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM table WHERE (((skyl1 > skyl2) AND skyl2 != 0)  OR ((skyl2 > skyl3) AND skyl3 != 0)  OR ((skyl3 > skyl4) AND skyl4 != 0))

# COMMAND ----------

# MAGIC %md 27 records don't have the sky level variables in order and therefore should be removed from our study:

# COMMAND ----------

df_1 = sqlContext.sql("Select * from table MINUS SELECT * FROM table WHERE (((skyl1 > skyl2) AND skyl2 != 0)  OR ((skyl2 > skyl3) AND skyl3 != 0)  OR ((skyl3 > skyl4) AND skyl4 != 0))")

df_1.select('skyl1','skyl2','skyl3','skyl4').describe().show()


# COMMAND ----------

# MAGIC %md In the above example, we see that the maximum value of `skyl1` and `skyl3` seem to be outliers or erroneous and thus, these values should be removed as well:

# COMMAND ----------

df_3 = sqlContext.sql("Select * from table MINUS (SELECT * FROM table WHERE (((skyl1 > skyl2) AND skyl2 != 0)  OR ((skyl2 > skyl3) AND skyl3 != 0)  OR ((skyl3 > skyl4) AND skyl4 != 0) OR (skyl1 = 103000)OR (skyl3 = 95000)))")

df_3.select('skyl1','skyl2','skyl3','skyl4').describe().show()

# COMMAND ----------

# MAGIC %md Visualize the range in which most of the sky level values for different sky level variables fall:

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
plt.style.use('ggplot')

hists_1 = df_3.select('skyl1').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_1 = {'bins': hists_1[0][:-1],'freq': hists_1[1]}


hists_2 = df_3.select('skyl2').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_2 = {'bins': hists_2[0][:-1],'freq': hists_2[1]}

hists_3 = df_3.select('skyl3').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_3 = {'bins': hists_3[0][:-1],'freq': hists_3[1]}

hists_4 = df_3.select('skyl4').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_4 = {'bins': hists_4[0][:-1],'freq': hists_4[1]}

plt.gcf().clear()

N1 = [0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000]
N2 = [0+625, 2500+625, 5000+625, 7500+625,10000+625, 12500+625, 15000+625, 17500+625, 20000+625, 22500+625, 25000+625, 27500+625, 30000+625, 32500+625, 35000+625,37500+625, 40000+625]
N3 = [0+1250, 2500+1250, 5000+1250, 7500+1250,10000+1250, 12500+1250, 15000+1250, 17500+1250, 20000+1250, 22500+1250, 25000+1250, 27500+1250, 30000+1250, 32500+1250, 35000+1250,37500+1250, 40000+1250]

N4 = [1875,2500+1875, 5000+1875, 7500+1875,10000+1875, 12500+1875, 15000+1875, 17500+1875, 20000+1875, 22500+1875, 25000+1875, 27500+1875, 30000+1875, 32500+1875, 35000+1875,37500+1875, 40000+1875]

p1 = plt.bar(N1, data_1['freq'], width=625, color = "#ff7f50",alpha=0.8)
p2 = plt.bar(N2, data_2['freq'], width=625, color = "#d1ff52",alpha=0.8)
p3 = plt.bar(N3, data_3['freq'], width=625, color = "#52d1ff",alpha=0.8)
p4 = plt.bar(N4, data_4['freq'], width=625, color = "#8052ff",alpha=0.8)
plt.xlim(xmax=41250)
plt.ylim(ymax=500000)
plt.legend((p1[0], p2[0], p3[0], p4[0]), ('Skyl1', 'Skyl2', 'Skyl3', 'Skyl4'))
plt.yticks(np.arange(0, 500000, 50000))
plt.xticks(np.arange(0, 41250, 5000))
plt.grid(color='b', linestyle='-', linewidth=0.5,alpha=0.2)

fig = plt.gcf()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Looking at the above plot, we define 5 different ranges for the sky level and classify all the existing values within these ranges.
# MAGIC 
# MAGIC The ranges will be:
# MAGIC 
# MAGIC - 0 to 5000
# MAGIC - 5001 to 10000
# MAGIC - 10001 to 15000
# MAGIC - 15001 to 20000
# MAGIC - 20001 and above

# COMMAND ----------

def categorize(var):
    if ((var >= 0) & (var <= 5000)): return "0_to_5000"
    if ((var > 5000) & (var <= 10000)): return "5001_to_10000"
    if ((var > 10000) & (var <= 15000)): return "10001_to_15000"
    if ((var > 15000) & (var <= 20000)): return "15001_to_20000"
    if ((var > 20000)): return "20001_and_above"
    
    return var
    
categorize_udf = udf(categorize)
new1 = df_3.withColumn("SkyLevel1", categorize_udf(df.skyl1))
new2 = new1.withColumn("SkyLevel2", categorize_udf(df.skyl2))
new3 = new2.withColumn("SkyLevel3", categorize_udf(df.skyl3))
new4 = new3.withColumn("SkyLevel4", categorize_udf(df.skyl4))

# COMMAND ----------

# MAGIC %md Next, by using the `crosstab()` method, determine the different `skyc` values associated with various sky level ranges:

# COMMAND ----------

new4.crosstab('SkyLevel1','skyc1').show()
new4.registerTempTable("table1")

# COMMAND ----------

# MAGIC %md When the sky coverage is clear then there is no sky level value associated with it!

# COMMAND ----------

# MAGIC  %sql SELECT distinct skyc1 from table  UNION SELECT distinct skyc2 from table UNION SELECT distinct skyc3 from table UNION SELECT distinct skyc4 from table
# MAGIC  

# COMMAND ----------

# MAGIC %md Create new binary explanatory variables by grouping the sky conditions and sky altitudes together, the columns will be as follows:
# MAGIC 
# MAGIC - BKN_0_to_5000
# MAGIC - OVC_0_to_5000
# MAGIC - SCT_0_to_5000
# MAGIC - FEW_0_to_5000
# MAGIC - VV_0_to_5000
# MAGIC - BKN_5001_to_10000
# MAGIC - OVC_5001_to_10000
# MAGIC - SCT_5001_to_10000
# MAGIC - FEW_5001_to_10000
# MAGIC - VV_5001_to_10000
# MAGIC - BKN_10001_to_15000
# MAGIC - OVC_10001_to_15000
# MAGIC - SCT_10001_to_15000
# MAGIC - FEW_10001_to_15000
# MAGIC - VV_10001_to_15000
# MAGIC - BKN_15001_to_20000
# MAGIC - OVC_15001_to_20000
# MAGIC - SCT_15001_to_20000
# MAGIC - FEW_15001_to_20000
# MAGIC - VV_15001_to_20000
# MAGIC - BKN_20001_and_above
# MAGIC - OVC_20001_and_above
# MAGIC - SCT_20001_and_above
# MAGIC - FEW_20001_and_above
# MAGIC - VV_20001_and_above
# MAGIC - CLEAR
# MAGIC 
# MAGIC The above columns will only have a value of either `1` or `0`. For instance, if for a particular record the sky condition is broken at an altitude that ranges from 0 to 5000 then the `BKN_0_to_5000` will be set to 1; otherwise 0.

# COMMAND ----------

SkyLevel = ["0_to_5000", "5001_to_10000", "10001_to_15000", "15001_to_20000", "20001_and_above"]

SkyCondition = ["BKN", "OVC", "SCT", "FEW", "VV"]


for l in SkyLevel:
  for c in SkyCondition:
    new4 = new4.withColumn(c + '_' + l, when(
    (new4.SkyLevel1 == l) & (new4.skyc1 == c) |
    (new4.SkyLevel2 == l) & (new4.skyc2 == c) |
    (new4.SkyLevel3 == l) & (new4.skyc3 == c) |
    (new4.SkyLevel4 == l) & (new4.skyc4 == c), 1).otherwise(0))
    
new4 = new4.withColumn('CLEAR', when(
    (new4.SkyLevel4.isNull()) & (new4.skyc4 == "CLR"), 1).otherwise(0))

# COMMAND ----------

# MAGIC %md Since, we have constructed the above mentioned variables from the existing `skyc` and `skyl` variables, we will drop the existing `skyc` and `skyl` variables from our data set.

# COMMAND ----------

new6 = new4.drop('SkyLevel1', 'SkyLevel2', 'SkyLevel3', 'SkyLevel4','skyc1', 'skyc2', 'skyc3', 'skyc4', 'skyl1', 'skyl2', 'skyl3', 'skyl4')

# COMMAND ----------

# MAGIC %md Check if there are any missing values remaining in our data set:

# COMMAND ----------

new6.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in new6.columns)).show()

# COMMAND ----------

# MAGIC %md We finally have a data set which has no `null` values, the next step is to see if any of our data records are duplicates:

# COMMAND ----------

# MAGIC %md ### Handling Duplicate records
# MAGIC 
# MAGIC Observations that are repeated in a DataFrame are called duplicates. By counting number of rows and number of distinct rows, we can determine the number of duplicates.

# COMMAND ----------

print('Count of rows: {0}'.format(new6.count()))
print('Count of distinct rows: {0}'.format(new6.distinct().count()))

# COMMAND ----------

# MAGIC %md As the output suggests, we do not have any duplicate values

# COMMAND ----------

# MAGIC %md #### Handling Outliers
# MAGIC 
# MAGIC Outliers are observations that are significantly different from the distribution of the rest of the sample. There are various options to deal with the outliers in a data set:
# MAGIC 
# MAGIC ##### 1. Interquartile range method (IQR): 
# MAGIC 
# MAGIC If a point is further than `1.5*IQR` from the mean that point is classified as an outlier and should be eliminated from the data

# COMMAND ----------

num_col_all = ['tmpf', 'dwpf', 'relh', 'drct', 'sknt', 'p01i', 'alti', 'mslp', 'vsby','gust']
bounds = {}

for col in num_col_all:
  quantiles = df.approxQuantile(col, [0.25, 0.75], 0.05)
  IQR = quantiles[1] - quantiles[0]
  bounds[col] = [quantiles[0] - 1.5 * IQR,quantiles[1] + 1.5 * IQR]
  
bounds

# COMMAND ----------

# MAGIC %md Why can't we use the IQR method to get rid of outliers?:
# MAGIC 
# MAGIC Although this is the most common method to remove outliers, it doesn't serve our purpose since the distribution of some of the variables in the METAR data set is not compatible with this approach. 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##### 2. Standard deviation from the mean: 
# MAGIC 
# MAGIC All the values that fall outside 4 standard deviations from mean of the variable are classified as outliers
# MAGIC 
# MAGIC Why can't we use this approach?
# MAGIC 
# MAGIC - If we use a common function and apply the standard deviation approach to all the numerical variables, we are left with only 320,355 observations in the data set
# MAGIC - Increasing the `standard deviation from the mean` value from 4 to a bigger number would mean that we are accommodating outliers in some variables to ensure minimum loss of data 
# MAGIC - However, even by using +/-8 standard deviation, all the outliers were not removed
# MAGIC 
# MAGIC Therefore, a common approach for filtering out outliers cannot be used and each variable will need to be handled individually since all the variables have:
# MAGIC   
# MAGIC    - different distributions; and
# MAGIC    - different amount of skewness
# MAGIC  
# MAGIC Additionally, the models that we create will only be limited to a dataset that has values for the month of August since we don't have the data from the entire year and are working with data from the month of August (our population itself is a sample from a much bigger population)
# MAGIC 
# MAGIC ## The approach that we adopted to deal with outliers:
# MAGIC - The outliers were removed through SQL queries by taking into account the highest and lowest national records since the common outlier detection techniques such as IQR and standard deviation did not provide a good result. We used this approach to remove outliers from `relh` `dwpf` `tmpf` `alti` `vsby` and `sknt` ( i.e. *relative humidity, dew point temperature, temperature, pressure, visibility and wind speed* respectively).

# COMMAND ----------


new6.registerTempTable("table3")
new11 = sqlContext.sql("Select * from table3 MINUS (SELECT * FROM table3 WHERE sknt > 220 or vsby > 40 or relh > 110 or alti > 32 or alti < 25 or tmpf < 0 or dwpf > 96 or dwpf< -23)")

# COMMAND ----------

new11.count()

# COMMAND ----------

# MAGIC %md `new11` is our final data set and in our next file, we will use this data set to fit different models.
# MAGIC 
# MAGIC --------------------------------------------------------------------- **THE END** -----------------------------------------------------------------------------------------------------------