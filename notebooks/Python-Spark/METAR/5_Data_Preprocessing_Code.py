# Databricks notebook source
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.functions import col,sum

data_dir  = "/mnt/datalab-datasets/metar/"
df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferSchema='true', comment="#", mode='DROPMALFORMED', nullValue="M").load(data_dir+'*.txt')
for name in df.schema.names:
  df= df.withColumnRenamed(name, name.strip())
  
df = df.withColumn('valid', df["valid"].cast('timestamp'))

def isPrecp(x):
  if x is None:
    return 3
  conditions = ['DZ', 'RA', 'SN', 'SG', 'IC', 'PL', 'GR', 'GS', 'UP']
  if any(condition in x for condition in conditions):
      return 1
  return 0

isPrecp_udf = udf(isPrecp, IntegerType())
df_target_presetwx = df.withColumn("precipitation0", isPrecp_udf(df.presentwx))

# this will be recoded to easier version after we are done with final dataframe creation
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

# COMMAND ----------

miss =['vsby', 'relh', 'dwpf', 'tmpf', 'alti']

for var in miss:
    final_df1 = final_df.withColumn(var, func.last(var, True).over(Window.partitionBy('station').orderBy('valid').rowsBetween(-sys.maxsize, 0)))
    
final = final_df1.drop('presentwx','precipitation0','precipitation1','metar','p01i','mslp')
final_df3 = final.withColumn("drct", when(final["drct"] == 360, 0).otherwise(final["drct"]))
final_df4=final_df3.fillna(0, subset=['gust'])
final_df6 = final_df4.na.drop(subset=['drct','tmpf','dwpf','relh','alti','vsby'])
final_df6.registerTempTable("table")
  
df_3 = sqlContext.sql("Select * from table MINUS (SELECT * FROM table WHERE (((skyl1 > skyl2) AND skyl2 != 0)  OR ((skyl2 > skyl3) AND skyl3 != 0)  OR ((skyl3 > skyl4) AND skyl4 != 0) OR (skyl1 = 103000)OR (skyl3 = 95000)))")

hists_1 = df_3.select('skyl1').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_1 = {'bins': hists_1[0][:-1],'freq': hists_1[1]}


hists_2 = df_3.select('skyl2').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_2 = {'bins': hists_2[0][:-1],'freq': hists_2[1]}

hists_3 = df_3.select('skyl3').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_3 = {'bins': hists_3[0][:-1],'freq': hists_3[1]}

hists_4 = df_3.select('skyl4').rdd.flatMap(lambda row: row).histogram([0, 2500, 5000, 7500,10000, 12500, 15000, 17500, 20000, 22500, 25000, 27500, 30000, 32500, 35000,37500, 40000,42500])
data_4 = {'bins': hists_4[0][:-1],'freq': hists_4[1]}

def categorize(var):
    if ((var >= 0) & (var <= 5000)): return "0_to_5000"
    if ((var > 5000) & (var <= 10000)): return "5000_to_10000"
    if ((var > 10000) & (var <= 15000)): return "10000_to_15000"
    if ((var > 15000) & (var <= 20000)): return "15000_to_20000"
    if ((var > 20000)): return "20000_and_above"
    
    return var
    
categorize_udf = udf(categorize)
new1 = df_3.withColumn("SkyLevel1", categorize_udf(df.skyl1))
new2 = new1.withColumn("SkyLevel2", categorize_udf(df.skyl2))
new3 = new2.withColumn("SkyLevel3", categorize_udf(df.skyl3))
new4 = new3.withColumn("SkyLevel4", categorize_udf(df.skyl4))
new5 = new4.fillna('NotAvailable')

SkyLevel = ["0_to_5000", "5000_to_10000", "10000_to_15000", "15000_to_20000", "20000_and_above"]
SkyCondition = ["BKN", "OVC", "SCT", "FEW", "VV"]

for l in SkyLevel:
  for c in SkyCondition:
    new5 = new5.withColumn(c + '_' + l, when(
    (new5.SkyLevel1 == l) & (new5.skyc1 == c) |
    (new5.SkyLevel2 == l) & (new5.skyc2 == c) |
    (new5.SkyLevel3 == l) & (new5.skyc3 == c) |
    (new5.SkyLevel4 == l) & (new5.skyc4 == c), 1).otherwise(0))

new5 = new5.withColumn('CLR', when((new5.SkyLevel1 == "NotAvailable") & (new5.skyc1 == "CLR") , 1).otherwise(0))
new6 = new5.drop('SkyLevel1', 'SkyLevel2', 'SkyLevel3', 'SkyLevel4','skyc1', 'skyc2', 'skyc3', 'skyc4', 'skyl1', 'skyl2', 'skyl3', 'skyl4')

new6 = new6.withColumn('NotAvailable', when(
  (new6.CLR == 0) & (new6.BKN_0_to_5000 == 0) & (new6.OVC_0_to_5000 == 0) & (new6.SCT_0_to_5000 == 0) & (new6.FEW_0_to_5000 == 0) & (new6.VV_0_to_5000 == 0) & (new6.BKN_5000_to_10000 == 0) & (new6.OVC_5000_to_10000 == 0) & (new6.SCT_5000_to_10000 == 0) & (new6.FEW_5000_to_10000 == 0) & (new6.VV_5000_to_10000 == 0) & (new6.BKN_10000_to_15000 == 0) & (new6.OVC_10000_to_15000 == 0) & (new6.SCT_10000_to_15000 == 0) & (new6.FEW_10000_to_15000 == 0) & (new6.VV_10000_to_15000 == 0) & (new6.BKN_15000_to_20000 == 0) & (new6.OVC_15000_to_20000 == 0) & (new6.SCT_15000_to_20000 == 0) & (new6.FEW_15000_to_20000 == 0) & (new6.VV_15000_to_20000 == 0) & (new6.BKN_20000_and_above == 0) & (new6.OVC_20000_and_above == 0) & (new6.SCT_20000_and_above == 0) & (new6.FEW_20000_and_above == 0) & (new6.VV_20000_and_above == 0)
                                            , 1).otherwise(0))

# COMMAND ----------

new6.registerTempTable("table3")
new11 = sqlContext.sql("Select * from table3 MINUS (SELECT * FROM table3 WHERE sknt > 220 or vsby > 40 or relh > 110 or alti > 32 or alti < 25 or tmpf < 0 or dwpf > 96 or dwpf< -23)")
