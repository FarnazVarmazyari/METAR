# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC > ##Analyzing the METAR (Meteorological Aerodrome Report) readings
# MAGIC Since the beginning of aviation weather has been a very important factor in flight planning. One must know if thunderstorms are too intense to make a flight, how much fuel to bring along, and many other variables. The IEM (Iowa Environmental Mesonet) maintains an ever growing archive of automated airport weather observations from cooperating members around the world.
# MAGIC  
# MAGIC 
# MAGIC >> *by Farnaz Varmazyari and Navleen S. Khanuja*
# MAGIC 
# MAGIC >>> *Fall 2017*
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Different tasks have been split up in the following files to enable a better understanding. The files are listed in the order in which they should be referred:
# MAGIC 
# MAGIC - ###  `1_Data download` 
# MAGIC This file contains the script that scrapes the data recorded by an automated surface observation system. The METAR readings are gathered after fixed time intervals at different airport locations. These readings are stored in text files which we are extracting and storing in S3 buckets.
# MAGIC 
# MAGIC - ###  `2_Data consolidation`
# MAGIC In this file, we examine the:
# MAGIC 
# MAGIC   - total number of files that we've downloaded
# MAGIC   - content of the files 
# MAGIC   - missing data points
# MAGIC   - total number and type of columns
# MAGIC   - total number of rows
# MAGIC 
# MAGIC - ###  `3_Understanding the data`
# MAGIC In this file, we examine the predictors in detail and try to understand how they have been derived from the METAR reading.
# MAGIC 
# MAGIC - ###  `4_Data preprocessing`
# MAGIC In this file, we work towards getting our final data set ready. To achieve this objective, we: 
# MAGIC 
# MAGIC   - create the target variable (which is derived from two different columns - a column that tells us about the present weather condition and a column that tells us about precipitation amount)
# MAGIC   - handle all the missing values individually based on the technique that seems appropriate for each predictor
# MAGIC   - check and get rid of any duplicate entries in our data set
# MAGIC   - define the outliers and get rid of them
# MAGIC 
# MAGIC - ### `5_Data_Preprocessing_Code`
# MAGIC This file contains the code that gets us to our final data set. In the files that we create next, we will just be referring to this particular file by `%run ./5_Data_Preprocessing_Code`. The `%run` command essentially works like an `import` and will execute all the commands in the `5_Data_Preprocessing_Code` file and make all the variables and data frames available to the current file
# MAGIC 
# MAGIC - ### `6_Fitting and evaluating models`
# MAGIC In this file, we fit models and evaluate them. To achieve this goal, we:
# MAGIC   - create the feature vectors and labels through pipeline
# MAGIC   - fit the following models:
# MAGIC     - Logistic Regression
# MAGIC     - Decision Tree
# MAGIC     - Random Forest
# MAGIC   - tune the parameters with `ParamGrid` and 3-fold cross validation
# MAGIC   - evaluate the best model obtained via cross validation using the test set
# MAGIC 
# MAGIC 
# MAGIC >>> The following file(s) can be referred in addition to the above files to get a better understanding of the data set and our techniques:
# MAGIC - #### `Interesting articles`

# COMMAND ----------

# MAGIC %md --------------------------------------------------------------------------------------------------------------------*The End*--------------------------------------------------------------------------------------------------