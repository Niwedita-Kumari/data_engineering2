# Databricks notebook source

dbutils.fs.mount(
  source = "wasbs://{container}@{storage}.blob.core.windows.net",
  mount_point = "{mount_path}",
  extra_configs = {"fs.azure.account.key.{storage}.blob.core.windows.net":"{secret_access_key}"}
)

# COMMAND to check mount is done successfully. This should list the files in the given mount path!----------

dbutils.fs.ls('{mount_path}')

# COMMAND to read raw data from Bronze layer (2 csv files and one json file)----------

df_customers = spark.read.parquet('{bronze_mount_path}/')
df_transactions = spark.read.parquet('{bronze_mount_path}/')
df_merchants = spark.read.parquet('{URL path for the json file}')

# COMMAND to create silver layer for data cleaning which involves type casting, filling null valued records with non-null values and dropping duplicate records ----------

from pyspark.sql.functions import col,SparkSession

# Data clean1 - Convert types update

df_customers = df_customers.select(
    col("customer_id").cast("int"),
    col("account_balance").cast("double"),
    col("join_date").cast("date"),
    col("risk_score").cast("double")
)
df_customers = df_customers.na.fill({"email": "unknown@outlook.com", "phone": "N/A"})

# COMMAND to display customers table----------

display(df_customers)

df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("amount").cast("double"),
    col("transaction_date").cast("date")
)

df_transactions = df_transactions.na.fill({"merchant": "Unknown", "amount": 0.0})

# COMMAND to display transactions table----------

display(df_transactions)

df_merchants = df_merchants.select("merchant_id", "name", "category", "location","commission_rate", "registravg_ratingation_date","total_volume").dropDuplicates(["merchant_id"])

# COMMAND to display merchants table----------

display(df_merchants)

# COMMAND to join all the 3 data sources----------

df_silver = df_customers.join(df_transactions, "customer_id").join(df_merchants, "merchant_id")

# COMMAND to display the join result----------

display(df_silver)

# COMMAND to dump the silver layer data to adls location in delta format----------

silver_path = "{silver_mount_path}"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

# COMMAND to create silver dataset----------

spark.sql(f""" CREATE TABLE silver_table USING DELTA LOCATION '{silver_mount_path}' """)

# COMMAND to check the silver_table data ----------

%sql select * from silver_table

# COMMAND to load cleaned data from Silver layer ----------

silver_df = spark.read.format("delta").load("{silver_mount_path}")

# COMMAND to display silver layer data----------

display(silver_df)

# COMMAND to display data wrt business requirements ----------

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy("name","customer_id","transaction_date","payment_method","merchant","merchant_id","location")
    .agg(avg(risk_score*avg_rating).alias("fraud_score"),
    sum(account_balance-amount).alias("Remaining_customer_amount_balance"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg(amount).alias("average_transaction_value"))

# COMMAND to display gold layer data----------

display(gold_df)

# COMMAND to dump the gold layer data to adls location in delta format----------

gold_path = "{gold_mount_path}"

gold_df.write.mode("overwrite").format("delta").save(gold_path)

# COMMAND to create gold dataset----------

spark.sql(f""" CREATE TABLE gold_table USING DELTA LOCATION '{gold_mount_path}' """)

# COMMAND to check the gold_table data ----------

%sql select * from gold_table
