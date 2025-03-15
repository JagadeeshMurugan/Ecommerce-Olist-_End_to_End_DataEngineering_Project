# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing Data From ADLS Gen2

# COMMAND ----------


storage_account = "jaggiolistdata"
application_id = "02581bcd-cea3-4193-a2e5-09129cd54fdc"
directory_id = "807971a1-d45a-4fb2-8adb-91f99bad00fd"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "lCK8Q~A1A1SarmK-FRju6UZ~NkbD~oleG6US5aiB")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://olistdata@jaggiolistdata.dfs.core.windows.net/Bronze/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading All Data

# COMMAND ----------

base_path = "abfss://olistdata@jaggiolistdata.dfs.core.windows.net/Bronze/"

orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payment_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format('csv').option('header', 'true').load(orders_path)
payments_df = spark.read.format('csv').option('header', 'true').load(payments_path)
reviews_df = spark.read.format('csv').option('header', 'true').load(reviews_path)
items_df = spark.read.format('csv').option('header', 'true').load(items_path)
customers_df = spark.read.format('csv').option('header', 'true').load(customers_path)
sellers_df = spark.read.format('csv').option('header', 'true').load(sellers_path)
geolocation_df = spark.read.format('csv').option('header', 'true').load(geolocation_path)
products_df = spark.read.format('csv').option('header', 'true').load(products_path)

display(orders_df)
display(payments_df)
display(reviews_df)
display(items_df)
display(customers_df)
display(sellers_df)
display(geolocation_df)
display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing data from MongoDB For Data Enrichment

# COMMAND ----------

import pandas as pd
import pymongo

from pymongo import MongoClient
# importing module
from pymongo import MongoClient

hostname = "uu2pm.h.filess.io"
database = "OlistNoSql_nounchain"
port = "27018"
username = "OlistNoSql_nounchain"
password = "2409774d0f0f649d86695d41cb9b155f3a38293f"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]




# COMMAND ----------

colection = mydatabase['product_categories']
mongo_data=pd.DataFrame(list(colection.find()))
mongo_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Data 

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Drop Duplciates 

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("Drop Duplicates").getOrCreate()

# Base Path
base_path = "abfss://olistdata@jaggiolistdata.dfs.core.windows.net/Bronze/"

# File Paths
file_paths = {
    "orders_df": "olist_orders_dataset.csv",
    "payments_df": "olist_order_payment_dataset.csv",
    "reviews_df": "olist_order_reviews_dataset.csv",
    "items_df": "olist_order_items_dataset.csv",
    "customers_df": "olist_customers_dataset.csv",
    "sellers_df": "olist_sellers_dataset.csv",
    "geolocation_df": "olist_geolocation_dataset.csv",
    "products_df": "olist_products_dataset.csv"
}

# Load, Drop Duplicates, and Display Each Dataset
for name, path in file_paths.items():
    full_path = base_path + path
    globals()[name] = spark.read.format('csv').option('header', 'true').load(full_path).dropDuplicates()

    # Display results
    print(f"Dataset loaded and cleaned: {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####  checking Null Values

# COMMAND ----------

from pyspark.sql import functions as F

# Function to count null values for each dataset
def count_nulls(df):
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])

# Checking null values for all datasets
for name, df in datasets.items():
    print(f"Null Values in {name}:")
    count_nulls(df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling Null Values

# COMMAND ----------

# Handling null values directly
orders_df = orders_df.fillna({
    'order_approved_at': 'Unknown',
    'order_delivered_carrier_date': 'Unknown',
    'order_delivered_customer_date': 'Unknown'
})

payments_df = payments_df.fillna(0)

reviews_df = reviews_df.fillna({
    'review_comment_title': 'No Comment',
    'review_comment_message': 'No Comment',
    'review_creation_date': 'Unknown',
    'review_answer_timestamp': 'Unknown'
})

items_df = items_df.fillna(0)

customers_df = customers_df.fillna(0)

sellers_df = sellers_df.fillna(0)

geolocation_df = geolocation_df.fillna(0)

products_df = products_df.fillna({
    'product_category_name': 'Unknown',
    'product_name_lenght': 0,  # Temporary fix
    'product_description_lenght': 0,
    'product_photos_qty': 0,
    'product_weight_g': 0,
    'product_length_cm': 0,
    'product_height_cm': 0,
    'product_width_cm': 0
})

print("✅ Null values fixed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Orders_df

# COMMAND ----------

orders_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, datediff, to_date

# Convert relevant columns to datetime
orders_df = orders_df.withColumn('order_approved_at', to_date(col('order_approved_at')))
orders_df = orders_df.withColumn('order_delivered_customer_date', to_date(col('order_delivered_customer_date')))
orders_df = orders_df.withColumn('order_purchase_timestamp', to_date(col('order_purchase_timestamp')))

# Feature 1: Order Processing Time (Days)
orders_df = orders_df.withColumn('order_processing_time', 
                                 datediff(col('order_delivered_customer_date'), col('order_approved_at')))

# Feature 2: Order Delivery Time (Days)
orders_df = orders_df.withColumn('order_delivery_time', 
                                 datediff(col('order_delivered_customer_date'), col('order_purchase_timestamp')))

# Feature 3: Is Delayed (Threshold: 10 days)
orders_df = orders_df.withColumn('is_delayed', col('order_processing_time') > 10)

# Display first 5 rows to verify
display(orders_df.select('order_processing_time', 'order_delivery_time', 'is_delayed').limit(5))

# COMMAND ----------

geolocation_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformming geolocation_df

# COMMAND ----------

from pyspark.sql import functions as F

# Create a 'region' feature based on state grouping
north_states = ['AM', 'PA', 'RR', 'AP']
south_states = ['PR', 'SC', 'RS']

geolocation_df = geolocation_df.withColumn(
    'region', 
    F.when(F.col('geolocation_state').isin(north_states), 'North')
     .when(F.col('geolocation_state').isin(south_states), 'South')
     .otherwise('Other')
)

# Display first 5 rows to verify
geolocation_df.select('geolocation_state', 'region').show(5)

# COMMAND ----------

products_df.display()


# COMMAND ----------

customers_df.display()

# COMMAND ----------

customers_df = customers_df.withColumn('customer_location', 
                                       F.concat(F.col('customer_city'), F.lit(', '), F.col('customer_state')))

customers_df = customers_df.withColumn('is_south_region', 
                                       F.when(F.col('customer_state').isin('SP', 'RJ', 'MG'), 1).otherwise(0))

# COMMAND ----------

customers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming selers_df

# COMMAND ----------

sellers_df = sellers_df.withColumn('seller_location', 
                                   F.concat(F.col('seller_city'), F.lit(', '), F.col('seller_state')))

multi_location_sellers = sellers_df.groupBy('seller_id').count().filter('count > 1')
sellers_df = sellers_df.withColumn('is_multi_location_seller', 
                                   F.when(F.col('seller_id').isin([row['seller_id'] for row in multi_location_sellers.collect()]), 1).otherwise(0))

# COMMAND ----------

sellers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming payments_df

# COMMAND ----------

payments_df = payments_df.withColumn('is_high_value_order', 
                                     F.when(F.col('payment_value') > 500, 1).otherwise(0))

payments_df = payments_df.withColumn('payment_installments_flag', 
                                     F.when(F.col('payment_installments') > 3, 1).otherwise(0))

payments_df = payments_df.withColumn('payment_ratio', 
                                     F.col('payment_value') / F.col('payment_installments'))

# COMMAND ----------

payments_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Reviews_df 

# COMMAND ----------

reviews_df = reviews_df.withColumn('is_positive_review', 
                                   F.when(F.col('review_score') >= 4, 1).otherwise(0))

from pyspark.sql import functions as F

reviews_df = reviews_df.withColumn('review_response_time_hours', 
                                   (F.abs(F.unix_timestamp('review_creation_date') - 
                                          F.unix_timestamp('review_answer_timestamp')) / 3600).cast('int'))

# COMMAND ----------

reviews_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining All Datasets

# COMMAND ----------

# Join orders with order items
final_df = orders_df.join(items_df, 'order_id', 'left') \
    .join(payments_df, 'order_id', 'left') \
    .join(reviews_df, 'order_id', 'left') \
    .join(customers_df, 'customer_id', 'left') \
    .join(geolocation_df, customers_df['customer_zip_code_prefix'] == geolocation_df['geolocation_zip_code_prefix'], 'left') \
    .join(sellers_df, 'seller_id', 'left') \
    .join(products_df, 'product_id', 'left')

# Display sample output
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Mongo Data (Product_Category_Name)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)
mongo_spark_df=spark.createDataFrame(mongo_data)


# COMMAND ----------

final_df = final_df.join(mongo_spark_df, 'product_category_name', 'left')
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sending Data to Silver Layer

# COMMAND ----------

final_df.write.mode('overwrite').parquet("abfss://olistdata@jaggiolistdata.dfs.core.windows.net/Silver/")