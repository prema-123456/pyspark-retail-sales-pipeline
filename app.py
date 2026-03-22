from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum
import streamlit as st

st.title("📊 Retail Sales Dashboard")

# Start Spark
spark = SparkSession.builder.appName("Retail App").getOrCreate()

# Load Data
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# Cleaning
df = df.dropna()
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Feature Engineering
df = df.withColumn("total_price", col("price") * col("quantity"))

# KPI
total_revenue = df.select(sum("total_price")).collect()[0][0]
st.metric("Total Revenue", f"₹{total_revenue}")

# Category Sales
st.subheader("Category-wise Sales")
category_sales = df.groupBy("category").sum("total_price").toPandas()
st.dataframe(category_sales)

# City Sales
st.subheader("City-wise Sales")
city_sales = df.groupBy("city").sum("total_price").toPandas()
st.dataframe(city_sales)